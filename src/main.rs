use chrono::{DateTime, Utc};
use gzlib::proto::{pricing::pricing_server::*, upl::upl_client::UplClient};
use gzlib::proto::{pricing::*, upl::SetSkuPriceRequest};
use packman::*;
use price::Sku;
use std::{env, path::PathBuf};
use tokio::sync::{oneshot, Mutex};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{
  transport::{Channel, Server},
  Request, Response, Status,
};

mod prelude;
mod price;

use prelude::*;

struct PricingService {
  skus: Mutex<VecPack<price::Sku>>,
  client_upl: Mutex<UplClient<Channel>>,
}

impl PricingService {
  // Init PricingService with the given DB
  fn init(db: VecPack<price::Sku>, upl_client: UplClient<Channel>) -> Self {
    Self {
      skus: Mutex::new(db),
      client_upl: Mutex::new(upl_client),
    }
  }
  // Set price
  async fn set_price(&self, p: SetPriceRequest) -> ServiceResult<PriceObject> {
    let mut first_time_sku: Option<Sku> = None;
    // If the sku has already a price set
    let sku = match self.skus.lock().await.find_id_mut(&p.sku) {
      Ok(sku_object) => {
        match sku_object.as_mut().unpack().set_price(
          p.price_net_retail,
          price::VAT::from_str(&p.vat).map_err(|e| ServiceError::bad_request(&e))?,
          p.price_gross_retail,
          p.created_by,
        ) {
          Ok(res) => res.clone(),
          Err(e) => return Err(ServiceError::bad_request(&e)),
        }
      }
      Err(_) => {
        // If the price is set for the first time
        let mut new_sku = price::Sku::new(p.sku);
        // Set new price to the new sku
        new_sku
          .set_price(
            p.price_net_retail,
            price::VAT::from_str(&p.vat).map_err(|e| ServiceError::bad_request(&e))?,
            p.price_gross_retail,
            p.created_by,
          )
          .map_err(|e| ServiceError::bad_request(&e))?;
        first_time_sku = Some(new_sku.clone());
        new_sku
      }
    };

    // Check if we have already a price obecjt for the requested sku
    match first_time_sku {
      // Store first time price sku if needed
      Some(new_sku) => self.skus.lock().await.insert(new_sku)?,
      None => (), // Do nothing
    }

    // Store prices to related UPLs
    self
      .client_upl
      .lock()
      .await
      .set_sku_price(SetSkuPriceRequest {
        sku: sku.sku,
        net_price: sku.net_retail_price,
        vat: sku.vat.to_string(),
        gross_price: sku.gross_retail_price,
      })
      .await
      .map_err(|e| ServiceError::bad_request(&e.to_string()))?;

    // Return new sku as PriceObject
    Ok(sku.into())
  }
  // Tries to get price
  async fn get_price(&self, r: GetPriceRequest) -> ServiceResult<PriceObject> {
    let res = self.skus.lock().await.find_id(&r.sku)?.unpack().clone();
    Ok(res.into())
  }
  // Get prices bulk
  async fn get_price_bulk(&self, q: GetPriceBulkRequest) -> ServiceResult<Vec<PriceObject>> {
    let res = self
      .skus
      .lock()
      .await
      .iter()
      .filter(|p| q.skus.contains(&p.unpack().sku))
      .map(|p| p.unpack().clone().into())
      .collect::<Vec<PriceObject>>();
    Ok(res)
  }
  // Get latest price changes
  async fn get_latest_price_changes(&self, r: PriceChangesRequest) -> ServiceResult<Vec<u32>> {
    // Determine from date
    let from = DateTime::parse_from_rfc3339(&r.date_from)
      .map_err(|_| ServiceError::bad_request("A megadott -tól- dátum hibás"))?
      .with_timezone(&Utc);
    // Determine till date
    let till = DateTime::parse_from_rfc3339(&r.date_till)
      .map_err(|_| ServiceError::bad_request("A megadott -ig- dátum hibás"))?
      .with_timezone(&Utc);
    // Get results
    let res = self
      .skus
      .lock()
      .await
      .iter()
      .filter(|s| {
        let sku = s.unpack();
        if let Some(price) = sku.history.last() {
          return price.created_at >= from && price.created_at <= till;
        }
        false
      })
      .map(|s| s.unpack().sku)
      .collect::<Vec<u32>>();
    Ok(res)
  }
  // Get price history items
  async fn get_price_history(&self, r: GetPriceRequest) -> ServiceResult<Vec<PriceHistoryObject>> {
    let res = self
      .skus
      .lock()
      .await
      .find_id(&r.sku)?
      .unpack()
      .history
      .iter()
      .map(|phi| phi.clone().into())
      .collect::<Vec<PriceHistoryObject>>();
    Ok(res)
  }
}

#[tonic::async_trait]
impl Pricing for PricingService {
  type GetPriceBulkStream = ReceiverStream<Result<PriceObject, Status>>;

  async fn set_price(
    &self,
    request: Request<SetPriceRequest>,
  ) -> Result<Response<PriceObject>, Status> {
    let res = self.set_price(request.into_inner()).await?;
    Ok(Response::new(res))
  }

  async fn get_price(
    &self,
    request: Request<GetPriceRequest>,
  ) -> Result<Response<PriceObject>, Status> {
    let res = self.get_price(request.into_inner()).await?;
    Ok(Response::new(res))
  }

  type GetPriceHistoryStream = ReceiverStream<Result<PriceHistoryObject, Status>>;

  async fn get_price_history(
    &self,
    request: Request<GetPriceRequest>,
  ) -> Result<Response<Self::GetPriceHistoryStream>, Status> {
    // Create channels
    let (mut tx, rx) = tokio::sync::mpsc::channel(4);
    // Get found price objects
    let res = self.get_price_history(request.into_inner()).await?;
    // Send found price_objects through the channel
    tokio::spawn(async move {
      for pho in res.into_iter() {
        tx.send(Ok(pho)).await.unwrap();
      }
    });
    return Ok(Response::new(ReceiverStream::new(rx)));
  }

  async fn get_price_bulk(
    &self,
    request: Request<GetPriceBulkRequest>,
  ) -> Result<Response<Self::GetPriceBulkStream>, Status> {
    // Create channels
    let (mut tx, rx) = tokio::sync::mpsc::channel(4);
    // Get found price objects
    let res = self.get_price_bulk(request.into_inner()).await?;
    // Send found price_objects through the channel
    tokio::spawn(async move {
      for ots in res.into_iter() {
        tx.send(Ok(ots)).await.unwrap();
      }
    });
    return Ok(Response::new(ReceiverStream::new(rx)));
  }

  async fn get_latest_price_changes(
    &self,
    request: Request<PriceChangesRequest>,
  ) -> Result<Response<PriceIds>, Status> {
    let res = self.get_latest_price_changes(request.into_inner()).await?;
    Ok(Response::new(PriceIds { price_ids: res }))
  }
}

#[tokio::main]
async fn main() -> prelude::ServiceResult<()> {
  let db: VecPack<price::Sku> =
    VecPack::load_or_init(PathBuf::from("data/prices")).expect("Error while loading price db");

  let client_upl = UplClient::connect(service_address("SERVICE_ADDR_UPL"))
    .await
    .expect("Could not connect to image processer service");

  let pricing_service = PricingService::init(db, client_upl);

  let addr = env::var("SERVICE_ADDR_PRICING")
    .unwrap_or("[::1]:50061".into())
    .parse()
    .unwrap();

  // Create shutdown channel
  let (tx, rx) = oneshot::channel();

  // Spawn the server into a runtime
  tokio::task::spawn(async move {
    Server::builder()
      .add_service(PricingServer::new(pricing_service))
      .serve_with_shutdown(addr, async { rx.await.unwrap() })
      .await
  });

  tokio::signal::ctrl_c().await.unwrap();

  println!("SIGINT");

  // Send shutdown signal after SIGINT received
  let _ = tx.send(());

  Ok(())
}
