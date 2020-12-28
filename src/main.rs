use gzlib::prelude::*;
use gzlib::proto::pricing::pricing_server::*;
use gzlib::proto::pricing::*;
use packman::*;
use std::{collections::HashMap, env, path::PathBuf};
use tokio::sync::{oneshot, Mutex};
use tonic::{transport::Server, Request, Response, Status};

mod prelude;
mod price;

use prelude::*;

pub struct PricingService {
    skus: Mutex<VecPack<price::Sku>>,
}

impl PricingService {
    fn new(db: VecPack<price::Sku>) -> Self {
        Self {
            skus: Mutex::new(db),
        }
    }
    async fn get_price(&self, q: GetPriceRequest) -> ServiceResult<PriceObject> {
        if let Ok(sku) = self.skus.lock().await.find_id(&q.sku) {
            let s = sku.unpack();
            return Ok(PriceObject {
                sku: q.sku,
                price_net_retail: s.net_retail_price as i32,
                vat: s.vat.to_string(),
                price_gross_retail: s.gross_retail_price as i32,
            });
        }

        Err(ServiceError::not_found(
            "A megadott SKU nem rendelkezik árral",
        ))
    }
    async fn get_price_bulk(&self, q: GetPriceBulkRequest) -> Vec<PriceObject> {
        let skul = self.skus.lock().await;
        let res: Vec<PriceObject> = skul
            .iter()
            // Only the requested SKUs
            .filter(|price| q.skus.contains(&price.unpack().sku))
            // Create price objects
            .map(|price| {
                let pobject = price.unpack();
                PriceObject {
                    sku: pobject.sku,
                    price_net_retail: pobject.net_retail_price as i32,
                    vat: pobject.vat.to_string(),
                    price_gross_retail: pobject.gross_retail_price as i32,
                }
            })
            .collect::<Vec<PriceObject>>();
        res
    }
    async fn set_price(&self, p: SetPriceRequest) -> ServiceResult<PriceObject> {
        // If the sku has already a price set
        if let Ok(mut sku_object) = self.skus.lock().await.find_id_mut(&p.sku) {
            match sku_object.as_mut().unpack().set_price(
                p.price_net_retail,
                price::VAT::from_str(&p.vat).map_err(|e| ServiceError::bad_request(&e))?,
                p.price_gross_retail,
                p.created_by,
            ) {
                Ok(res) => {
                    return Ok(PriceObject {
                        sku: res.sku,
                        price_net_retail: res.net_retail_price as i32,
                        vat: res.vat.to_string(),
                        price_gross_retail: res.gross_retail_price as i32,
                    });
                }
                Err(e) => return Err(ServiceError::bad_request(&e)),
            }
        }

        // If the price is set for the first time
        let mut new_sku = price::Sku::new(p.sku);
        new_sku
            .set_price(
                p.price_net_retail,
                price::VAT::from_str(&p.vat).map_err(|e| ServiceError::bad_request(&e))?,
                p.price_gross_retail,
                p.created_by,
            )
            .map_err(|e| ServiceError::bad_request(&e))?;
        self.skus.lock().await.insert(new_sku.clone())?;
        Ok(PriceObject {
            sku: new_sku.sku,
            price_net_retail: new_sku.net_retail_price as i32,
            vat: new_sku.vat.to_string(),
            price_gross_retail: new_sku.gross_retail_price as i32,
        })
    }
}

#[tonic::async_trait]
impl Pricing for PricingService {
    type GetPriceBulkStream = tokio::sync::mpsc::Receiver<Result<PriceObject, Status>>;
    type GetLatestPriceChangesStream =
        tokio::sync::mpsc::Receiver<Result<PriceHistoryObject, Status>>;

    async fn set_price(
        &self,
        request: Request<SetPriceRequest>,
    ) -> Result<Response<PriceObject>, Status> {
        Ok(Response::new(
            self.set_price(request.into_inner())
                .await
                .map_err(|e| Status::invalid_argument(e.to_string()))?,
        ))
    }

    async fn get_price(
        &self,
        request: Request<GetPriceRequest>,
    ) -> Result<Response<PriceObject>, Status> {
        Ok(Response::new(
            self.get_price(request.into_inner())
                .await
                .map_err(|e| Status::invalid_argument(e.to_string()))?,
        ))
    }

    async fn get_price_bulk(
        &self,
        request: Request<GetPriceBulkRequest>,
    ) -> Result<Response<Self::GetPriceBulkStream>, Status> {
        // Create channels
        let (mut tx, rx) = tokio::sync::mpsc::channel(4);
        // Get found price objects
        let res = self.get_price_bulk(request.into_inner()).await;
        // Send found price_objects through the channel
        for price_object in res.into_iter() {
            tx.send(Ok(price_object))
                .await
                .map_err(|_| Status::internal("Error while sending price bulk over channel"))?
        }
        return Ok(Response::new(rx));
    }

    async fn get_latest_price_changes(
        &self,
        request: Request<PriceChangesRequest>,
    ) -> Result<Response<Self::GetLatestPriceChangesStream>, Status> {
        let (mut tx, rx) = tokio::sync::mpsc::channel(4);
        return Ok(Response::new(rx));
    }
}

#[tokio::main]
async fn main() -> prelude::ServiceResult<()> {
    let db: VecPack<price::Sku> = VecPack::load_or_init(PathBuf::from("data/pricing"))
        .expect("Error while loading pricing db");

    let pricing_service = PricingService::new(db);

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
