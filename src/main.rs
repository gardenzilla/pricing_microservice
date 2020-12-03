use gzlib::prelude::*;
use gzlib::proto::pricing::pricing_service_server::*;
use gzlib::proto::pricing::*;
use packman::*;
use std::{collections::HashMap, env, path::PathBuf};
use tokio::sync::{oneshot, Mutex};
use tonic::{transport::Server, Request, Response, Status};

mod prelude;
mod pricing;

use prelude::*;

pub struct _PricingService {
    skus: Mutex<VecPack<pricing::Sku>>,
}

impl _PricingService {
    fn new(db: VecPack<pricing::Sku>) -> Self {
        Self {
            skus: Mutex::new(db),
        }
    }
    async fn get_price(&self, q: GetPriceRequest) -> ServiceResult<PriceResponse> {
        if let Ok(sku) = self
            .skus
            .lock()
            .await
            .find_id(&hex_str_to_u32(&q.sku).map_err(|e| ServiceError::bad_request(&e))?)
        {
            let s = sku.unpack();
            return Ok(PriceResponse {
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
    async fn get_price_bulk(&self, q: GetPriceRequestBulk) -> ServiceResult<GetPriceResponse> {
        let mut res: HashMap<String, PriceResponseBulk> = HashMap::new();
        let skul = self.skus.lock().await;
        q.skus.iter().for_each(|_sku| {
            if let Ok(sku) = u32::from_str_radix(&_sku, 16) {
                if let Ok(s) = skul.find_id(&sku) {
                    res.insert(
                        _sku.to_string(),
                        PriceResponseBulk {
                            sku: _sku.to_string(),
                            price_net_retail: s.unpack().net_retail_price as i32,
                            vat: s.unpack().vat.to_string(),
                            price_gross_retail: s.unpack().gross_retail_price as i32,
                            status_code: 200,
                        },
                    );
                } else {
                    res.insert(
                        _sku.to_string(),
                        PriceResponseBulk {
                            sku: _sku.to_string(),
                            price_net_retail: 0,
                            vat: "".into(),
                            price_gross_retail: 0,
                            status_code: 404,
                        },
                    );
                }
            }
        });
        Ok(GetPriceResponse { result: res })
    }
    async fn set_price(&self, p: SetPriceRequest) -> ServiceResult<PriceResponse> {
        let sku = u32::from_str_radix(&p.sku, 16)
            .map_err(|_| ServiceError::bad_request("Helytelen SKU ID!"))?;

        // If the sku has already a price set
        if let Ok(mut sku_object) = self.skus.lock().await.find_id_mut(&sku) {
            match sku_object.as_mut().unpack().set_price(
                p.price_net_retail,
                pricing::VAT::from_str(&p.vat).map_err(|e| ServiceError::bad_request(&e))?,
                p.price_gross_retail,
                p.created_by,
            ) {
                Ok(res) => {
                    return Ok(PriceResponse {
                        sku: format!("{:x}", res.sku),
                        price_net_retail: res.net_retail_price as i32,
                        vat: res.vat.to_string(),
                        price_gross_retail: res.gross_retail_price as i32,
                    })
                }
                Err(e) => return Err(ServiceError::bad_request(&e)),
            }
        }

        // If the price is set for the first time
        let mut new_sku = pricing::Sku::new(sku);
        new_sku
            .set_price(
                p.price_net_retail,
                pricing::VAT::from_str(&p.vat).map_err(|e| ServiceError::bad_request(&e))?,
                p.price_gross_retail,
                p.created_by,
            )
            .map_err(|e| ServiceError::bad_request(&e))?;
        self.skus.lock().await.insert(new_sku.clone())?;
        Ok(PriceResponse {
            sku: format!("{:x}", new_sku.sku),
            price_net_retail: new_sku.net_retail_price as i32,
            vat: new_sku.vat.to_string(),
            price_gross_retail: new_sku.gross_retail_price as i32,
        })
    }
}

#[tonic::async_trait]
impl PricingService for _PricingService {
    async fn set_price(
        &self,
        request: Request<SetPriceRequest>,
    ) -> Result<Response<PriceResponse>, Status> {
        Ok(Response::new(
            self.set_price(request.into_inner())
                .await
                .map_err(|e| Status::invalid_argument(e.to_string()))?,
        ))
    }

    async fn get_price(
        &self,
        request: Request<GetPriceRequest>,
    ) -> Result<Response<PriceResponse>, Status> {
        Ok(Response::new(
            self.get_price(request.into_inner())
                .await
                .map_err(|e| Status::invalid_argument(e.to_string()))?,
        ))
    }

    async fn get_price_bulk(
        &self,
        request: Request<GetPriceRequestBulk>,
    ) -> Result<Response<GetPriceResponse>, Status> {
        Ok(Response::new(
            self.get_price_bulk(request.into_inner())
                .await
                .map_err(|e| Status::invalid_argument(e.to_string()))?,
        ))
    }
}

#[tokio::main]
async fn main() -> prelude::ServiceResult<()> {
    let db: VecPack<pricing::Sku> = VecPack::load_or_init(PathBuf::from("data/pricing"))
        .expect("Error while loading pricing db");

    let pricing_service = _PricingService::new(db);

    let addr = env::var("SERVICE_ADDR_PRICING")
        .unwrap_or("[::1]:50061".into())
        .parse()
        .unwrap();

    // Create shutdown channel
    let (tx, rx) = oneshot::channel();

    // Spawn the server into a runtime
    tokio::task::spawn(async move {
        Server::builder()
            .add_service(PricingServiceServer::new(pricing_service))
            .serve_with_shutdown(addr, async { rx.await.unwrap() })
            .await
    });

    tokio::signal::ctrl_c().await.unwrap();

    println!("SIGINT");

    // Send shutdown signal after SIGINT received
    let _ = tx.send(());

    Ok(())
}
