use gzlib::proto::pricing::pricing_service_server::*;
use gzlib::proto::pricing::*;
use packman::*;
use std::path::PathBuf;
use tokio::sync::{oneshot, Mutex};
use tonic::{transport::Server, Request, Response, Status};

mod prelude;
mod pricing;

use prelude::*;

pub struct PricingService {
    skus: Mutex<VecPack<pricing::Sku>>,
}

impl PricingService {
    fn new(skus: Mutex<VecPack<pricing::Sku>>) -> Self {
        Self { skus }
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

fn main() {
    let mut db: VecPack<pricing::Sku> = VecPack::load_or_init(PathBuf::from("data/pricing"))
        .expect("Error while loading pricing db");
}
