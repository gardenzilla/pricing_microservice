use chrono::prelude::*;
use packman::*;
use serde::{Deserialize, Serialize};
use std::ops::Mul;
use std::path::PathBuf;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum VAT {
    AAM,
    FAD,
    TAM,
    _5,
    _18,
    _27,
}

impl Default for VAT {
    fn default() -> Self {
        VAT::_27
    }
}

impl VAT {
    pub fn from_str(str: &str) -> Result<VAT, String> {
        match str {
            "AAM" => Ok(VAT::AAM),
            "aam" => Ok(VAT::AAM),
            "FAD" => Ok(VAT::FAD),
            "fad" => Ok(VAT::FAD),
            "TAM" => Ok(VAT::TAM),
            "tam" => Ok(VAT::TAM),
            "5" => Ok(VAT::_5),
            "18" => Ok(VAT::_18),
            "27" => Ok(VAT::_27),
            _ => Err("Nem megfelelő Áfa formátum! 5, 18, 27, AAM, TAM, FAD".into()),
        }
    }
}

impl ToString for VAT {
    fn to_string(&self) -> String {
        match self {
            VAT::AAM => "AAM".to_string(),
            VAT::FAD => "FAD".to_string(),
            VAT::TAM => "TAM".to_string(),
            VAT::_5 => "5".to_string(),
            VAT::_18 => "18".to_string(),
            VAT::_27 => "27".to_string(),
        }
    }
}

impl Mul<VAT> for u32 {
    type Output = u32;

    fn mul(self, rhs: VAT) -> Self::Output {
        let res = match rhs {
            VAT::AAM => self as f32 * 1.0,
            VAT::FAD => self as f32 * 1.0,
            VAT::TAM => self as f32 * 1.0,
            VAT::_5 => self as f32 * 1.05,
            VAT::_18 => self as f32 * 1.18,
            VAT::_27 => self as f32 * 1.27,
        };
        res.round() as u32
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct HistoryItem {
    net_retail_price: u32,
    vat: VAT,
    gross_retail_price: u32,
    created_by: String,
    created_at: DateTime<Utc>,
}

impl Default for HistoryItem {
    fn default() -> Self {
        Self {
            net_retail_price: 0,
            vat: VAT::default(),
            gross_retail_price: 0,
            created_by: "".into(),
            created_at: Utc::now(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Sku {
    sku: u32,
    net_retail_price: u32,
    vat: VAT,
    gross_retail_price: u32,
    history: Vec<HistoryItem>,
}

impl Default for Sku {
    fn default() -> Self {
        Self {
            sku: 0,
            net_retail_price: 0,
            vat: VAT::default(),
            gross_retail_price: 0,
            history: Vec::new(),
        }
    }
}

impl VecPackMember for Sku {
    type Out = u32;

    fn get_id(&self) -> &Self::Out {
        &self.sku
    }
}

fn main() {
    let mut db: VecPack<Sku> = VecPack::load_or_init(PathBuf::from("data/pricing"))
        .expect("Error while loading pricing db");
}
