use chrono::prelude::*;
use packman::*;
use serde::{Deserialize, Serialize};
use std::ops::Mul;

#[derive(Serialize, Deserialize, Clone, Debug, Copy)]
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
  pub net_retail_price: u32,
  pub vat: VAT,
  pub gross_retail_price: u32,
  pub created_by: u32,
  pub created_at: DateTime<Utc>,
}

impl Default for HistoryItem {
  fn default() -> Self {
    Self {
      net_retail_price: 0,
      vat: VAT::default(),
      gross_retail_price: 0,
      created_by: 0,
      created_at: Utc::now(),
    }
  }
}

impl HistoryItem {
  fn new(net_retail_price: u32, vat: VAT, gross_retail_price: u32, created_by: u32) -> Self {
    Self {
      net_retail_price,
      vat,
      gross_retail_price,
      created_by,
      created_at: Utc::now(),
    }
  }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Sku {
  pub sku: u32,
  pub net_retail_price: u32,
  pub vat: VAT,
  pub gross_retail_price: u32,
  pub history: Vec<HistoryItem>,
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

impl Sku {
  pub fn new(sku: u32) -> Self {
    let mut _sku = Self::default();
    _sku.sku = sku;
    _sku
  }
  pub fn set_price(
    &mut self,
    net_retail_price: i32,
    vat: VAT,
    gross_retail_price: i32,
    created_by: u32,
  ) -> Result<&Self, String> {
    // Check prices positive integers
    if net_retail_price < 0 || gross_retail_price < 0 {
      return Err("A megadott árak csak pozitív egész számok lehetnek!".into());
    }

    // Check price
    // net * VAT should be eq => gross
    if (net_retail_price as u32 * vat) != gross_retail_price as u32 {
      return Err("Ár hiba! A megadott nettó ár * ÁFA nem egyezik meg a bruttó árral!".into());
    }

    // Set new prices
    self.net_retail_price = net_retail_price as u32;
    self.vat = vat;
    self.gross_retail_price = gross_retail_price as u32;

    // Set price history
    self.history.push(HistoryItem::new(
      net_retail_price as u32,
      vat,
      gross_retail_price as u32,
      created_by,
    ));

    // Return the latest object reference
    Ok(self)
  }
}

impl VecPackMember for Sku {
  type Out = u32;

  fn get_id(&self) -> &Self::Out {
    &self.sku
  }
}
