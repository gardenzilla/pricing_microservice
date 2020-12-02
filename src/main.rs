use packman::*;
use std::path::PathBuf;

mod pricing;

fn main() {
    let mut db: VecPack<pricing::Sku> = VecPack::load_or_init(PathBuf::from("data/pricing"))
        .expect("Error while loading pricing db");
}
