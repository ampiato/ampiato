// Modules
pub mod ast;
pub mod core;
mod db;
pub mod replication;
mod ts;
mod value_provider;
pub mod prelude;

// Reeexported modules

// Ampiato modules
pub use crate::core::defs::Time;
pub use crate::core::{Error, TableMetadata, TableValues};
pub use crate::replication::FromTupleData;

pub use db::Db;
pub use ts::{TimeSeriesChanges, TimeSeriesDense, TimeSeriesInterval};
pub use value_provider::ValueProvider;

pub fn print_banner() {
    use colored::Colorize as _;
    println!("{}", include_str!("../banner.txt").green().bold());
}
