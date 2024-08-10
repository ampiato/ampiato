pub mod defs;

use std::error::Error as StdError;

use defs::Time;

#[derive(Debug)]
pub enum Error {
    UnexpectedNumberOfColumns { actual: usize, expected: usize },
    UnknownTable { table_name: String },
    ReplicationNotEnabled,
    DatabaseError(sqlx::Error),
    ReplicationError(String)
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::UnexpectedNumberOfColumns { actual, expected } => f.write_fmt(format_args!(
                "Unexpected number of columns: actual: {}, expected: {}",
                actual, expected
            )),
            Error::UnknownTable { table_name } => {
                f.write_fmt(format_args!("Unknown table: {}", table_name))
            },
            Error::ReplicationNotEnabled => {
                f.write_str("Replication not enabled")
            },
            Error::DatabaseError(e) => {
                f.write_fmt(format_args!("Database error: {}", e))
            },
            Error::ReplicationError(e) => {
                f.write_fmt(format_args!("Replication error: {}", e))
            }
        }
    }
}

pub type BoxDynError = Box<dyn StdError + 'static + Send + Sync>;

impl From<sqlx::Error> for Error {
    fn from(e: sqlx::Error) -> Self {
        Error::DatabaseError(e)
    }
}

impl StdError for Error {}


pub trait TableMetadata: Sized {
    fn query() -> &'static str;
    fn selector_names() -> Vec<&'static str>;
    fn column_names() -> Vec<&'static str>;
    fn table_name() -> &'static str;
}

pub trait TableValues<Selector> {
    fn time(&self) -> Time;
    fn selector(&self) -> Selector;
    fn values(&self) -> Vec<(&'static str, &f64)>;
}
