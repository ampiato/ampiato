use crate::core::Error;

use crate::replication::pgoutput::TupleData;

pub trait FromTupleData: Sized {
    fn from_tuple_data(tuple_data: &TupleData) -> Result<Self, Error>;
}

pub trait TableFromTupleData: Sized {
    fn from_tuple_data(relation_name: &str, tuple_data: &TupleData) -> Result<Self, Error>;
}
