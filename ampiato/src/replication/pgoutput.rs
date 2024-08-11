use core::panic;
use std::num::ParseFloatError;
use std::num::ParseIntError;
use std::str::from_utf8;
use std::str::Utf8Error;

use crate::Error;
use binrw::prelude::*;
use binrw::NullString;
use byteorder::BigEndian;
use byteorder::ByteOrder;
use chrono::DateTime;
use chrono::NaiveDateTime;
use chrono::Utc;

use crate::core::defs::Time;

const POSTGRES_EPOCH: i64 = 946684800000; // PostgreSQL epoch in microseconds since UNIX epoch

#[derive(Debug)]
pub enum ParseError {
    Utf8Error(Utf8Error),
    ParseIntError(ParseIntError),
    ParseFloatError(ParseFloatError),
    ChronoParseError(chrono::ParseError),
}

impl From<Utf8Error> for ParseError {
    fn from(value: Utf8Error) -> Self {
        ParseError::Utf8Error(value)
    }
}

impl From<ParseIntError> for ParseError {
    fn from(value: ParseIntError) -> Self {
        ParseError::ParseIntError(value)
    }
}

impl From<ParseFloatError> for ParseError {
    fn from(value: ParseFloatError) -> Self {
        ParseError::ParseFloatError(value)
    }
}

impl From<chrono::ParseError> for ParseError {
    fn from(value: chrono::ParseError) -> Self {
        ParseError::ChronoParseError(value)
    }
}

impl From<ParseError> for Error {
    fn from(value: ParseError) -> Self {
        Error::ReplicationError(format!("{:?}", value))
    }
}

#[derive(BinRead, Debug)]
#[br(import(size: u32))]
pub struct StringWithSize {
    #[br(count = size)]
    pub string: Vec<u8>,
}

fn parse_timestamp_tz(ts: u64) -> DateTime<Utc> {
    let timestamp = chrono::Duration::microseconds(ts as i64);
    let postgres_epoch = chrono::DateTime::<chrono::Utc>::from_timestamp(946_684_800, 0).unwrap();
    postgres_epoch + timestamp
}

fn parse_string(s: NullString) -> String {
    s.to_string()
}

#[derive(BinRead, Debug)]
pub struct MessageBegin {
    pub final_lsn: u64,
    #[br(map = parse_timestamp_tz)]
    pub commit_timestamp: DateTime<Utc>,
    pub transaction_id: u32,
}

#[derive(BinRead, Debug)]
pub struct Message {
    pub transaction_id: Option<u32>,
    pub flags: u8,
    pub lsn: u64,
    #[br(map = parse_string)]
    pub prefix: String,
    pub length: u32,
    #[br(count = length)]
    pub content: Vec<u8>,
}

#[derive(BinRead, Debug)]
pub struct MessageCommit {
    pub flags: u8,
    pub lsn: u64,
    pub end_lsn: u64,
    #[br(map = parse_timestamp_tz)]
    pub commit_timestamp: DateTime<Utc>,
}

#[derive(BinRead, Debug)]
pub struct MessageOrigin {
    pub lsn: u64,
    pub size: u32,
    #[br(map = parse_string)]
    pub name: String,
}

#[derive(BinRead, Debug)]
pub struct MessageRelation {
    pub transaction_id: u16,
    pub relation_oid: u16,
    // namespace_size: u32,
    #[br(map = parse_string)]
    pub namespace: String,
    // relation_name_size: u32,
    #[br(map = parse_string)]
    pub relation_name: String,
    pub replica_identity_setting: u8,
    pub number_of_columns: u16,
    #[br(count = number_of_columns)]
    pub columns: Vec<Column>,
}

#[derive(BinRead, Debug)]
pub struct MessageType {
    pub transaction_id: Option<u32>,
    pub type_oid: u32,
    pub namespace_size: u32,
    #[br(args(namespace_size))]
    pub namespace: StringWithSize,
    pub name_size: u32,
    #[br(args(name_size))]
    pub name: StringWithSize,
}

#[derive(BinRead, Debug)]
pub struct MessageInsert {
    pub transaction_id: u16,
    pub relation_oid: u16,
    #[br(magic = b'N')]
    pub new_tuple: TupleData,
}

#[derive(BinRead, Debug)]
pub struct MessageUpdate {
    pub transaction_id: u16,
    pub relation_oid: u16,
    #[br(try)]
    pub key_or_old_tuple: Option<KeyOrOldTupleData>,
    #[br(magic = b'N')]
    pub new_tuple: TupleData,
}

#[derive(BinRead, Debug)]
pub struct MessageDelete {
    pub transaction_id: u16,
    pub relation_oid: u16,
    #[br(try)]
    pub key_or_old_tuple: Option<KeyOrOldTupleData>,
}

#[derive(BinRead, Debug)]
pub struct MessageTruncate {
    pub transaction_id: Option<u32>,
    pub number_of_relations: u32,
    pub option_bits: u8,
    #[br(count = number_of_relations)]
    pub relation_oids: Vec<u32>,
}

#[derive(BinRead, Debug)]
pub struct MessageStreamStart {
    pub transaction_id: u32,
    pub is_first_segment: u8,
}

#[derive(BinRead, Debug)]
pub struct MessageStreamCommit {
    pub transaction_id: u32,
    pub flags: u8,
    pub lsn: u64,
    pub end_lsn: u64,
    pub commit_timestamp: u64,
}

#[derive(BinRead, Debug)]
pub struct MessageStreamAbort {
    pub transaction_id: u32,
    pub subtransaction_id: u32,
    pub lsn: u64,
    pub abort_timestamp: u64,
}

#[derive(BinRead, Debug)]
pub struct MessageBeginPrepare {
    pub prepare_lsn: u64,
    pub end_lsn: u64,
    pub prepare_timestamp: u64,
    pub transaction_id: u32,
    pub gid_size: u32,
    #[br(args(gid_size))]
    pub gid: StringWithSize,
}

#[derive(BinRead, Debug)]
pub struct MessagePrepare {
    pub flags: u8,
    pub prepare_lsn: u64,
    pub end_lsn: u64,
    pub prepare_timestamp: u64,
    pub transaction_id: u32,
    pub gid_size: u32,
    #[br(args(gid_size))]
    pub gid: StringWithSize,
}

#[derive(BinRead, Debug)]
pub struct MessageCommitPrepared {
    pub flags: u8,
    pub commit_lsn: u64,
    pub end_lsn: u64,
    pub commit_timestamp: u64,
    pub transaction_id: u32,
    pub gid_size: u32,
    #[br(args(gid_size))]
    pub gid: StringWithSize,
}

#[derive(BinRead, Debug)]
pub struct MessageRollbackPrepared {
    pub flags: u8,
    pub prepare_end_lsn: u64,
    pub rollback_end_lsn: u64,
    pub prepare_timestamp: u64,
    pub rollback_timestamp: u64,
    pub transaction_id: u32,
    pub gid_size: u32,
    #[br(args(gid_size))]
    pub gid: StringWithSize,
}

#[derive(BinRead, Debug)]
pub struct MessageStreamPrepare {
    pub flags: u8,
    pub prepare_lsn: u64,
    pub end_lsn: u64,
    pub prepare_timestamp: u64,
    pub transaction_id: u32,
    pub gid_size: u32,
    #[br(args(gid_size))]
    pub gid: StringWithSize,
}

#[derive(BinRead, Debug)]
#[br(big)]
pub enum LogicalReplicationMessage {
    #[br(magic = b'B')]
    Begin(MessageBegin),
    #[br(magic = b'M')]
    Message(Message),
    #[br(magic = b'C')]
    Commit(MessageCommit),
    #[br(magic = b'O')]
    Origin(MessageOrigin),
    #[br(magic = b'R')]
    Relation(MessageRelation),
    #[br(magic = b'Y')]
    Type(MessageType),
    #[br(magic = b'I')]
    Insert(MessageInsert),
    #[br(magic = b'U')]
    Update(MessageUpdate),
    #[br(magic = b'D')]
    Delete(MessageDelete),
    #[br(magic = b'T')]
    Truncate(MessageTruncate),
    #[br(magic = b'S')]
    StreamStart(MessageStreamStart),
    #[br(magic = b'E')]
    StreamStop,
    #[br(magic = b'c')]
    StreamCommit(MessageStreamCommit),
    #[br(magic = b'A')]
    StreamAbort(MessageStreamAbort),
    #[br(magic = b'b')]
    BeginPrepare(MessageBeginPrepare),
    #[br(magic = b'P')]
    Prepare(MessagePrepare),
    #[br(magic = b'K')]
    CommitPrepared(MessageCommitPrepared),
    #[br(magic = b'r')]
    RollbackPrepared(MessageRollbackPrepared),
    #[br(magic = b'p')]
    StreamPrepare(MessageStreamPrepare),
}

#[derive(BinRead, Debug)]
pub struct Column {
    pub flags: u8,
    #[br(map = parse_string)]
    pub name: String,
    pub type_oid: u32,
    pub type_modifier: u32,
}

#[derive(BinRead, Debug)]
#[br(big)]
pub struct TupleData {
    pub number_of_columns: u16,
    #[br(count = number_of_columns)]
    pub columns: Vec<ColumnValue>,
}

#[derive(BinRead, Debug)]
pub enum ColumnValue {
    #[br(magic = b'n')]
    Null {
        length: u32,
        #[br(count = length)]
        data: Vec<u8>,
    },
    #[br(magic = b'u')]
    UnchangedToast {
        length: u32,
        #[br(count = length)]
        data: Vec<u8>,
    },
    #[br(magic = b't')]
    Text {
        length: u32,
        #[br(count = length)]
        data: Vec<u8>,
    },
    #[br(magic = b'b')]
    Binary {
        length: u32,
        #[br(count = length)]
        data: Vec<u8>,
    },
}


impl ColumnValue {
    pub fn as_bytes<'r>(&'r self) -> Result<&'r [u8], ParseError> {
        match self {
            ColumnValue::Text { data, .. } => Ok(data),
            ColumnValue::Binary { data, .. } => Ok(data),
            _ => panic!("Invalid column type"),
        }
    }

    pub fn as_str<'r>(&'r self) -> Result<&'r str, ParseError> {
        Ok(from_utf8(self.as_bytes()?)?)
    }
}

pub trait EntityRef {
    type EntityDef;

    fn entity_name() -> &'static str;
    fn id(&self) -> i64;
    fn from_entity_id(id: i64) -> Self;
}

impl<T: EntityRef> Decode for T {
    fn decode(value: &ColumnValue) -> Result<Self, ParseError> {
        Ok(match value {
            ColumnValue::Text { .. } => {
                Self::from_entity_id(Decode::decode(value)?)
            }
            ColumnValue::Binary { .. } => {
                Self::from_entity_id(Decode::decode(value)?)
            }
            _ => panic!("Invalid column type"),
        })
    }
}


pub trait Decode: Sized {
    fn decode(value: &ColumnValue) -> Result<Self, ParseError>;
}

impl Decode for Time {
    fn decode(value: &ColumnValue) -> Result<Self, ParseError> {
        Ok(match value {
            ColumnValue::Text { .. } => {
                let s = value.as_str()?;
                let dt = NaiveDateTime::parse_from_str(
                    s,
                    if s.contains('+') {
                        // Contains a time-zone specifier
                        // This is given for timestamptz for some reason
                        // Postgres already guarantees this to always be UTC
                        "%Y-%m-%d %H:%M:%S%.f%#z"
                    } else {
                        "%Y-%m-%d %H:%M:%S%.f"
                    },
                )?;

                Time::from_naive_datetime(dt)
            }
            ColumnValue::Binary { .. } => {
                let us: i64 = Decode::decode(value)?;
                Time(POSTGRES_EPOCH + us / 1_000_000)
            }
            _ => panic!("Invalid column type"),
        })
    }
}

impl Decode for i64 {
    fn decode(value: &ColumnValue) -> Result<Self, ParseError> {
        Ok(match value {
            ColumnValue::Text { .. } => value.as_str()?.parse()?,
            ColumnValue::Binary { data, .. } => BigEndian::read_int(&data, data.len()),
            _ => panic!("Invalid column type"),
        })
    }
}

impl Decode for f32 {
    fn decode(value: &ColumnValue) -> Result<Self, ParseError> {
        Ok(match value {
            ColumnValue::Text { .. } => value.as_str()?.parse()?,
            ColumnValue::Binary { data, .. } => BigEndian::read_f32(&data),
            _ => panic!("Invalid column type"),
        })
    }
}

impl Decode for f64 {
    fn decode(value: &ColumnValue) -> Result<Self, ParseError> {
        Ok(match value {
            ColumnValue::Text { .. } => value.as_str()?.parse()?,
            ColumnValue::Binary { data, .. } => BigEndian::read_f64(&data),
            _ => panic!("Invalid column type"),
        })
    }
}

#[derive(BinRead, Debug)]
#[br(big)]
pub enum KeyOrOldTupleData {
    #[br(magic = b'K')]
    Key(TupleData),
    #[br(magic = b'O')]
    Old(TupleData),
}

pub fn decode(msg: &[u8]) -> Result<LogicalReplicationMessage, binrw::Error> {
    LogicalReplicationMessage::read(&mut binrw::io::Cursor::new(msg))
}

