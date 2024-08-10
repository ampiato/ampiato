use std::ops::Add;

use chrono::{DateTime, FixedOffset, Utc};
use petgraph::matrix_graph::NodeIndex;

pub type Index = NodeIndex<usize>;

// #[derive(Debug, Clone)]
// pub struct V<T: Clone> {
//     val: T,
//     refs: HashSet<Index>,
// }

// impl<T: Clone> V<T> {
//     pub fn new_with_refs(val: T, refs: HashSet<Index>) -> Self {
//         V { val, refs }
//     }

//     pub fn new_with_ref(val: T, r#ref: Index) -> Self {
//         V {
//             val,
//             refs: HashSet::from_iter([r#ref]),
//         }
//     }

//     pub fn get_val(&self) -> &T {
//         &self.val
//     }

//     pub fn get_refs(&self) -> &HashSet<Index> {
//         &self.refs
//     }
// }

// pub fn join_refs<T1: Clone, T2: Clone>(a: &V<T1>, b: &V<T2>) -> HashSet<Index> {
//     a.get_refs().union(b.get_refs()).cloned().collect()
// }

// pub type F64 = V<f64>;
// pub type OptionF64 = V<Option<f64>>;
// pub type Bool = V<bool>;

// impl Mul for F64 {
//     type Output = F64;

//     fn mul(self, rhs: Self) -> Self::Output {
//         let val = self.get_val() * rhs.get_val();
//         F64::new_with_refs(val, join_refs(&self, &rhs))
//     }
// }

#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, sqlx::Type)]
pub struct Time(pub i64);

impl Time {
    pub fn now() -> Self {
        Time(Utc::now().timestamp())
    }

    pub fn from_string(s: &str) -> Result<Self, chrono::ParseError> {
        Ok(Time(
            DateTime::<FixedOffset>::parse_from_rfc3339(s)?.timestamp(),
        ))
    }

    pub fn from_datetime(dt: DateTime<Utc>) -> Self {
        Time(dt.timestamp())
    }

    pub fn from_naive_datetime(dt: chrono::NaiveDateTime) -> Self {
        Time(DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc).timestamp())
    }

    pub fn from_timestamp(timestamp: i64) -> Self {
        Time(timestamp)
    }

    pub fn timestamp(&self) -> i64 {
        self.0
    }

    pub fn as_datetime(&self) -> DateTime<Utc> {
        DateTime::<Utc>::from_timestamp(self.0, 0).unwrap()
    }

    pub fn as_naive_datetime(&self) -> chrono::NaiveDateTime {
        self.as_datetime().naive_utc()
    }

    pub fn as_naive_date(&self) -> chrono::NaiveDate {
        self.as_naive_datetime().date()
    }

    pub fn as_naive_time(&self) -> chrono::NaiveTime {
        self.as_naive_datetime().time()
    }
}

impl ToString for Time {
    fn to_string(&self) -> String {
        self.as_datetime().to_rfc3339()
    }
}

impl From<i64> for Time {
    fn from(i: i64) -> Self {
        Time(i)
    }
}

impl Add for Time {
    type Output = Time;

    fn add(self, rhs: Self) -> Self::Output {
        Time(self.0 + rhs.0)
    }
}

impl Add<i64> for Time {
    type Output = Time;

    fn add(self, rhs: i64) -> Self::Output {
        Time(self.0 + rhs)
    }
}

impl std::fmt::Debug for Time {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let datetime = DateTime::<Utc>::from_timestamp(self.0, 0).unwrap();
        write!(f, "{}", datetime.naive_utc())
    }
}
