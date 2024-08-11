#![allow(unused_imports, dead_code, non_snake_case)]

use std::collections::HashMap;

use ampiato::core::BoxDynError;
use ampiato::replication::pgoutput;
use ampiato::replication::pgoutput::Decode;
use ampiato::replication::pgoutput::EntityRef;
use ampiato::replication::TableFromTupleData;
use ampiato::FromTupleData;
use ampiato::{Error, TableMetadata, TableValues};
use ampiato::{Time, TimeSeriesChanges, TimeSeriesDense, ValueProvider as _};
use sqlx::Row;

pub type Db = ampiato::Db<Selector, Table, ValueProvider>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, sqlx::Type)]
pub struct Blok(i64);

impl pgoutput::EntityRef for Blok {
    type EntityDef = BlokDef;

    fn entity_name() -> &'static str {
        "Blok"
    }

    fn id(&self) -> i64 {
        self.0
    }

    fn from_entity_id(id: i64) -> Self {
        Self(id)
    }
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct BlokDef {
    pub IdBlokDef: i64,
    pub Jmeno: String,
    pub Barva: String,
}

impl BlokDef {
    pub fn query() -> &'static str {
        r#"SELECT * FROM "BlokDef" ORDER BY "IdBlokDef""#
    }
}

pub mod tables {
    use super::*;

    #[derive(Debug, Clone)]
    pub struct BlokVykon {
        // Selectors
        pub Blok: Blok,
        pub Time: Time,

        // Columns
        pub pInst: f64,
        pub pDos: f64,
        pub pMin: f64,
    }
    impl sqlx::FromRow<'_, sqlx::postgres::PgRow> for BlokVykon {
        fn from_row(row: &sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
            Ok(Self {
                Blok: Blok(row.try_get("IdBlokDef")?),
                Time: Time(row.try_get("Time")?),
                pInst: row.try_get("pInst")?,
                pDos: row.try_get("pDos")?,
                pMin: row.try_get("pMin")?,
            })
        }
    }

    impl super::TableMetadata for BlokVykon {
        fn query() -> &'static str {
            r#"            
            SELECT
                "IdBlokDef",
                EXTRACT(EPOCH FROM "Time")::BIGINT AS "Time",
                "pInst",
                "pDos",
                "pMin"
            FROM
                "BlokVykon"
            "#
        }

        fn selector_names() -> Vec<&'static str> {
            vec!["Blok"]
        }

        fn column_names() -> Vec<&'static str> {
            vec!["pInst", "pDos", "pMin"]
        }

        fn table_name() -> &'static str {
            "BlokVykon"
        }
    }

    impl FromTupleData for BlokVykon {
        fn from_tuple_data(tuple_data: &pgoutput::TupleData) -> Result<Self, Error> {
            if tuple_data.number_of_columns != 6 {
                return Err(Error::UnexpectedNumberOfColumns {
                    actual: tuple_data.number_of_columns as usize,
                    expected: 6,
                });
            }

            Ok(Self {
                Blok: Decode::decode(&tuple_data.columns[0])?,
                Time: Decode::decode(&tuple_data.columns[1])?,
                pInst: Decode::decode(&tuple_data.columns[2])?,
                pDos: Decode::decode(&tuple_data.columns[3])?,
                pMin: Decode::decode(&tuple_data.columns[4])?,
            })
        }
    }

    impl super::TableValues<Selector> for BlokVykon {
        fn time(&self) -> Time {
            self.Time
        }

        fn selector(&self) -> Selector {
            Selector::Blok(self.Blok)
        }

        fn values(&self) -> Vec<(&'static str, &f64)> {
            vec![
                ("BlokVykonpInst", &self.pInst),
                ("BlokVykonpDos", &self.pDos),
                ("BlokVykonpMin", &self.pMin),
            ]
        }
    }

    #[derive(Debug, Clone)]
    pub struct BlokVS {
        // Selectors
        pub Blok: Blok,
        pub Time: Time,

        // Columns
        pub Abs: f64,
    }
    impl sqlx::FromRow<'_, sqlx::postgres::PgRow> for BlokVS {
        fn from_row(row: &sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
            Ok(Self {
                Blok: Blok(row.try_get("IdBlokDef")?),
                Time: Time(row.try_get("Time")?),
                Abs: row.try_get("Abs")?,
            })
        }
    }

    impl super::TableMetadata for BlokVS {
        fn query() -> &'static str {
            r#"            
            SELECT
                "IdBlokDef",
                EXTRACT(EPOCH FROM "Time")::BIGINT AS "Time",
                "Abs"
            FROM
                "BlokVS"
            "#
        }

        fn selector_names() -> Vec<&'static str> {
            vec!["Blok"]
        }

        fn column_names() -> Vec<&'static str> {
            vec!["Abs"]
        }

        fn table_name() -> &'static str {
            "BlokVS"
        }
    }

    impl FromTupleData for BlokVS {
        fn from_tuple_data(tuple_data: &pgoutput::TupleData) -> Result<Self, Error> {
            if tuple_data.number_of_columns != 4 {
                return Err(Error::UnexpectedNumberOfColumns {
                    actual: tuple_data.number_of_columns as usize,
                    expected: 4,
                });
            }

            Ok(Self {
                Blok: Decode::decode(&tuple_data.columns[0])?,
                Time: Decode::decode(&tuple_data.columns[1])?,
                Abs: Decode::decode(&tuple_data.columns[2])?,
            })
        }
    }

    impl super::TableValues<Selector> for BlokVS {
        fn time(&self) -> Time {
            self.Time
        }

        fn selector(&self) -> Selector {
            Selector::Blok(self.Blok)
        }

        fn values(&self) -> Vec<(&'static str, &f64)> {
            vec![("BlokVSAbs", &self.Abs)]
        }
    }

    #[derive(Debug, Clone)]
    pub struct Market {
        // Selectors
        pub Time: Time,

        // Columns
        pub CzkEur: f64,
        pub cEle: f64,
    }
    impl sqlx::FromRow<'_, sqlx::postgres::PgRow> for Market {
        fn from_row(row: &sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
            Ok(Self {
                Time: Time(row.try_get("Time")?),
                CzkEur: row.try_get("CzkEur")?,
                cEle: row.try_get("cEle")?,
            })
        }
    }

    impl super::TableMetadata for Market {
        fn query() -> &'static str {
            r#"            
            SELECT
                EXTRACT(EPOCH FROM "Time")::BIGINT AS "Time",
                "CzkEur",
                "cEle"
            FROM
                "Market"
            "#
        }

        fn selector_names() -> Vec<&'static str> {
            vec![]
        }

        fn column_names() -> Vec<&'static str> {
            vec!["CzkEur", "cEle"]
        }

        fn table_name() -> &'static str {
            "Market"
        }
    }

    impl FromTupleData for Market {
        fn from_tuple_data(tuple_data: &pgoutput::TupleData) -> Result<Self, Error> {
            if tuple_data.number_of_columns != 4 {
                return Err(Error::UnexpectedNumberOfColumns {
                    actual: tuple_data.number_of_columns as usize,
                    expected: 4,
                });
            }

            Ok(Self {
                Time: Decode::decode(&tuple_data.columns[0])?,
                CzkEur: Decode::decode(&tuple_data.columns[1])?,
                cEle: Decode::decode(&tuple_data.columns[2])?,
            })
        }
    }

    impl super::TableValues<Selector> for Market {
        fn time(&self) -> Time {
            self.Time
        }

        fn selector(&self) -> Selector {
            Selector::Unit(())
        }

        fn values(&self) -> Vec<(&'static str, &f64)> {
            vec![("MarketCzkEur", &self.CzkEur), ("MarketcEle", &self.cEle)]
        }
    }
}
#[derive(Debug)]
pub enum Table {
    BlokVykon(tables::BlokVykon),
    BlokVS(tables::BlokVS),
    Market(tables::Market),
}

impl TableFromTupleData for Table {
    fn from_tuple_data(
        relation_name: &str,
        tuple_data: &pgoutput::TupleData,
    ) -> Result<Self, Error> {
        match relation_name {
            "BlokVykon" => Ok(Table::BlokVykon(tables::BlokVykon::from_tuple_data(
                tuple_data,
            )?)),
            "BlokVS" => Ok(Table::BlokVS(tables::BlokVS::from_tuple_data(tuple_data)?)),
            "Market" => Ok(Table::Market(tables::Market::from_tuple_data(tuple_data)?)),
            table_name => Err(Error::UnknownTable {
                table_name: table_name.to_string(),
            }),
        }
    }
}

impl TableValues<Selector> for Table {
    fn time(&self) -> Time {
        match self {
            Table::BlokVykon(t) => t.time(),
            Table::BlokVS(t) => t.time(),
            Table::Market(t) => t.time(),
        }
    }

    fn selector(&self) -> Selector {
        match self {
            Table::BlokVykon(t) => t.selector(),
            Table::BlokVS(t) => t.selector(),
            Table::Market(t) => t.selector(),
        }
    }

    fn values(&self) -> Vec<(&'static str, &f64)> {
        match self {
            Table::BlokVykon(t) => t.values(),
            Table::BlokVS(t) => t.values(),
            Table::Market(t) => t.values(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Selector {
    Blok(Blok),
    Unit(()),
}

#[derive(Debug)]
pub struct ValueProvider {
    Blok: HashMap<String, BlokDef>,
    BlokVykonpInst: HashMap<Selector, TimeSeriesChanges<f64>>,
    BlokVykonpDos: HashMap<Selector, TimeSeriesChanges<f64>>,
    BlokVykonpMin: HashMap<Selector, TimeSeriesChanges<f64>>,
    BlokVSAbs: HashMap<Selector, TimeSeriesChanges<f64>>,
    MarketCzkEur: HashMap<Selector, TimeSeriesDense<f64>>,
    MarketcEle: HashMap<Selector, TimeSeriesDense<f64>>,
}

impl ValueProvider {
    pub fn new() -> Self {
        Self {
            Blok: HashMap::new(),
            BlokVykonpInst: HashMap::new(),
            BlokVykonpDos: HashMap::new(),
            BlokVykonpMin: HashMap::new(),
            BlokVSAbs: HashMap::new(),
            MarketCzkEur: HashMap::new(),
            MarketcEle: HashMap::new(),
        }
    }

    pub fn get_entity_def_Blok(&self, name: &str) -> Option<&BlokDef> {
        self.Blok.get(name)
    }

    pub fn get_entity_Blok(&self, name: &str) -> Option<Blok> {
        let entity_def = self.get_entity_def_Blok(name)?;
        Some(Blok(entity_def.IdBlokDef))
    }

    fn _get_value_impl(&self, name: &'static str, selector: &Selector, t: &Time) -> Option<f64> {
        match name {
            "BlokVykonpInst" => self.BlokVykonpInst.get(selector)?.get(t),
            "BlokVykonpDos" => self.BlokVykonpDos.get(selector)?.get(t),
            "BlokVykonpMin" => self.BlokVykonpMin.get(selector)?.get(t),
            "BlokVSAbs" => self.BlokVSAbs.get(selector)?.get(t),
            "MarketCzkEur" => self.MarketCzkEur.get(selector)?.get(t),
            "MarketcEle" => self.MarketcEle.get(selector)?.get(t),
            _ => panic!("Unknown quantity {}", name),
        }
    }
}

impl ampiato::ValueProvider<Selector> for ValueProvider {
    async fn from_pool(pool: &sqlx::PgPool) -> Self {
        let vp = load_value_provider(pool).await;
        vp
    }

    fn set_value(&mut self, name: &'static str, selector: Selector, t: Time, value: f64) {
        match name {
            "BlokVykonpInst" => self
                .BlokVykonpInst
                .entry(selector)
                .or_default()
                .set(&t, value),
            "BlokVykonpDos" => self
                .BlokVykonpDos
                .entry(selector)
                .or_default()
                .set(&t, value),
            "BlokVykonpMin" => self
                .BlokVykonpMin
                .entry(selector)
                .or_default()
                .set(&t, value),
            "BlokVSAbs" => self.BlokVSAbs.entry(selector).or_default().set(&t, value),
            "MarketCzkEur" => self
                .MarketCzkEur
                .entry(selector)
                .or_default()
                .set(&t, value),
            "MarketcEle" => self.MarketcEle.entry(selector).or_default().set(&t, value),
            name => panic!("Unknown quantity {}", name),
        }
    }

    fn get_value(&self, name: &'static str, selector: &Selector, t: &Time) -> f64 {
        match self._get_value_impl(name, selector, t) {
            Some(v) => v,
            None => panic!("Value not found: {}({:?})", name, selector),
        }
    }

    fn get_value_opt(&self, name: &'static str, selector: &Selector, t: &Time) -> Option<f64> {
        self._get_value_impl(name, selector, t)
    }
}

pub mod BlokVykon {
    use super::{Blok, Db, Selector};
    use ampiato::Time;

    pub fn pInst(db: &Db, b: Blok, t: Time) -> f64 {
        db.get_value("BlokVykonpInst", Selector::Blok(b), t)
    }

    pub fn pDos(db: &Db, b: Blok, t: Time) -> f64 {
        db.get_value("BlokVykonpDos", Selector::Blok(b), t)
    }

    pub fn pMin(db: &Db, b: Blok, t: Time) -> f64 {
        db.get_value("BlokVykonpMin", Selector::Blok(b), t)
    }
}

pub mod BlokVS {
    use super::{Blok, Db, Selector};
    use ampiato::Time;

    pub fn Abs(db: &Db, b: Blok, t: Time) -> f64 {
        db.get_value("BlokVSAbs", Selector::Blok(b), t)
    }
}

pub mod Market {
    use super::{Blok, Db, Selector};
    use ampiato::Time;

    pub fn CzkEur(db: &Db, t: Time) -> f64 {
        db.get_value("MarketCzkEur", Selector::Unit(()), t)
    }

    pub fn cEle(db: &Db, t: Time) -> f64 {
        db.get_value("MarketcEle", Selector::Unit(()), t)
    }
}

pub async fn load_value_provider(pool: &sqlx::PgPool) -> ValueProvider {
    let mut vp = ValueProvider::new();
    let rows = sqlx::query_as::<_, BlokDef>(&BlokDef::query())
        .fetch_all(pool)
        .await
        .unwrap();
    for row in rows {
        vp.Blok.insert(row.Jmeno.clone(), row);
    }
    let rows = sqlx::query_as::<_, tables::BlokVykon>(&tables::BlokVykon::query())
        .fetch_all(pool)
        .await
        .unwrap();
    for row in rows {
        let sel = row.selector();
        for (name, value) in row.values() {
            vp.set_value(name, sel, row.Time, *value);
        }
    }
    let rows = sqlx::query_as::<_, tables::BlokVS>(&tables::BlokVS::query())
        .fetch_all(pool)
        .await
        .unwrap();
    for row in rows {
        let sel = row.selector();
        for (name, value) in row.values() {
            vp.set_value(name, sel, row.Time, *value);
        }
    }
    let rows = sqlx::query_as::<_, tables::Market>(&tables::Market::query())
        .fetch_all(pool)
        .await
        .unwrap();
    for row in rows {
        let sel = row.selector();
        for (name, value) in row.values() {
            vp.set_value(name, sel, row.Time, *value);
        }
    }
    vp
}

pub mod prelude {
    pub use super::Blok;
    pub use super::BlokVS::Abs;
    pub use super::BlokVykon::{pDos, pInst, pMin};
    pub use super::Market::{cEle, CzkEur};
    pub use super::{load_value_provider, Db, Selector, Table, ValueProvider};
    pub use ampiato::ast::*;
    pub use ampiato::prelude::*;
    pub use ampiato::Time;
}
