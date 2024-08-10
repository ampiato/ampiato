use ampiato_macro::tem_fn;
use ampiato_tem::Time;
use value_provider::prelude::*;

mod value_provider;

// const H: i64 = 3600;
// const OI: i64 = 15 * 60;

fn min<T: PartialOrd>(a: T, b: T) -> T {
    if a < b {
        a
    } else {
        b
    }
}

#[tem_fn]
fn cEleCzk(db: &Db, t: Time) -> f64 {
    cEle(db, t) * CzkEur(db, t)
}

#[tem_fn]
fn pMax(db: &Db, b: Blok, t: Time) -> f64 {
    min(pInst(db, b, t), pDos(db, b, t))
}

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    let mut db = Db::from_env(true).await?;

    ampiato_tem::print_banner();

    let t = Time::from_string("2024-01-18 02:00:00+01:00").unwrap();

    let c_ele_czk = cEleCzk(&db, t);
    println!("Na počátku bylo cEleCzk: {}", c_ele_czk);

    let subscription_id = db.subscribe("cEleCzk", &Selector::Unit(()), &t);

    loop {
        let updated_subscribers = db.sync_changes().await?;

        if updated_subscribers.contains(&subscription_id) {
            let c_ele_czk = cEleCzk(&db, t);
            println!("cEleCzk: {}", c_ele_czk);
        }
    }
}
