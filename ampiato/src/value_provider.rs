use crate::core::defs::Time;

pub trait ValueProvider<Sel>: Sized {
    fn from_pool(pool: &sqlx::PgPool) -> impl std::future::Future<Output = Self> + Send;
    fn set_value(&mut self, name: &'static str, selector: Sel, t: Time, value: f64);
    fn get_value(&self, name: &'static str, selector: &Sel, t: &Time) -> f64;
    fn get_value_opt(&self, name: &'static str, selector: &Sel, t: &Time) -> Option<f64>;
}
