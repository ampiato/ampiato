use crate::core::defs::Time;

#[derive(Debug, Default)]
pub struct TimeSeriesChanges<V: Clone> {
    index: Vec<Time>,
    values: Vec<V>,
}

impl<V: Clone + std::fmt::Debug> TimeSeriesChanges<V> {
    pub fn push(&mut self, time: Time, value: V) {
        if !self.index.is_empty() {
            assert!(*self.index.last().unwrap() < time);
        }
        self.index.push(time);
        self.values.push(value);
    }

    pub fn set(&mut self, time: &Time, new_value: V) {
        let idx = bisection::bisect_left(&self.index, time);
        if idx != self.index.len() && self.index[idx] == *time {
            // Existing item. Just update the value.
            self.values[idx] = new_value;
        } else {
            // New item. Insert it.
            self.index.insert(idx, *time);
            self.values.insert(idx, new_value);
        }
    }

    pub fn get(&self, time: &Time) -> Option<V> {
        let idx = bisection::bisect_left(&self.index, time);
        if idx == self.index.len() {
            return self.values.last().cloned();
        }
        if self.index[idx] != *time {
            return None;
        }
        Some(self.values[idx].clone())
    }
}

#[derive(Debug, Default)]
pub struct TimeSeriesDense<V: Clone> {
    index: Vec<Time>,
    values: Vec<V>,
}

impl<V: Clone> TimeSeriesDense<V> {
    pub fn push(&mut self, time: Time, value: V) {
        if !self.index.is_empty() {
            assert!(*self.index.last().unwrap() < time);
        }
        self.index.push(time);
        self.values.push(value);
    }

    pub fn set(&mut self, time: &Time, new_value: V) {
        let idx = bisection::bisect_left(&self.index, time);
        if idx != self.index.len() && self.index[idx] == *time {
            // Existing item. Just update the value.
            self.values[idx] = new_value;
        } else {
            // New item. Insert it.
            self.index.insert(idx, *time);
            self.values.insert(idx, new_value);
        }
    }

    pub fn get(&self, time: &Time) -> Option<V> {
        let idx = bisection::bisect_left(&self.index, time);
        if idx == self.index.len() {
            return None;
        }
        if self.index[idx] != *time {
            return None;
        }
        Some(self.values[idx].clone())
    }
}

pub struct TimeSeriesInterval<V: Clone> {
    _index: Vec<(Time, Time)>,
    _values: Vec<V>,
}

impl<V: Clone> TimeSeriesInterval<V> {
    pub fn push(&mut self, _start: Time, _end: Time, _value: V) {
        todo!()
    }

    pub fn get(&self, _time: &Time) -> Option<V> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    pub use super::*;

    #[test]
    fn simple_time_series_changes() {
        let mut ts = TimeSeriesChanges::<f64>::default();
        ts.push(Time(1), 1.0);
        ts.push(Time(2), 2.0);
        ts.push(Time(3), 3.0);

        assert_eq!(ts.get(&Time(0)), None);
        assert_eq!(ts.get(&Time(1)), Some(1.0));
        assert_eq!(ts.get(&Time(2)), Some(2.0));
        assert_eq!(ts.get(&Time(3)), Some(3.0));
        assert_eq!(ts.get(&Time(4)), Some(3.0));
    }
    #[test]
    fn simple_time_series_dense() {
        let mut ts = TimeSeriesDense::<f64>::default();
        ts.push(Time(1), 1.0);
        ts.push(Time(2), 2.0);
        ts.push(Time(3), 3.0);

        assert_eq!(ts.get(&Time(0)), None);
        assert_eq!(ts.get(&Time(1)), Some(1.0));
        assert_eq!(ts.get(&Time(2)), Some(2.0));
        assert_eq!(ts.get(&Time(3)), Some(3.0));
        assert_eq!(ts.get(&Time(4)), None);
    }
}
