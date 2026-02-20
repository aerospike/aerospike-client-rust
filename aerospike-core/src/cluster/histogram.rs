// Copyright 2015-2018 Aerospike, Inc.
//
// Portions may be licensed to Aerospike, Inc. under one or more contributor
// license agreements.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

#[cfg(feature = "serialization")]
use serde::Serialize;

use num_traits::{Num, NumCast, ToPrimitive, Zero};
use std::fmt;
use std::ops::AddAssign;

use crate::errors::{Error::ClientError, Result};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[cfg_attr(feature = "serialization", derive(Serialize))]
pub enum Type {
    Linear,
    Logarithmic,
}

trait HVal: Num + NumCast + ToPrimitive + Copy + PartialOrd + Zero {}
impl<T> HVal for T where T: Num + NumCast + ToPrimitive + Copy + PartialOrd + Zero {}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serialization", derive(Serialize))]
pub struct Histogram<T: HVal> {
    pub htype: Type,
    pub base: T,

    pub buckets: Vec<u64>,
    pub min: T,
    pub max: T,
    pub sum: f64,
    pub count: u64,
}

impl<T: HVal> Histogram<T> {
    /// Creates new Histogram.
    pub fn new(htype: Type, base: T, buckets: usize) -> Self {
        Self {
            htype,
            base,
            buckets: vec![0; buckets],
            min: T::zero(),
            max: T::zero(),
            sum: 0.0,
            count: 0,
        }
    }

    /// Creates a new linear histogram.
    pub fn new_linear(base: T, buckets: usize) -> Self {
        Self::new(Type::Linear, base, buckets)
    }

    /// Creates a new exponential histogram.
    pub fn new_exponential(base: T, buckets: usize) -> Self {
        Self::new(Type::Logarithmic, base, buckets)
    }

    /// Clears the histogram.
    pub fn reset(&mut self) {
        for v in &mut self.buckets {
            *v = 0;
        }
        self.min = T::zero();
        self.max = T::zero();
        self.sum = 0.0;
        self.count = 0;
    }

    /// Resets and changes the parameters of the histogram.
    pub fn reshape(&mut self, htype: Type, base: T, buckets: usize) {
        if self.htype == htype && self.base == base && self.buckets.len() == buckets {
            return;
        }

        self.htype = htype;
        self.base = base;
        self.buckets = vec![0; buckets];
        self.reset();
    }

    /// Merges another histogram into this one.
    pub fn merge(&mut self, other: &Histogram<T>) -> Result<()> {
        if self.base != other.base
            || self.htype != other.htype
            || self.buckets.len() != other.buckets.len()
        {
            return Err(ClientError("Histograms do not match".to_string()));
        }

        if other.min < self.min || self.min == T::zero() {
            self.min = other.min;
        }

        if other.max > self.max {
            self.max = other.max;
        }

        self.sum += other.sum;
        self.count += other.count;

        for i in 0..self.buckets.len() {
            self.buckets[i] += other.buckets[i];
        }

        Ok(())
    }

    /// Return the average value in the histogram.
    pub fn average(&self) -> f64 {
        if self.count > 0 {
            self.sum / self.count as f64
        } else {
            0.0
        }
    }

    /// Find the median value in the histogram.
    pub fn median(&self) -> T {
        let mut s = 0;
        let c = self.count / 2;

        for (i, bv) in self.buckets.iter().enumerate() {
            s += *bv;
            if s >= c {
                match self.htype {
                    Type::Linear => {
                        let v: f64 = (i as f64 + 1.0) * self.base.to_f64().unwrap();
                        return NumCast::from(v).unwrap();
                    }
                    Type::Logarithmic => {
                        let v = self.base.to_f64().unwrap().powf(i as f64 + 1.0);
                        return NumCast::from(v).unwrap();
                    }
                }
            }
        }

        self.max
    }

    /// Puts and new value in the histogram.
    pub fn put(&mut self, v: T) {
        if self.count == 0 {
            self.max = v;
            self.min = v;
        } else {
            if v > self.max {
                self.max = v;
            } else if v < self.min {
                self.min = v;
            }
        }

        self.sum += v.to_f64().unwrap();
        self.count += 1;

        let mut slot: isize = 0;

        if v > T::zero() {
            match self.htype {
                Type::Linear => {
                    let div = v.to_f64().unwrap() / self.base.to_f64().unwrap();
                    slot = div.floor() as isize;
                }
                Type::Logarithmic => {
                    let v = v.to_f64().unwrap();
                    let base = self.base.to_f64().unwrap();
                    slot = (v.ln() / base.ln()).floor() as isize;
                }
            }
        }

        if slot as usize >= self.buckets.len() {
            *self.buckets.last_mut().unwrap() += 1;
        } else if slot < 0 {
            self.buckets[0] += 1;
        } else {
            self.buckets[slot as usize] += 1;
        }
    }
}

impl<T: HVal> fmt::Display for Histogram<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.htype {
            Type::Linear => {
                for i in 0..self.buckets.len() - 1 {
                    let v = self.base.to_f64().unwrap() * i as f64;
                    writeln!(
                        f,
                        "[{}, {}) => {}",
                        v,
                        v + self.base.to_f64().unwrap(),
                        self.buckets[i]
                    )?;
                }
                writeln!(
                    f,
                    "[{}, inf) => {}",
                    self.base.to_f64().unwrap() * (self.buckets.len() - 1) as f64,
                    self.buckets.last().unwrap()
                )
            }
            Type::Logarithmic => {
                writeln!(
                    f,
                    "[0, {}) => {}",
                    self.base.to_f64().unwrap(),
                    self.buckets[0]
                )?;

                for i in 1..self.buckets.len() - 1 {
                    let v = self.base.to_f64().unwrap().powf(i as f64);
                    writeln!(
                        f,
                        "[{}, {}) => {}",
                        v,
                        v * self.base.to_f64().unwrap(),
                        self.buckets[i]
                    )?;
                }

                writeln!(
                    f,
                    "[{}, inf) => {}",
                    self.base
                        .to_f64()
                        .unwrap()
                        .powf((self.buckets.len() - 1) as f64),
                    self.buckets.last().unwrap()
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ================================
    // Integer Values — Linear
    // ================================

    #[test]
    fn linear_histogram_correct() {
        let l = vec![1, 1, 3, 4, 5, 5, 9, 11, 11, 11, 16, 16, 21];
        let mut h = Histogram::new(Type::Linear, 5i32, 5);

        let mut sum = 0;
        for v in &l {
            sum += v;
            h.put(*v);
        }

        assert_eq!(h.min, 1);
        assert_eq!(h.max, 21);
        assert_eq!(h.count, l.len() as u64);
        assert_eq!(h.sum, sum as f64);
        assert_eq!(h.buckets, vec![4, 3, 3, 2, 1]);
    }

    #[test]
    fn linear_histogram_median() {
        let l = vec![
            1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000, 11000, 12000, 13000,
        ];
        let mut h = Histogram::new(Type::Linear, 1000i32, 10);

        let mut sum = 0;
        for v in &l {
            sum += v;
            h.put(*v);
        }

        assert_eq!(h.min, 1000);
        assert_eq!(h.max, 13000);
        assert_eq!(h.count, l.len() as u64);
        assert_eq!(h.sum, sum as f64);
        assert_eq!(h.buckets, vec![0, 1, 1, 1, 1, 1, 1, 1, 1, 5]);
        assert_eq!(h.median(), 7000);
    }

    // ================================
    // Integer Values — Logarithmic
    // ================================

    #[test]
    fn exponential_histogram_correct() {
        let l: Vec<i32> = (0..=20).collect();
        let mut h = Histogram::new(Type::Logarithmic, 2i32, 5);

        let mut sum = 0;
        for v in &l {
            sum += v;
            h.put(*v);
        }

        assert_eq!(h.min, 0);
        assert_eq!(h.max, 20);
        assert_eq!(h.count, l.len() as u64);
        assert_eq!(h.sum, sum as f64);
        assert_eq!(h.buckets, vec![2, 2, 4, 8, 5]);
    }

    #[test]
    fn exponential_histogram_barriers() {
        let l = vec![
            0, 1, 2, 3, 4, 5, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129, 255, 256,
            257, 511, 512, 513, 1023, 1024, 1025,
        ];
        let mut h = Histogram::new(Type::Logarithmic, 4i32, 8);

        let mut sum = 0;
        for v in &l {
            sum += v;
            h.put(*v);
        }

        assert_eq!(h.min, 0);
        assert_eq!(h.max, 1025);
        assert_eq!(h.count, l.len() as u64);
        assert_eq!(h.sum, sum as f64);
        assert_eq!(h.buckets, vec![4, 6, 6, 6, 6, 2, 0, 0]);
    }

    #[test]
    fn exponential_histogram_median() {
        let l = vec![
            10_000, 12_000, 3_000, 4_000, 50_000, 600_000, 75_000, 7_000, 21_000, 11_000, 113_000,
            29_000, 189_000,
        ];
        let mut h = Histogram::new(Type::Logarithmic, 2i32, 18);

        let mut sum = 0;
        for v in &l {
            sum += v;
            h.put(*v);
        }

        assert_eq!(h.min, 3_000);
        assert_eq!(h.max, 600_000);
        assert_eq!(h.count, l.len() as u64);
        assert_eq!(h.sum, sum as f64);
        assert_eq!(
            h.buckets,
            vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 1, 3, 2, 1, 2, 2]
        );
        assert_eq!(h.median(), 1 << 14);
    }

    // ================================
    // Log2 Histogram
    // ================================

    #[test]
    fn log2_histogram_correct() {
        let l: Vec<u64> = (0..=20).collect();
        let mut h = Histogram::new(Type::Logarithmic, 2u64, 5);

        let mut sum: u64 = 0;
        for v in &l {
            sum += v;
            h.put(*v);
        }

        assert_eq!(h.min, 0);
        assert_eq!(h.max, 20);
        assert_eq!(h.count, l.len() as u64);
        assert_eq!(h.sum as u64, sum);
        assert_eq!(h.buckets, vec![2, 2, 4, 8, 5]);
    }

    #[test]
    fn log2_histogram_median() {
        let l = vec![
            10_000, 12_000, 3_000, 4_000, 50_000, 600_000, 75_000, 7_000, 21_000, 11_000, 113_000,
            29_000, 189_000,
        ];
        let mut h = Histogram::new(Type::Logarithmic, 2u64, 18);

        let mut sum: u64 = 0;
        for v in &l {
            sum += v;
            h.put(*v);
        }

        assert_eq!(h.min, 3_000);
        assert_eq!(h.max, 600_000);
        assert_eq!(h.count, l.len() as u64);
        assert_eq!(h.sum as u64, sum);
        assert_eq!(
            h.buckets,
            vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 1, 3, 2, 1, 2, 2]
        );
        assert_eq!(h.median(), 1 << 14);
    }
}
