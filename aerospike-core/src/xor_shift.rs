// Copyright 2014-2024 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! A fast `xorshift128+` pseudo-random number generator.
//!
//! [`XorShift`] is a small, very fast, **non-cryptographic** RNG with a 128-bit
//! state. Each instance is independent and holds no internal locking, so a
//! single generator must not be shared across threads without external
//! synchronization; create one per thread instead.

use rand::random;

/// A `xorshift128+` pseudo-random number generator.
///
/// Not cryptographically secure. Suitable for fast, high-volume random number
/// generation (e.g. picking nodes, jittering, generating identifiers).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct XorShift {
    state: [u64; 2],
}

impl XorShift {
    /// Creates a generator seeded from the thread-local random source.
    #[must_use]
    pub fn new() -> Self {
        XorShift {
            state: [random::<u64>(), random::<u64>()],
        }
    }

    /// Creates a generator from an explicit 128-bit seed.
    ///
    /// The two seed words must not both be zero — an all-zero state can only
    /// ever produce zero. If both are zero, a fixed non-zero state is used
    /// instead.
    #[must_use]
    pub const fn with_seed(s0: u64, s1: u64) -> Self {
        if s0 == 0 && s1 == 0 {
            // Golden-ratio constant; any non-zero state works.
            XorShift {
                state: [0x9E37_79B9_7F4A_7C15, 1],
            }
        } else {
            XorShift { state: [s0, s1] }
        }
    }

    /// Returns the next pseudo-random `u64`.
    pub const fn next_u64(&mut self) -> u64 {
        let mut s1 = self.state[0];
        let s0 = self.state[1];
        self.state[0] = s0;
        s1 ^= s1 << 23;
        self.state[1] = s1 ^ s0 ^ (s1 >> 17) ^ (s0 >> 26);
        self.state[1].wrapping_add(s0)
    }

    /// Returns the next pseudo-random `i64`. The value may be negative.
    pub const fn next_i64(&mut self) -> i64 {
        self.next_u64() as i64
    }

    /// Fills `buf` with pseudo-random bytes.
    pub fn fill_bytes(&mut self, buf: &mut [u8]) {
        let mut chunks = buf.chunks_exact_mut(8);
        for chunk in &mut chunks {
            chunk.copy_from_slice(&self.next_u64().to_le_bytes());
        }
        let remainder = chunks.into_remainder();
        if !remainder.is_empty() {
            let bytes = self.next_u64().to_le_bytes();
            remainder.copy_from_slice(&bytes[..remainder.len()]);
        }
    }
}

impl Default for XorShift {
    fn default() -> Self {
        Self::new()
    }
}

impl Iterator for XorShift {
    type Item = u64;

    fn next(&mut self) -> Option<u64> {
        Some(self.next_u64())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_reference_sequence() {
        // Pins bit-exact parity with the reference `xorshift128+` Uint64
        // routine for the seed (1, 2).
        let mut r = XorShift::with_seed(1, 2);
        assert_eq!(r.next_u64(), 8_388_677);
        assert_eq!(r.next_u64(), 33_554_692);
    }

    #[test]
    fn deterministic_for_same_seed() {
        let mut a = XorShift::with_seed(0x1234_5678, 0x9abc_def0);
        let mut b = XorShift::with_seed(0x1234_5678, 0x9abc_def0);
        for _ in 0..1000 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn zero_seed_does_not_produce_only_zeros() {
        let mut r = XorShift::with_seed(0, 0);
        let any_non_zero = (0..16).any(|_| r.next_u64() != 0);
        assert!(any_non_zero, "all-zero seed degenerated to a zero stream");
    }

    #[test]
    fn next_i64_reinterprets_bits() {
        let mut a = XorShift::with_seed(42, 99);
        let mut b = XorShift::with_seed(42, 99);
        assert_eq!(a.next_i64(), b.next_u64() as i64);
    }

    #[test]
    fn fill_bytes_fills_whole_buffer_including_tail() {
        // Length deliberately not a multiple of 8 to exercise the remainder.
        let mut buf = [0u8; 30];
        let mut r = XorShift::with_seed(7, 11);
        r.fill_bytes(&mut buf);
        assert!(buf.iter().any(|&b| b != 0), "buffer left all-zero");

        // Same seed reproduces the same bytes.
        let mut buf2 = [0u8; 30];
        XorShift::with_seed(7, 11).fill_bytes(&mut buf2);
        assert_eq!(buf, buf2);
    }

    #[test]
    fn produces_varied_values() {
        let mut r = XorShift::with_seed(0xdead_beef, 0xcafe_babe);
        let first = r.next_u64();
        let differs = (0..50).any(|_| r.next_u64() != first);
        assert!(differs);
    }
}
