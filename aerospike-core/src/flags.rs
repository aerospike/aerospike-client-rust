// Copyright 2015-2026 Aerospike, Inc.
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

//! The one shape every flag set and return-type selector in the operation and
//! expression builders takes.

/// A set of flags over an integer: SCREAMING constants, `|` and `|=`,
/// `bits()`, `contains()`, and an unchecked `from_bits()` so a flag the
/// server accepts before this client names it can still be sent.
macro_rules! bit_flags {
    (
        $(#[$meta:meta])*
        $vis:vis struct $name:ident($repr:ty);
        $( $(#[$fmeta:meta])* const $flag:ident = $value:expr; )*
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
        $vis struct $name($repr);

        impl $name {
            $( $(#[$fmeta])* pub const $flag: Self = Self($value); )*

            /// Flags from their raw server value. Any bit pattern is accepted,
            /// so a flag the server supports before this client names it can
            /// still be sent; the server rejects bits it does not know with
            /// `PARAMETER_ERROR`.
            #[must_use]
            pub const fn from_bits(bits: $repr) -> Self {
                Self(bits)
            }

            /// The raw bit set sent to the server.
            #[must_use]
            pub const fn bits(self) -> $repr {
                self.0
            }

            /// True when every flag in `other` is set in `self`.
            #[must_use]
            pub const fn contains(self, other: Self) -> bool {
                self.0 & other.0 == other.0
            }
        }

        impl ::core::ops::BitOr for $name {
            type Output = Self;
            fn bitor(self, rhs: Self) -> Self {
                Self(self.0 | rhs.0)
            }
        }

        impl ::core::ops::BitOrAssign for $name {
            fn bitor_assign(&mut self, rhs: Self) {
                self.0 |= rhs.0;
            }
        }
    };
}
pub(crate) use bit_flags;

/// What a CDT operation returns: one selector constant, optionally
/// [`inverted`]. Selectors are numbers, not bits, so there is no `|`;
/// `inverted()` is the only combinator.
///
/// [`inverted`]: ListReturnType::inverted
macro_rules! return_type {
    (
        $(#[$meta:meta])*
        $vis:vis struct $name:ident;
        $( $(#[$fmeta:meta])* const $sel:ident = $value:expr; )*
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
        $vis struct $name(i64);

        impl $name {
            $( $(#[$fmeta])* pub const $sel: Self = Self($value); )*

            pub(crate) const INVERTED_BIT: i64 = 0x10000;

            /// Select what the operation did *not* match: the items outside
            /// the given index, rank or value range. On a `remove_*`
            /// operation those are the items removed.
            #[must_use]
            pub const fn inverted(self) -> Self {
                Self(self.0 | Self::INVERTED_BIT)
            }

            /// Whether [`inverted`](Self::inverted) was applied.
            #[must_use]
            pub const fn is_inverted(self) -> bool {
                self.0 & Self::INVERTED_BIT != 0
            }

            /// A return type from its raw server value, for a selector the
            /// server supports before this client names it.
            #[must_use]
            pub const fn from_bits(bits: i64) -> Self {
                Self(bits)
            }

            /// The raw value sent to the server.
            #[must_use]
            pub const fn bits(self) -> i64 {
                self.0
            }
        }
    };
}
pub(crate) use return_type;
