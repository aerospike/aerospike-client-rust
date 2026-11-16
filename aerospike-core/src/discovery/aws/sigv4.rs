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

//! AWS Signature Version 4 signing of one request, and the date arithmetic
//! it needs (no calendar crate: the client has none, and the two conversions
//! below are all that is needed).

#![allow(clippy::redundant_pub_crate)]

use std::collections::BTreeMap;
use std::fmt::Write as _;

pub(crate) const ALGORITHM: &str = "AWS4-HMAC-SHA256";

/// `yyyyMMddTHHmmssZ` of an epoch millisecond value.
pub(crate) fn amz_date(millis: i64) -> String {
    let secs = millis.div_euclid(1000);
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    format!(
        "{year:04}{month:02}{day:02}T{:02}{:02}{:02}Z",
        rem / 3600,
        (rem % 3600) / 60,
        rem % 60
    )
}

/// Parses `yyyy-MM-ddTHH:mm:ss[.fff]Z` into epoch milliseconds.
pub(crate) fn parse_iso8601(text: &str) -> Option<i64> {
    let text = text.trim();
    let rest = text.strip_suffix('Z')?;
    let (date, time) = rest.split_once('T')?;
    let mut date_parts = date.split('-');
    let year: i64 = date_parts.next()?.parse().ok()?;
    let month: u32 = date_parts.next()?.parse().ok()?;
    let day: u32 = date_parts.next()?.parse().ok()?;
    if date_parts.next().is_some() || !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    let (time, fraction) = match time.split_once('.') {
        Some((t, f)) => (t, Some(f)),
        None => (time, None),
    };
    let mut time_parts = time.split(':');
    let hour: i64 = time_parts.next()?.parse().ok()?;
    let minute: i64 = time_parts.next()?.parse().ok()?;
    let second: i64 = time_parts.next()?.parse().ok()?;
    if time_parts.next().is_some() || hour > 23 || minute > 59 || second > 60 {
        return None;
    }
    let millis = match fraction {
        Some(f) if !f.is_empty() && f.chars().all(|c| c.is_ascii_digit()) => {
            let padded = format!("{f:0<3}");
            padded[..3].parse::<i64>().ok()?
        }
        Some(_) => return None,
        None => 0,
    };
    let days = days_from_civil(year, month, day);
    Some((days * 86_400 + hour * 3600 + minute * 60 + second) * 1000 + millis)
}

/// Days since 1970-01-01 of a proleptic Gregorian date (Howard Hinnant's
/// algorithm).
const fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let m = m as i64;
    let d = d as i64;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// The inverse of [`days_from_civil`].
const fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// The `Authorization` header value. Header names must be lower case.
#[allow(clippy::too_many_arguments)]
pub(crate) fn authorization(
    access_key_id: &str,
    secret_access_key: &str,
    method: &str,
    path: &str,
    query: &str,
    headers: &BTreeMap<String, String>,
    payload: &[u8],
    region: &str,
    service: &str,
    amz_date: &str,
) -> String {
    let date = &amz_date[..8];
    let scope = scope(date, region, service);
    let request = canonical_request(method, path, query, headers, &hex(&sha256(payload)));
    let signature = signature(
        &signing_key(secret_access_key, date, region, service),
        &string_to_sign(amz_date, &scope, &request),
    );
    format!(
        "{ALGORITHM} Credential={access_key_id}/{scope}, SignedHeaders={}, Signature={signature}",
        signed_headers(headers)
    )
}

pub(crate) fn canonical_request(
    method: &str,
    path: &str,
    query: &str,
    headers: &BTreeMap<String, String>,
    payload_hash: &str,
) -> String {
    let mut out = String::with_capacity(256);
    let _ = write!(out, "{method}\n{path}\n{query}\n");
    for (name, value) in headers {
        let _ = writeln!(out, "{name}:{}", value.trim());
    }
    let _ = write!(out, "\n{}\n{payload_hash}", signed_headers(headers));
    out
}

pub(crate) fn signed_headers(headers: &BTreeMap<String, String>) -> String {
    headers.keys().map(String::as_str).collect::<Vec<_>>().join(";")
}

pub(crate) fn scope(date: &str, region: &str, service: &str) -> String {
    format!("{date}/{region}/{service}/aws4_request")
}

pub(crate) fn string_to_sign(amz_date: &str, scope: &str, canonical_request: &str) -> String {
    format!(
        "{ALGORITHM}\n{amz_date}\n{scope}\n{}",
        hex(&sha256(canonical_request.as_bytes()))
    )
}

pub(crate) fn signing_key(secret_access_key: &str, date: &str, region: &str, service: &str) -> Vec<u8> {
    let key = hmac(format!("AWS4{secret_access_key}").as_bytes(), date);
    let key = hmac(&key, region);
    let key = hmac(&key, service);
    hmac(&key, "aws4_request")
}

pub(crate) fn signature(signing_key: &[u8], string_to_sign: &str) -> String {
    hex(&hmac(signing_key, string_to_sign))
}

/// Percent-encodes every byte except the RFC 3986 unreserved characters.
pub(crate) fn encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 16);
    for b in value.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(char::from(b));
            }
            _ => {
                let _ = write!(out, "%{b:02X}");
            }
        }
    }
    out
}

pub(crate) fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

pub(crate) fn sha256(bytes: &[u8]) -> Vec<u8> {
    ring::digest::digest(&ring::digest::SHA256, bytes)
        .as_ref()
        .to_vec()
}

fn hmac(key: &[u8], data: &str) -> Vec<u8> {
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, key);
    ring::hmac::sign(&key, data.as_bytes()).as_ref().to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &str = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
    const EMPTY_HASH: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    fn headers(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    #[test]
    fn signing_key_derivation_example() {
        assert_eq!(
            hex(&signing_key(SECRET, "20120215", "us-east-1", "iam")),
            "f4780e2d9f65fa895f9c67b32ce1baf0b0d8a43505a000a1a9e090d414db404d"
        );
    }

    #[test]
    fn iam_list_users_example() {
        let headers = headers(&[
            ("content-type", "application/x-www-form-urlencoded; charset=utf-8"),
            ("host", "iam.amazonaws.com"),
            ("x-amz-date", "20150830T123600Z"),
        ]);
        let canonical = canonical_request(
            "GET",
            "/",
            "Action=ListUsers&Version=2010-05-08",
            &headers,
            &hex(&sha256(&[])),
        );
        assert_eq!(
            canonical,
            "GET\n/\nAction=ListUsers&Version=2010-05-08\n\
             content-type:application/x-www-form-urlencoded; charset=utf-8\n\
             host:iam.amazonaws.com\nx-amz-date:20150830T123600Z\n\n\
             content-type;host;x-amz-date\n"
                .to_string()
                + EMPTY_HASH
        );
        assert_eq!(
            hex(&sha256(canonical.as_bytes())),
            "f536975d06c0309214f805bb90ccff089219ecd68b2577efef23edd43b7e1a59"
        );
        let scope = scope("20150830", "us-east-1", "iam");
        let sts = string_to_sign("20150830T123600Z", &scope, &canonical);
        assert_eq!(
            sts,
            "AWS4-HMAC-SHA256\n20150830T123600Z\n20150830/us-east-1/iam/aws4_request\n\
             f536975d06c0309214f805bb90ccff089219ecd68b2577efef23edd43b7e1a59"
        );
        let key = signing_key(SECRET, "20150830", "us-east-1", "iam");
        assert_eq!(hex(&key), "c4afb1cc5771d871763a393e44b703571b55cc28424d1a5e86da6ed3c154a4b9");
        assert_eq!(
            signature(&key, &sts),
            "5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7"
        );
    }

    #[test]
    fn test_suite_get_vanilla() {
        let headers = headers(&[("host", "example.amazonaws.com"), ("x-amz-date", "20150830T123600Z")]);
        let canonical = canonical_request("GET", "/", "", &headers, EMPTY_HASH);
        assert_eq!(
            canonical,
            "GET\n/\n\nhost:example.amazonaws.com\nx-amz-date:20150830T123600Z\n\nhost;x-amz-date\n"
                .to_string()
                + EMPTY_HASH
        );
        let sts = string_to_sign("20150830T123600Z", &scope("20150830", "us-east-1", "service"), &canonical);
        assert_eq!(
            sts,
            "AWS4-HMAC-SHA256\n20150830T123600Z\n20150830/us-east-1/service/aws4_request\n\
             bb579772317eb040ac9ed261061d46c1f17a8133879d6129b6e1c25292927e63"
        );
        let auth = authorization(
            "AKIDEXAMPLE",
            SECRET,
            "GET",
            "/",
            "",
            &headers,
            &[],
            "us-east-1",
            "service",
            "20150830T123600Z",
        );
        assert_eq!(
            auth,
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, \
             SignedHeaders=host;x-amz-date, \
             Signature=5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31"
        );
    }

    #[test]
    fn encode_unreserved_only() {
        assert_eq!(encode("tag:aerospike-cluster"), "tag%3Aaerospike-cluster");
        assert_eq!(encode("a b~c_d.e-f/+="), "a%20b~c_d.e-f%2F%2B%3D");
        assert_eq!(encode("é"), "%C3%A9");
    }

    #[test]
    fn dates_round_trip() {
        assert_eq!(amz_date(1_440_938_160_000), "20150830T123600Z");
        assert_eq!(amz_date(0), "19700101T000000Z");
        assert_eq!(parse_iso8601("2015-08-30T12:36:00Z"), Some(1_440_938_160_000));
        assert_eq!(parse_iso8601("2015-08-30T12:36:00.5Z"), Some(1_440_938_160_500));
        assert_eq!(parse_iso8601("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(parse_iso8601("2000-02-29T23:59:59Z").map(amz_date), Some("20000229T235959Z".into()));
        assert_eq!(parse_iso8601("not a date"), None);
        assert_eq!(parse_iso8601("2015-13-30T12:36:00Z"), None);
        assert_eq!(parse_iso8601("2015-08-30T12:36:00+00:00"), None);
    }
}
