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

use std::convert::AsRef;
use std::collections::HashMap;
use std::env;
use std::str::FromStr;

use clap::{App, Arg};
use num_cpus;

use crate::db_object_spec::{parse_object_spec_list, DBObjectSpec};
use crate::workers::Workload;

const AFTER_HELP: &str = r###"

SETTING SEED HOSTS:

The list of seed hosts can be specified using -h/--hosts or by
setting the AEROSPIKE_HOSTS environment variable. The format is:

    <hostname_or_ip>[:<port>][,<hostname_or_ip>[:<port>][,...]]

If no port is specified, the default port is used.
IPv6 addresses must be enclosed in square brackets.

SELECTING WORKLOADS

The -w/--workload parameter is used to select the desired workload for the
benchmark:

* Insert workload (-w I)
* Read workload (-w RU)

"###;

lazy_static! {
    pub static ref NUM_CPUS: String = format!("{}", num_cpus::get());
}

#[derive(Debug)]
pub struct Options {
    pub hosts: String,
    pub namespace: String,
    pub set: String,
    pub keys: i64,
    pub start_key: i64,
    pub concurrency: i64,
    pub workload: Workload,
    pub conn_pools_per_node: usize,
    pub use_services_alternate: bool,
    pub ip_map: Option<HashMap<String, String>>,
    pub bins: usize,
    pub bin_name_base: String,
    pub object_specs: Vec<DBObjectSpec>,
}

pub fn parse_options() -> Result<Options, String> {
    let matches = build_cli().get_matches();
    let ip_map_str = matches
        .value_of("ip_map")
        .map(|s| s.to_owned())
        .or_else(|| env::var("AEROSPIKE_IP_MAP").ok());

    Ok(Options {
        hosts: matches
            .value_of("hosts")
            .map(|s| s.to_owned())
            .or_else(|| env::var("AEROSPIKE_HOSTS").ok())
            .unwrap_or_else(|| String::from("127.0.0.1:3000")),
        namespace: matches.value_of("namespace").unwrap().to_owned(),
        set: matches.value_of("set").unwrap().to_owned(),
        keys: i64::from_str(matches.value_of("keys").unwrap()).unwrap(),
        start_key: i64::from_str(matches.value_of("startkey").unwrap()).unwrap(),
        concurrency: i64::from_str(matches.value_of("concurrency").unwrap()).unwrap(),
        workload: Workload::from_str(matches.value_of("workload").unwrap()).unwrap(),
        conn_pools_per_node: usize::from_str(matches.value_of("connPoolsPerNode").unwrap())
            .unwrap(),
        use_services_alternate: matches.is_present("use_services_alternate"),
        ip_map: ip_map_str.as_deref().map(parse_ip_map).transpose()?,
        bins: usize::from_str(matches.value_of("bins").unwrap()).unwrap(),
        bin_name_base: matches.value_of("binNameBase").unwrap().to_owned(),
        object_specs: matches
            .value_of("objectSpec")
            .map(|s| parse_object_spec_list(s).unwrap())
            .unwrap_or_else(|| vec![DBObjectSpec::default()])
    })
    
}

fn build_cli() -> App<'static, 'static> {
    App::new(crate_name!())
        .bin_name("benchmark")
        .version(crate_version!())
        .about(crate_description!())
        .arg(Arg::from_usage(
            "-h, --hosts=[hosts] 'List of seed hosts (see below)'",
        ))
        .arg(Arg::from_usage("-n, --namespace 'Aerospike namespace'").default_value("test"))
        .arg(Arg::from_usage("-s, --set 'Aerospike set name'").default_value("testset"))
        .arg(
            Arg::from_usage("-k, --keys")
                .help(
                    "Set the number of keys the client is dealing with. If using an 'insert' \
                     workload (detailed below), the client will write this number of keys, \
                     starting from value = startkey. Otherwise, the client will read and update \
                     randomly across the values between startkey and startkey + num_keys. startkey \
                     can be set using '-S' or '--startkey'.",
                )
                .validator(|val| validate::<i64>(val, "Must be number".into()))
                .default_value("100000"),
        )
        .arg(
            Arg::from_usage("-S, --startkey")
                .help(
                    "Set the starting value of the working set of keys. If using an 'insert' \
                     workload, the start_value indicates the first value to write. Otherwise, the \
                     start_value indicates the smallest value in the working set of keys.",
                )
                .validator(|val| validate::<i64>(val, "Must be number".into()))
                .default_value("0"),
        )
        .arg(
            Arg::from_usage("-c, --concurrency 'No. threads used to generate load'")
                .validator(|val| validate::<i64>(val, "Must be number".into()))
                .default_value(&*NUM_CPUS),
        )
        .arg(
            Arg::from_usage(
                "-w, --workload 'Workload definition: I | RU (see below for \
                 details)'",
            )
            .default_value("I"),
        )
        .arg(
            Arg::from_usage("-Y, --connPoolsPerNode 'Number of connection pools per node'")
                .validator(|val| validate::<usize>(val, "Must be number".into()))
                .default_value("1"),
        )
        .arg(
            Arg::with_name("use_services_alternate")
                .long("use-services-alternate")
                .help("Use server \"services-alternate\" / \"service-alternate\" addresses"),
        )
        .arg(
            Arg::with_name("ip_map")
                .long("ip_map")
                .takes_value(true)
                .help("Map advertised IPs to reachable IPs (format: from=to[,from=to...])")
                .validator(|val| parse_ip_map(&val).map(|_| ()).map_err(|e| e)),
        )
         .arg(
            Arg::from_usage("-b, --bins 'Number of bins per record'")
                .validator(|val| validate::<usize>(val, "Must be number".into()))
                .default_value("1"),
         )
        .arg(
            Arg::from_usage("--binNameBase 'Specify Prefix for bins name'")
                .default_value("testBin"),
        )
        .arg(
            Arg::from_usage("-o, --objectSpec [objectSpec] 'Comma-separated object specs: I | D | B:<size> | S:<size> | R:<bytes>:<randPct>'")
                .default_value("I")
                .validator(|val| validate_object_spec_list(val)),
        )
        .after_help(AFTER_HELP.trim())
}

fn validate<T: FromStr>(value: String, err: String) -> Result<(), String> {
    match T::from_str(value.as_ref()) {
        Ok(_) => Ok(()),
        Err(_) => Err(err.into()),
    }
}

fn parse_ip_map(spec: &str) -> Result<HashMap<String, String>, String> {
    let mut map = HashMap::new();
    let spec = spec.trim();
    if spec.is_empty() {
        return Ok(map);
    }

    for entry in spec.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let (from, to) = entry
            .split_once('=')
            .ok_or_else(|| format!("Invalid ip-map entry `{}` (expected from=to)", entry))?;
        let from = from.trim();
        let to = to.trim();
        if from.is_empty() || to.is_empty() {
            return Err(format!("Invalid ip-map entry `{}` (expected from=to)", entry));
        }
        map.insert(from.to_string(), to.to_string());
    }
    Ok(map)
}

fn validate_object_spec_list(value: String) -> Result<(), String> {
    parse_object_spec_list(value.as_ref()).map(|_| ())
}