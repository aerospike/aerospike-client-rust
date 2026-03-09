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

use std::collections::HashMap;
use std::convert::AsRef;
use std::env;
use std::str::FromStr;

use clap::{App, Arg};

use crate::{
    db_object_spec::{parse_object_spec_list, DBObjectSpec},
    stats::ReportStyle,
    workers::Workload,
};

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
* Read/Update workload (-w RU)
* Read/Replace workload (-w RR), same as RU but writes use Replace

"###;

lazy_static! {
    static ref DEFAULT_CORES: String = std::thread::available_parallelism()
        .map(|n| n.get().to_string())
        .unwrap_or_else(|_| "1".to_string());
    static ref DEFAULT_TASKS: String = std::thread::available_parallelism()
        .map(|n| (n.get() * 2).to_string())
        .unwrap_or_else(|_| "1".to_string());
}

#[derive(Debug)]
pub struct Options {
    pub hosts: String,
    pub namespace: String,
    pub set: String,
    pub keys: i64,
    pub start_key: i64,
    pub tasks: i64,
    pub cores: i64,
    pub workload: Workload,
    pub conn_pools_per_node: usize,
    pub use_services_alternate: bool,
    pub ip_map: Option<HashMap<String, String>>,
    pub bins: usize,
    pub bin_name_base: String,
    pub object_specs: Vec<DBObjectSpec>,
    pub batch_size: usize,
    pub report_style: ReportStyle,
    pub duration_secs: Option<u64>,
}

pub fn parse_options() -> Result<Options, String> {
    let matches = build_cli().get_matches();
    let ip_map_str = matches
        .value_of("ip_map")
        .map(|s| s.to_owned())
        .or_else(|| env::var("AEROSPIKE_IP_MAP").ok());

    let workload = Workload::from_str(matches.value_of("workload").unwrap_or("I"))?;

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
        tasks: i64::from_str(matches.value_of("tasks").unwrap()).unwrap(),
        cores: i64::from_str(matches.value_of("cores").unwrap()).unwrap(),
        workload,
        conn_pools_per_node: usize::from_str(matches.value_of("connPoolsPerNode").unwrap())
            .unwrap(),
        use_services_alternate: matches.is_present("use_services_alternate"),
        ip_map: ip_map_str.as_deref().map(parse_ip_map).transpose()?,
        bins: usize::from_str(matches.value_of("bins").unwrap()).unwrap(),
        bin_name_base: matches.value_of("bin_prefix").unwrap().to_owned(),
        object_specs: matches
            .value_of("object_spec")
            .map(|s| parse_object_spec_list(s).unwrap())
            .unwrap_or_else(|| vec![DBObjectSpec::default()]),
        batch_size: usize::from_str(matches.value_of("batch_size").unwrap()).unwrap(),
        report_style: parse_report_style(matches.value_of("report_style").unwrap_or("pretty")),
        duration_secs: parse_duration_secs(matches.value_of("duration"), workload)?,
    })
    .and_then(|opts| custom_validations(&opts).map(|()| opts))
}

fn parse_duration_secs(
    duration_str: Option<&str>,
    workload: Workload,
) -> Result<Option<u64>, String> {
    let parsed = duration_str
        .map(|s| {
            u64::from_str(s)
                .map_err(|_| "duration must be a positive number of seconds".to_string())
        })
        .transpose()?;
    Ok(match workload {
        Workload::Initialize => {
            if parsed.is_some() {
                return Err(
                    "duration (-d/--duration) is not allowed for Initialize (I) workload"
                        .to_string(),
                );
            }
            None
        }
        _ => parsed.or(Some(10)),
    })
}

fn parse_report_style(s: &str) -> ReportStyle {
    match s {
        "asbench" => ReportStyle::Asbench,
        _ => ReportStyle::Pretty,
    }
}

// put all custom validation here
fn custom_validations(opts: &Options) -> Result<(), String> {
    let batches_allowed = matches!(
        opts.workload,
        Workload::ReadUpdate { .. } | Workload::ReadReplace { .. }
    );
    if !batches_allowed && opts.batch_size > 1 {
        return Err(
            "batch size (-b/--batch-size) is only applicable for RU/RR workload".to_string(),
        );
    }
    if opts.duration_secs.is_some_and(|secs| secs == 0) {
        return Err("Duration must be greater than 0".to_string());
    }
    Ok(())
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
            Arg::from_usage("-t, --tasks 'No. of async tasks to be used'")
                .validator(|val| validate::<i64>(val, "Must be number".into()))
                .default_value(&*DEFAULT_TASKS),
        )
        .arg(
            Arg::from_usage("-c, --cores 'No. of CPU Cores to be used'")
                .validator(|val| validate::<i64>(val, "Must be number".into()))
                .default_value(&*DEFAULT_CORES),
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
                .long("ip-map")
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
            Arg::with_name("bin_prefix")
                .short("p")
                .long("bin-prefix")
                .help("Specify prefix for bin names")
                .default_value("testBin")
         )
        .arg(
            Arg::from_usage("-o, --object-spec 'Comma-separated object specs: I | D | B:<size> | S:<size> | R:<bytes>:<randPct>'")
                .default_value("I")
                .validator(|val| parse_object_spec_list(val.as_ref()).map(|_| ()).map_err(|e| e)),
        )
        .arg(
            Arg::with_name("batch_size")
                .short("B")
                .long("batch-size")
                .help("Applicable only for RU workload. Disabled by default")
                .default_value("1")
        )
        .arg(
            Arg::with_name("report_style")
                .long("report-style")
                .help("Output format: default (sectioned) or asbench (C benchmark one-line style)")
                .takes_value(true)
                .possible_values(&["pretty", "asbench"])
        )
        .arg(
            Arg::with_name("duration")
                .short("d")
                .long("duration")
                .help("Run non-Insert workload for this many seconds (instead of a fixed key count). Default: 10 for Read workloads (RU, RR, etc.). Ignored for Insert (I).")
                .takes_value(true)
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
            return Err(format!(
                "Invalid ip-map entry `{}` (expected from=to)",
                entry
            ));
        }
        map.insert(from.to_string(), to.to_string());
    }
    Ok(map)
}
