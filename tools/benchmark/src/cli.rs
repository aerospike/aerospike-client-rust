use std::env;
use std::str::FromStr;
use std::convert::AsRef;

use clap::{App, Arg};

use workers::Workload;

const AFTER_HELP: &'static str = r###"

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

"###;

#[derive(Debug)]
pub struct Options {
    pub hosts: String,
    pub namespace: String,
    pub set: String,
    pub keys: i64,
    pub start_key: i64,
    pub concurrency: i64,
    pub workload: Workload,
}

pub fn parse_options() -> Options {
    let matches = build_cli().get_matches();
    Options {
        hosts: matches.value_of("hosts")
            .map(|s| s.to_owned())
            .or_else(|| env::var("AEROSPIKE_HOSTS").ok())
            .unwrap_or_else(|| String::from("127.0.0.1:3000")),
        namespace: matches.value_of("namespace").unwrap().to_owned(),
        set: matches.value_of("set").unwrap().to_owned(),
        keys: i64::from_str(matches.value_of("keys").unwrap()).unwrap(),
        start_key: i64::from_str(matches.value_of("startkey").unwrap()).unwrap(),
        concurrency: i64::from_str(matches.value_of("concurrency").unwrap()).unwrap(),
        workload: Workload::from_str(matches.value_of("workload").unwrap()).unwrap(),
    }
}

fn build_cli() -> App<'static, 'static> {
    App::new(crate_name!())
        .bin_name("benchmark")
        .version(crate_version!())
        .about(crate_description!())
        .arg(Arg::from_usage("-h, --hosts=HOSTS 'List of seed hosts (see below)'"))
        .arg(Arg::from_usage("-n, --namespace=NAMESPACE 'Aerospike namespace'")
            .default_value("test"))
        .arg(Arg::from_usage("-s, --set=SET 'Aerospike set name'").default_value("testset"))
        .arg(Arg::from_usage("-k, --keys=KEYS")
            .help("Set the number of keys the client is dealing with. If using an 'insert' \
                   workload (detailed below), the client will write this number of keys, \
                   starting from value = startkey. Otherwise, the client will read and update \
                   randomly across the values between startkey and startkey + num_keys. startkey \
                   can be set using '-S' or '--startkey'.")
            .validator(|val| validate::<i64>(val, "Must be number".into()))
            .default_value("10"))
        .arg(Arg::from_usage("-S, --startkey=STARTKEY")
            .help("Set the starting value of the working set of keys. If using an 'insert' \
                   workload, the start_value indicates the first value to write. Otherwise, the \
                   start_value indicates the smallest value in the working set of keys.")
            .validator(|val| validate::<i64>(val, "Must be number".into()))
            .default_value("0"))
        .arg(Arg::from_usage("-c, --concurrency=THREADS 'No. threads used to generate load'")
            .validator(|val| validate::<i64>(val, "Must be number".into()))
            .default_value("2"))
        .arg(Arg::from_usage("-w, --workload=WORKLOAD 'Workload definition (see below for \
                              details)'")
            .default_value("I"))
        .after_help(AFTER_HELP.trim())
}

fn validate<T: FromStr>(value: String, err: String) -> Result<(), String> {
    match T::from_str(value.as_ref()) {
        Ok(_) => Ok(()),
        Err(_) => Err(err.into()),
    }
}
