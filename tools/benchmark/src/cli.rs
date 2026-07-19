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

//! Command-line interface. Option set mirrors the Java client's
//! `benchmarks` application where applicable; options tied to the JVM
//! (virtual threads, Netty event loops, `-a` async toggle) have no
//! counterpart because this client is async-native — concurrency is
//! controlled with `--tasks` and `--cores`.

use std::collections::{HashMap, HashSet};
use std::env;
use std::path::PathBuf;
use std::str::FromStr;

use clap::Parser;

use aerospike::policy::Replica;
use aerospike::{CommitLevel, ReadModeAP, ReadModeSC};

use crate::{
    db_object_spec::{parse_object_spec_list, DBObjectSpec},
    stats::{LatencyMode, ReportStyle},
    workers::Workload,
};

const AFTER_HELP: &str = r"SETTING SEED HOSTS:

The list of seed hosts can be specified using -h/--hosts or by
setting the AEROSPIKE_HOSTS environment variable. The format is:

    <hostname_or_ip>[:<port>][,<hostname_or_ip>[:<port>][,...]]

If no port is specified, the default port is used.
IPv6 addresses must be enclosed in square brackets.

SELECTING WORKLOADS

The -w/--workload parameter selects the benchmark workload:

* I                          Insert: linear write of --keys records
* RU[,rd%[,rdAll%[,wrAll%]]] Read/Update mix (default read 100%)
* RR[,rd%[,rdAll%[,wrAll%]]] Read/Replace: like RU, writes use Replace
* RMU                        Read-Modify-Update (read all, write one bin)
* RMI                        Read-Modify-Increment (+1)
* RMD                        Read-Modify-Decrement (-1)
* TXN,r:<n>,w:<m>[,v:<var>]  Business transaction: n reads + m writes per
                             transaction; v randomizes counts (int or pct)
* TXN,t:<pattern>            Fixed op pattern, e.g. 'rrwub10':
                             r/R read one/all bins, u/U update one/all,
                             p/P replace one/all, i increment,
                             b<n> batch read of n keys, w/W = u/U
* passing -F/--key-file switches to a read-from-file workload

EXAMPLES

    benchmark -w I -k 1000000
    benchmark -w RU,50 -d 30 -g 10000 -l 7,1
    benchmark -w 'TXN,r:10,w:2,v:20%' --transactions 100000
    benchmark -w RU,80 --mrt-size 4 -d 30
";

#[derive(Parser, Debug)]
#[command(
    name = "benchmark",
    bin_name = "benchmark",
    version,
    about = "Benchmark suite for the Aerospike Rust client",
    after_help = AFTER_HELP,
    disable_help_flag = true
)]
#[allow(clippy::struct_excessive_bools)]
pub struct Cli {
    /// Print help (-h is taken by --hosts, matching the Java benchmark)
    #[arg(long, action = clap::ArgAction::Help)]
    pub help: Option<bool>,

    // ---------------- connection ----------------
    /// List of seed hosts (see below)
    #[arg(short = 'h', long)]
    pub hosts: Option<String>,

    /// User name for authentication (password via -P or AEROSPIKE_PASSWORD)
    #[arg(short = 'U', long)]
    pub user: Option<String>,

    /// Password for authentication
    #[arg(short = 'P', long)]
    pub password: Option<String>,

    /// Expected cluster name
    #[arg(long)]
    pub cluster_name: Option<String>,

    /// Number of synchronous connection pools per node
    #[arg(short = 'Y', long, default_value_t = 1)]
    pub conn_pools_per_node: u8,

    /// Minimum number of connections per node
    #[arg(long, default_value_t = 0)]
    pub min_conns_per_node: usize,

    /// Maximum number of connections per node
    #[arg(long)]
    pub max_conns_per_node: Option<usize>,

    /// Maximum errors per node per error-rate-window before backoff
    #[arg(long)]
    pub max_error_rate: Option<usize>,

    /// Number of cluster-tend iterations in the max-error-rate window
    #[arg(long)]
    pub error_rate_window: Option<usize>,

    /// Interval between cluster tends, in milliseconds (min 250)
    #[arg(long)]
    pub tend_interval: Option<u32>,

    /// Rack id of this benchmark instance (enables rack-aware reads with
    /// --replica prefer-rack)
    #[arg(long)]
    pub rack_id: Option<usize>,

    /// Use server "services-alternate" addresses
    #[arg(long)]
    pub use_services_alternate: bool,

    /// Map advertised IPs to reachable IPs (format: from=to[,from=to...])
    #[arg(long)]
    pub ip_map: Option<String>,

    // ---------------- workload ----------------
    /// Aerospike namespace
    #[arg(short = 'n', long, default_value = "test")]
    pub namespace: String,

    /// Aerospike set name
    #[arg(short = 's', long, default_value = "testset")]
    pub set: String,

    /// Workload definition (see below)
    #[arg(short = 'w', long, default_value = "I")]
    pub workload: String,

    /// Number of keys in the working set
    #[arg(short = 'k', long, default_value_t = 100_000)]
    pub keys: i64,

    /// First key of the working set
    #[arg(short = 'S', long, default_value_t = 0)]
    pub startkey: i64,

    /// Read keys from this file (one per line); switches the workload to
    /// read-from-file
    #[arg(short = 'F', long)]
    pub key_file: Option<PathBuf>,

    /// Type of the keys in --key-file: S (string) or I (integer)
    #[arg(short = 'K', long, default_value = "S", value_parser = ["S", "I"])]
    pub key_type: String,

    /// Number of bins per record
    #[arg(short = 'b', long, default_value_t = 1)]
    pub bins: usize,

    /// Prefix for bin names (bins are named <prefix>_1, <prefix>_2, ...)
    #[arg(short = 'p', long, default_value = "testBin")]
    pub bin_prefix: String,

    /// Comma-separated object specs: I | D | B:<size> | S:<size> | R:<bytes>:<randPct>
    #[arg(short = 'o', long, default_value = "I")]
    pub object_spec: String,

    /// Generate a fresh random value for every write (default: values are
    /// generated once at startup and reused, like the Java benchmark)
    #[arg(short = 'R', long)]
    pub random: bool,

    /// Record expiration in seconds: -1 never expire, 0 namespace default,
    /// > 0 expire after that many seconds
    #[arg(short = 'e', long, default_value_t = 0, allow_hyphen_values = true)]
    pub expiration: i64,

    /// Send the user key to the server on writes
    #[arg(long)]
    pub send_key: bool,

    /// Reset record TTL on reads when within this percentage of the most
    /// recent write TTL (1-100)
    #[arg(long, value_parser = clap::value_parser!(u8).range(1..=100))]
    pub read_touch_ttl_percent: Option<u8>,

    /// Restrict single-record operations to keys in these partition IDs
    /// (comma-separated)
    #[arg(long)]
    pub partition_ids: Option<String>,

    /// Round-robin batch reads over these namespaces (comma-separated)
    #[arg(long)]
    pub batch_namespaces: Option<String>,

    /// UDF package (module) name; when set together with --udf-function,
    /// RU/RR reads execute this UDF instead of a get
    #[arg(long)]
    pub udf_package: Option<String>,

    /// UDF function name
    #[arg(long)]
    pub udf_function: Option<String>,

    /// Comma-separated UDF argument values (strings)
    #[arg(long)]
    pub udf_values: Option<String>,

    /// Records per multi-record transaction; wraps operations of the I,
    /// RU and RR workloads in server transactions (commit/abort)
    #[arg(long)]
    pub mrt_size: Option<usize>,

    // ---------------- policies ----------------
    /// Replica policy for reads
    #[arg(short = 'r', long, default_value = "sequence",
          value_parser = ["master", "master-proles", "sequence", "prefer-rack", "random"])]
    pub replica: String,

    /// Read consistency for AP namespaces
    #[arg(long, default_value = "one", value_parser = ["one", "all"])]
    pub read_mode_ap: String,

    /// Read mode for SC (strong consistency) namespaces
    #[arg(long, default_value = "session",
          value_parser = ["session", "linearize", "allow-replica", "allow-unavailable"])]
    pub read_mode_sc: String,

    /// Write commit level
    #[arg(long, default_value = "all", value_parser = ["all", "master"])]
    pub commit_level: String,

    /// Set BOTH socket and total timeout (ms) for reads and writes
    #[arg(short = 'T', long)]
    pub timeout: Option<u32>,

    /// Socket timeout (ms) for reads and writes
    #[arg(long)]
    pub socket_timeout: Option<u32>,

    /// Socket timeout (ms) for reads only
    #[arg(long)]
    pub read_socket_timeout: Option<u32>,

    /// Socket timeout (ms) for writes only
    #[arg(long)]
    pub write_socket_timeout: Option<u32>,

    /// Total timeout (ms) for reads and writes
    #[arg(long)]
    pub total_timeout: Option<u32>,

    /// Total timeout (ms) for reads only
    #[arg(long)]
    pub read_total_timeout: Option<u32>,

    /// Total timeout (ms) for writes only
    #[arg(long)]
    pub write_total_timeout: Option<u32>,

    /// Timeout delay (ms): keep the socket alive briefly after a client
    /// timeout to allow it to recover
    #[arg(long)]
    pub timeout_delay: Option<u32>,

    /// Maximum number of retries
    #[arg(long)]
    pub max_retries: Option<usize>,

    /// Milliseconds to sleep between retries
    #[arg(long)]
    pub sleep_between_retries: Option<u32>,

    // ---------------- benchmark ----------------
    /// Number of concurrent tasks generating load
    #[arg(short = 't', long, default_value_t = default_tasks())]
    pub tasks: i64,

    /// Number of runtime worker threads (CPU cores)
    #[arg(short = 'c', long, default_value_t = default_cores())]
    pub cores: i64,

    /// Batch size for RU/RR workloads (1 = single-record operations)
    #[arg(short = 'B', long, default_value_t = 1)]
    pub batch_size: usize,

    /// Target aggregate throughput in transactions/second (0 = unlimited)
    #[arg(short = 'g', long, default_value_t = 0)]
    pub throughput: u64,

    /// Stop after this many transactions in total (0 = unlimited)
    #[arg(long, default_value_t = 0)]
    pub transactions: u64,

    /// Run non-Insert workloads for this many seconds (default 10 when
    /// neither --duration nor --transactions is given)
    #[arg(short = 'd', long)]
    pub duration: Option<u64>,

    /// Latency histogram: 'ycsb[,<warmup ops>]' or '[alt,]<columns>,<shift>[,us|ms]'
    /// (e.g. '7,1' or 'alt,7,1,us'); default is a fixed 6-bucket histogram
    #[arg(short = 'l', long)]
    pub latency: Option<String>,

    /// Report reads of missing records separately (nf=) instead of
    /// counting them as successes
    #[arg(short = 'N', long)]
    pub report_not_found: bool,

    /// Output format: pretty (sectioned) or asbench (C benchmark one-line style)
    #[arg(long, default_value = "pretty", value_parser = ["pretty", "asbench"])]
    pub report_style: String,

    /// Debug mode: verbose logging
    #[arg(short = 'D', long)]
    pub debug: bool,
}

fn default_cores() -> i64 {
    std::thread::available_parallelism()
        .map(|n| n.get() as i64)
        .unwrap_or(1)
}

fn default_tasks() -> i64 {
    default_cores() * 2
}

/// Fully parsed and validated benchmark configuration.
#[derive(Debug)]
pub struct Options {
    pub hosts: String,
    pub user: Option<String>,
    pub password: Option<String>,
    pub cluster_name: Option<String>,
    pub namespace: String,
    pub set: String,
    pub keys: i64,
    pub start_key: i64,
    pub tasks: i64,
    pub cores: i64,
    pub workload: Workload,
    pub conn_pools_per_node: u8,
    pub min_conns_per_node: usize,
    pub max_conns_per_node: Option<usize>,
    pub max_error_rate: Option<usize>,
    pub error_rate_window: Option<usize>,
    pub tend_interval: Option<u32>,
    pub rack_id: Option<usize>,
    pub use_services_alternate: bool,
    pub ip_map: Option<HashMap<String, String>>,
    pub bins: usize,
    pub bin_name_base: String,
    pub object_specs: Vec<DBObjectSpec>,
    pub random_values: bool,
    pub expiration: i64,
    pub send_key: bool,
    pub read_touch_ttl_percent: Option<u8>,
    pub partition_ids: Option<HashSet<u16>>,
    pub batch_namespaces: Vec<String>,
    pub udf: Option<(String, String, Vec<String>)>,
    pub mrt_size: Option<usize>,
    pub key_file: Option<PathBuf>,
    pub string_keys: bool,
    pub replica: Replica,
    pub read_mode_ap: ReadModeAP,
    pub read_mode_sc: ReadModeSC,
    pub commit_level: CommitLevel,
    pub timeout: Option<u32>,
    pub socket_timeout: Option<u32>,
    pub read_socket_timeout: Option<u32>,
    pub write_socket_timeout: Option<u32>,
    pub total_timeout: Option<u32>,
    pub read_total_timeout: Option<u32>,
    pub write_total_timeout: Option<u32>,
    pub timeout_delay: Option<u32>,
    pub max_retries: Option<usize>,
    pub sleep_between_retries: Option<u32>,
    pub batch_size: usize,
    pub throughput: u64,
    pub transactions: u64,
    pub duration_secs: Option<u64>,
    pub latency_mode: LatencyMode,
    pub report_not_found: bool,
    pub report_style: ReportStyle,
    pub debug: bool,
}

pub fn parse_options() -> Result<Options, String> {
    from_cli(Cli::parse())
}

#[allow(clippy::too_many_lines)]
fn from_cli(cli: Cli) -> Result<Options, String> {
    let mut workload = Workload::from_str(&cli.workload)?;
    if cli.key_file.is_some() {
        workload = Workload::ReadFromFile;
    }

    let ip_map = cli
        .ip_map
        .or_else(|| env::var("AEROSPIKE_IP_MAP").ok())
        .as_deref()
        .map(parse_ip_map)
        .transpose()?;

    let latency_mode = cli
        .latency
        .as_deref()
        .map(LatencyMode::from_str)
        .transpose()?
        .unwrap_or(LatencyMode::Default);

    let udf = match (&cli.udf_package, &cli.udf_function) {
        (Some(pkg), Some(f)) => Some((
            pkg.clone(),
            f.clone(),
            cli.udf_values
                .as_deref()
                .map(|s| s.split(',').map(|v| v.trim().to_owned()).collect())
                .unwrap_or_default(),
        )),
        (None, None) => None,
        _ => {
            return Err(
                "--udf-package and --udf-function must be specified together".to_string(),
            )
        }
    };

    let partition_ids = cli
        .partition_ids
        .as_deref()
        .map(parse_partition_ids)
        .transpose()?;

    let batch_namespaces: Vec<String> = cli
        .batch_namespaces
        .as_deref()
        .map(|s| {
            s.split(',')
                .map(str::trim)
                .filter(|n| !n.is_empty())
                .map(ToOwned::to_owned)
                .collect()
        })
        .unwrap_or_default();

    let duration_secs = parse_duration_secs(cli.duration, cli.transactions, workload_kind(&workload))?;

    let options = Options {
        hosts: cli
            .hosts
            .or_else(|| env::var("AEROSPIKE_HOSTS").ok())
            .unwrap_or_else(|| String::from("127.0.0.1:3000")),
        user: cli.user,
        password: cli
            .password
            .or_else(|| env::var("AEROSPIKE_PASSWORD").ok()),
        cluster_name: cli.cluster_name,
        namespace: cli.namespace,
        set: cli.set,
        keys: cli.keys,
        start_key: cli.startkey,
        tasks: cli.tasks,
        cores: cli.cores,
        workload,
        conn_pools_per_node: cli.conn_pools_per_node,
        min_conns_per_node: cli.min_conns_per_node,
        max_conns_per_node: cli.max_conns_per_node,
        max_error_rate: cli.max_error_rate,
        error_rate_window: cli.error_rate_window,
        tend_interval: cli.tend_interval,
        rack_id: cli.rack_id,
        use_services_alternate: cli.use_services_alternate,
        ip_map,
        bins: cli.bins,
        bin_name_base: cli.bin_prefix,
        object_specs: parse_object_spec_list(&cli.object_spec)?,
        random_values: cli.random,
        expiration: cli.expiration,
        send_key: cli.send_key,
        read_touch_ttl_percent: cli.read_touch_ttl_percent,
        partition_ids,
        batch_namespaces,
        udf,
        mrt_size: cli.mrt_size,
        key_file: cli.key_file,
        string_keys: cli.key_type == "S",
        replica: match cli.replica.as_str() {
            "master" => Replica::Master,
            "master-proles" => Replica::MasterProles,
            "prefer-rack" => Replica::PreferRack,
            "random" => Replica::Random,
            _ => Replica::Sequence,
        },
        read_mode_ap: match cli.read_mode_ap.as_str() {
            "all" => ReadModeAP::All,
            _ => ReadModeAP::One,
        },
        read_mode_sc: match cli.read_mode_sc.as_str() {
            "linearize" => ReadModeSC::Linearize,
            "allow-replica" => ReadModeSC::AllowReplica,
            "allow-unavailable" => ReadModeSC::AllowUnavailable,
            _ => ReadModeSC::Session,
        },
        commit_level: match cli.commit_level.as_str() {
            "master" => CommitLevel::CommitMaster,
            _ => CommitLevel::CommitAll,
        },
        timeout: cli.timeout,
        socket_timeout: cli.socket_timeout,
        read_socket_timeout: cli.read_socket_timeout,
        write_socket_timeout: cli.write_socket_timeout,
        total_timeout: cli.total_timeout,
        read_total_timeout: cli.read_total_timeout,
        write_total_timeout: cli.write_total_timeout,
        timeout_delay: cli.timeout_delay,
        max_retries: cli.max_retries,
        sleep_between_retries: cli.sleep_between_retries,
        batch_size: cli.batch_size,
        throughput: cli.throughput,
        transactions: cli.transactions,
        duration_secs,
        latency_mode,
        report_not_found: cli.report_not_found,
        report_style: match cli.report_style.as_str() {
            "asbench" => ReportStyle::Asbench,
            _ => ReportStyle::Pretty,
        },
        debug: cli.debug,
    };

    custom_validations(&options)?;
    Ok(options)
}

enum WorkloadKind {
    Insert,
    Other,
}

fn workload_kind(w: &Workload) -> WorkloadKind {
    if *w == Workload::Initialize {
        WorkloadKind::Insert
    } else {
        WorkloadKind::Other
    }
}

fn parse_duration_secs(
    duration: Option<u64>,
    transactions: u64,
    kind: WorkloadKind,
) -> Result<Option<u64>, String> {
    Ok(match kind {
        WorkloadKind::Insert => {
            if duration.is_some() {
                return Err(
                    "duration (-d/--duration) is not allowed for Initialize (I) workload"
                        .to_string(),
                );
            }
            None
        }
        WorkloadKind::Other => {
            // A transaction limit replaces the default duration; an explicit
            // duration still applies as an additional stop condition.
            if duration.is_none() && transactions > 0 {
                None
            } else {
                duration.or(Some(10))
            }
        }
    })
}

// put all custom validation here
fn custom_validations(opts: &Options) -> Result<(), String> {
    let batches_allowed = matches!(
        opts.workload,
        Workload::ReadUpdate { .. } | Workload::ReadReplace { .. }
    );
    if !batches_allowed && opts.batch_size > 1 {
        return Err(
            "batch size (-B/--batch-size) is only applicable for RU/RR workloads".to_string(),
        );
    }
    if opts.duration_secs.is_some_and(|secs| secs == 0) {
        return Err("Duration must be greater than 0".to_string());
    }
    if let Some(mrt) = opts.mrt_size {
        if mrt == 0 {
            return Err("--mrt-size must be greater than 0".to_string());
        }
        let mrt_allowed = matches!(
            opts.workload,
            Workload::Initialize | Workload::ReadUpdate { .. } | Workload::ReadReplace { .. }
        );
        if !mrt_allowed {
            return Err("--mrt-size is only supported for the I, RU and RR workloads".to_string());
        }
        if opts.batch_size > 1 {
            return Err("--mrt-size cannot be combined with --batch-size".to_string());
        }
        if opts.workload == Workload::Initialize && opts.keys % (mrt as i64) != 0 {
            return Err("--keys must be a multiple of --mrt-size for the Insert workload".to_string());
        }
    }
    if opts.udf.is_some() && opts.batch_size > 1 {
        return Err("UDF reads cannot be combined with --batch-size".to_string());
    }
    if opts.workload == Workload::ReadFromFile && opts.key_file.is_none() {
        return Err("read-from-file workload requires -F/--key-file".to_string());
    }
    Ok(())
}

fn parse_partition_ids(spec: &str) -> Result<HashSet<u16>, String> {
    let mut ids = HashSet::new();
    for part in spec.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let id: u16 = part
            .parse()
            .map_err(|e| format!("Invalid partition id `{part}`: {e}"))?;
        if id >= 4096 {
            return Err(format!("Partition id {id} out of range (0-4095)"));
        }
        ids.insert(id);
    }
    if ids.is_empty() {
        return Err("No partition ids given".to_string());
    }
    Ok(ids)
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
            .ok_or_else(|| format!("Invalid ip-map entry `{entry}` (expected from=to)"))?;
        let from = from.trim();
        let to = to.trim();
        if from.is_empty() || to.is_empty() {
            return Err(format!("Invalid ip-map entry `{entry}` (expected from=to)"));
        }
        map.insert(from.to_string(), to.to_string());
    }
    Ok(map)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opts(args: &[&str]) -> Result<Options, String> {
        let mut argv = vec!["benchmark"];
        argv.extend_from_slice(args);
        from_cli(Cli::try_parse_from(argv).map_err(|e| e.to_string())?)
    }

    #[test]
    fn defaults_parse() {
        let o = opts(&[]).unwrap();
        assert_eq!(o.workload, Workload::Initialize);
        assert_eq!(o.batch_size, 1);
        assert_eq!(o.throughput, 0);
        assert!(!o.random_values);
    }

    #[test]
    fn txn_limit_disables_default_duration() {
        let o = opts(&["-w", "RU", "--transactions", "5000"]).unwrap();
        assert_eq!(o.duration_secs, None);
        assert_eq!(o.transactions, 5000);
        let o = opts(&["-w", "RU"]).unwrap();
        assert_eq!(o.duration_secs, Some(10));
    }

    #[test]
    fn mrt_validations() {
        assert!(opts(&["-w", "RMU", "--mrt-size", "4"]).is_err());
        assert!(opts(&["-w", "RU", "--mrt-size", "4", "-B", "10"]).is_err());
        assert!(opts(&["-w", "I", "--mrt-size", "3", "-k", "100"]).is_err()); // 100 % 3 != 0
        assert!(opts(&["-w", "I", "--mrt-size", "4", "-k", "100"]).is_ok());
    }

    #[test]
    fn udf_requires_both_parts() {
        assert!(opts(&["--udf-package", "pkg"]).is_err());
        let o = opts(&[
            "-w",
            "RU",
            "--udf-package",
            "pkg",
            "--udf-function",
            "f",
            "--udf-values",
            "a, b",
        ])
        .unwrap();
        let (pkg, f, vals) = o.udf.unwrap();
        assert_eq!((pkg.as_str(), f.as_str()), ("pkg", "f"));
        assert_eq!(vals, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn partition_ids_parse() {
        let o = opts(&["-w", "RU", "--partition-ids", "1, 2,4095"]).unwrap();
        let ids = o.partition_ids.unwrap();
        assert_eq!(ids.len(), 3);
        assert!(opts(&["--partition-ids", "4096"]).is_err());
    }

    #[test]
    fn policy_enums_parse() {
        let o = opts(&[
            "-w",
            "RU",
            "-r",
            "prefer-rack",
            "--read-mode-sc",
            "linearize",
            "--commit-level",
            "master",
        ])
        .unwrap();
        assert_eq!(o.replica, Replica::PreferRack);
        assert_eq!(o.read_mode_sc, ReadModeSC::Linearize);
        assert_eq!(o.commit_level, CommitLevel::CommitMaster);
    }
}
