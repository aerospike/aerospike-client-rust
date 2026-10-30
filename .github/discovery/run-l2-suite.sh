#!/usr/bin/env bash
# Run inside the L2 client container, on the client network. The discovery
# workflow (or tests/discovery/run_l2.sh) injects the DISCOVERY_* and
# TOXIPROXY_* variables through the topology's env file. L2_PROFILE picks the
# rows: every row outside its profile skips itself, so the default profile
# runs the whole binary and the others run one module.
#
# L2_TEST_BINARY names a prebuilt `discovery_l2` test binary (the workflow
# builds it on the runner, which shares the container's glibc). Without it
# the script runs `cargo test`, for a client image that carries a toolchain.
# L2_FILTER, space-separated test name substrings, replaces the profile's
# selection, to rerun single rows.
set -euo pipefail

host="${DISCOVERY_ENDPOINT_HOSTNAME:?DISCOVERY_ENDPOINT_HOSTNAME is required}"
ports="${DISCOVERY_ENDPOINT_PORTS:?DISCOVERY_ENDPOINT_PORTS is required}"
subset="${DISCOVERY_SUBSET:?DISCOVERY_SUBSET is required}"
profile="${L2_PROFILE:-default}"

case "$subset" in
  ce|ee) ;;
  *)
    echo "DISCOVERY_SUBSET must be ce or ee (got: ${subset})" >&2
    exit 1
    ;;
esac

[[ "$host" =~ ^[A-Za-z0-9.-]+$ ]] || {
  echo "Refusing unexpected endpoint hostname: ${host}" >&2
  exit 1
}

case "$profile" in
  default) filter="" ;;
  reachable) filter="l2::reachable::" ;;
  pass-through-names|pass-through-ports) filter="l2::pass_through::" ;;
  port-map-range) filter="l2::port_map_range::" ;;
  cn-only) filter="l2::cn_only::" ;;
  low-mtu) filter="l2::low_mtu::" ;;
  port-range) filter="l2::source_port_range::" ;;
  idle-reset) filter="l2::idle_reset::" ;;
  *)
    echo "Unknown profile ${profile}" >&2
    exit 1
    ;;
esac

if [[ "$subset" == "ee" ]]; then
  check_ports="${DISCOVERY_TLS_ENDPOINT_PORTS:?DISCOVERY_TLS_ENDPOINT_PORTS is required}"
else
  check_ports="$ports"
fi

# A cluster that never came up must fail here, not look like a skipped suite.
IFS=',' read -r -a port_list <<< "$check_ports"
if ((${#port_list[@]} == 0)); then
  echo "No proxy ports to check" >&2
  exit 1
fi
for port in "${port_list[@]}"; do
  [[ "$port" =~ ^[0-9]+$ ]] || {
    echo "Refusing unexpected endpoint port: ${port}" >&2
    exit 1
  }
  echo "Checking TCP ${host}:${port}"
  timeout 10 bash -c "echo >/dev/tcp/${host}/${port}"
done

if [[ -n "${AEROSPIKE_CA_CERT:-}" && ! -r "${AEROSPIKE_CA_CERT}" ]]; then
  echo "AEROSPIKE_CA_CERT ${AEROSPIKE_CA_CERT} is not readable" >&2
  exit 1
fi

args=(--test-threads=1 --nocapture)
if [[ -n "${L2_FILTER:-}" ]]; then
  # shellcheck disable=SC2206 -- the filter is a list of test names
  args+=(${L2_FILTER})
elif [[ -n "$filter" ]]; then
  args+=("$filter")
fi

if [[ -n "${L2_TEST_BINARY:-}" ]]; then
  [[ -x "${L2_TEST_BINARY}" ]] || {
    echo "L2_TEST_BINARY ${L2_TEST_BINARY} is not executable" >&2
    exit 1
  }
  echo "Running ${L2_TEST_BINARY} ${args[*]}"
  exec "${L2_TEST_BINARY}" "${args[@]}"
fi

command -v cargo >/dev/null 2>&1 || {
  echo "Neither L2_TEST_BINARY nor a cargo toolchain is available" >&2
  exit 1
}
echo "Running cargo test --test discovery_l2 -- ${args[*]}"
exec cargo test --test discovery_l2 --no-default-features \
  --features async,serialization,rt-tokio,tls -- "${args[@]}"
