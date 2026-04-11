# CLIENT-4405: Stale Partition Map Reproduction (Bridge Network)

This directory contains a self-contained Docker Compose setup that reproduces
the stale partition map bug on a real Docker bridge network -- no ASX framework,
no Python wrapper, pure Rust client connecting to Aerospike nodes via bridge IPs.

## Quick Start

```bash
cd tools/docker-bridge-test
docker compose up --build
# Watch the rust-test container output for results
docker compose logs -f rust-test
# Cleanup
docker compose down -v
```

## What Happens

1. 4 Aerospike nodes start on a bridge network (IPs like 172.x.x.x)
2. The Rust test binary connects via bridge IP, loads 10000 records
3. Kills nodes 2, 3, 4 one at a time (restarting 2 and 3 in between)
4. After each kill: waits for tend eviction + migrations, probes get/put/remove
5. Node 4 is left permanently dead (simulating real node loss)
6. Final cleanup probe: reports failure counts

## Notes

- The Rust test container needs Docker socket access (`/var/run/docker.sock`)
  to run `docker kill` / `docker start` / `docker exec` on Aerospike containers.
- Uses the community edition (`aerospike/aerospike-server:8.0.0.4`) to avoid
  enterprise license cluster-size limits. The bug is client-side, not server-edition-dependent.
- First run takes ~5 minutes for Rust compilation. Subsequent runs use cache.
