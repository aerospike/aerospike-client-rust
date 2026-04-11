#!/bin/bash
set -e

echo "Waiting for Aerospike cluster to be ready..."
for i in $(seq 1 60); do
    if docker exec as-bridge-1 asinfo -v status 2>/dev/null | grep -q "ok"; then
        echo "as-bridge-1 is up (attempt $i)"
        break
    fi
    echo "  Waiting... (attempt $i/60)"
    sleep 2
done

echo "Waiting for all 4 nodes to form a cluster..."
for i in $(seq 1 60); do
    SIZE=$(docker exec as-bridge-1 asinfo -v "statistics" 2>/dev/null | tr ';' '\n' | grep "^cluster_size=" | cut -d= -f2 || echo "0")
    if [ "$SIZE" = "4" ]; then
        echo "Cluster is $SIZE nodes (attempt $i)"
        break
    fi
    echo "  Cluster size: $SIZE (attempt $i/60)"
    sleep 2
done

echo ""
echo "========================================="
echo "  Starting Rust client test"
echo "========================================="
echo ""

cargo test --no-default-features --features async,serialization,rt-tokio \
    --test lib -- test_bridge_replacement_loop --nocapture
