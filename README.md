# Demon
Deterministic transactions over Monotonic state.

A prototype implementation of the semi-serializability mixed-consistency replication strategy.
The repository also includes implementations of various other replication strategies for comparison.

### How to run

Start a small cluster with:
```bash
git submodule update --init # first, initialize the git submodule

docker compose up --build
```
The default docker image runs a wrapper around the database server, which listens to control commands
and allows running different DB cluster configurations and protocols from a single docker container.

### Benchmarking
This repository includes basic benchmarking infrastructure.
You can define an experiment suite as a json file, for an example, look at [benchmarks.json](benchmarks.json),
which works for the cluster defined by [compose.yml](compose.yml).

The benchmark infrastructure supports both micro benchmarks based on small RDTs, like replicated non-negative counters,
as well as a full TPCC benchmark.
All benchmarks are executed with clients concurrently executing queries at every replica.

To execute a set of experiments, use the `bench_harness` script.
This script snapshots intermediate results on disk, so in case of a broken internet connection, or another error,
re-executing the command will pick up from where it failed.
```bash
python bench_harness.py benchmarks.json --output-dir ./bench_data
```
You can plot the results with:
```bash
python plot.py ./bench_data
```
