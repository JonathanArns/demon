# Demon
Deterministic transactions over Monotonic state.

A prototype implementation of a mixed-consistency distributed transaction processing protocol.

### How to run

Start a small cluster with:
```bash
docker compose up --build
```

Interactively query from the terminal with:
```bash
./repl.py
```

Run a microbenchmark of random queries:
```bash
cargo run --bin microbench -- http://localhost:8080 http://localhost:8081 http://localhost:8082
```
