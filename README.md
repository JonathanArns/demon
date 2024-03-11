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

### Things to work on
#### Missing functionality
- partitioning (local and cross-regional)
- a useful data and query model
- failure recovery (optional for this prototype)

#### Optimization
- more efficient (parallel) execution
