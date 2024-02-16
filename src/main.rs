/// client API, calls sequencer or executor
mod api;
/// The transaction sequencing layer (replicated Log)
mod sequencer;
/// The gossip replication layer for weak operations
mod gossip;

// local only components

/// Mixed consistency local query executor (multi-threaded, with concurrency control)
mod executor;
/// The storage layer (single-threaded, versioned CRDTs)
mod storage;


/// Demon is fully event-driven. All actions are triggered here.
mod core;

/// networking, cluster membership?
mod network;

fn main() {
    println!("Hello, world!");
}
