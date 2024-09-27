use std::time::{SystemTime, UNIX_EPOCH};

use lazy_static::lazy_static;
use serde::Serialize;
use tokio::sync::{mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender}, Mutex};


lazy_static!{
    static ref TX_RX: (UnboundedSender<TimedInstrumentationEvent>, Mutex<UnboundedReceiver<TimedInstrumentationEvent>>) = {
        let (tx, rx) = unbounded_channel();
        (tx, Mutex::new(rx))
    };
}

#[derive(Debug)]
pub struct InstrumentationEvent {
    pub kind: String,
    pub meta: Option<String>,
    pub val: Option<u64>,
}

#[derive(Clone, Debug, Serialize)]
pub struct TimedInstrumentationEvent {
    pub unix_micros: u64,
    pub kind: String,
    pub meta: Option<String>,
    pub val: Option<u64>,
}

/// Logs an InstrumentationEvent.
pub fn log_instrumentation(event: InstrumentationEvent) {
    let tx = &TX_RX.0;
    tx.send(TimedInstrumentationEvent {
        unix_micros: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as u64,
        kind: event.kind,
        meta: event.meta,
        val: event.val,
    }).expect("instrumentation failed");
}

/// Reads all InstrumentationEvents that have been logged since the previous invocation of this function.
pub async fn read_instrumentation_events() -> Vec<TimedInstrumentationEvent> {
    let rx = &mut *TX_RX.1.lock().await;
    let mut buffer = Vec::with_capacity(1024);
    while !rx.is_empty() {
        let num = rx.recv_many(&mut buffer, 1024).await;
        if num < 1024 {
            break
        }
    }
    buffer
}
