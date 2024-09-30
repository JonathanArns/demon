use std::{fmt::Debug, sync::Arc};
use tokio::sync::Mutex;
use super::QueryResult;
use crate::rdts::Operation;

/// A naive storage implementation meant for non-hybrid consistency models.
#[derive(Debug)]
pub struct Storage<O: Operation> {
    state: Arc<Mutex<O::State>>,
}

impl<O: Operation> Storage<O> {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(O::State::default())),
        }
    }

    /// Executes a blue operation.
    pub async fn exec(&self, op: O) -> QueryResult<O> {
        let output = op.apply(&mut *self.state.lock().await);
        QueryResult{ value: output }
    }

    /// Generates the shadow op for `op` on the current state.
    pub async fn generate_shadow(&self, op: O) -> Option<O> {
        op.generate_shadow(&mut *self.state.lock().await)
    }
}
