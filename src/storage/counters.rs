use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::Operation;

/// Prelim def of a key
pub type Key = u64;
/// Prelim def of a stored Value
pub type Value = i64;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CounterOp {
    /// weak
    Read{key: Key},
    /// weak
    Add{key: Key, val: Value},
    /// weak
    Subtract{key: Key, val: Value},
    /// strong
    Set{key: Key, val: Value},
}

impl Operation for CounterOp {
    type State = HashMap<Key, Value>;
    type ReadVal = Option<Value>;

    fn is_weak(&self) -> bool {
        match *self {
            Self::Read{..} => true,
            Self::Add{..} => true,
            Self::Subtract{..} => true,
            Self::Set{..} => false,
        }
    }

    fn apply(&self, state: &mut Self::State) -> Option<Self::ReadVal> {
        match *self {
            Self::Read { key } => {
                Some(state.get(&key).map(|v| v.to_owned()))
            },
            Self::Add { key, val } => {
                if let Some(v) = state.get_mut(&key) {
                    *v += val;
                } else {
                    state.insert(key, val);
                }
                None
            },
            Self::Subtract { key, val } => {
                if let Some(v) = state.get_mut(&key) {
                    *v -= val;
                } else {
                    state.insert(key, -val);
                }
                None
            },
            Self::Set { key, val } => {
                state.insert(key, val);
                None
            },
        }
    }
}
