use std::collections::HashMap;

use anyhow::anyhow;
use rand::{Rng, thread_rng};
use serde::{Deserialize, Serialize};

use super::Operation;

pub type Key = u64;
pub type Value = i64;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CounterOp {
    Read{key: Key},
    Add{key: Key, val: Value},
    Subtract{key: Key, val: Value},
    Set{key: Key, val: Value},
}

impl CounterOp {
    fn key(&self) -> Key {
        match *self {
            Self::Read{key} => key,
            Self::Add{key, ..} => key,
            Self::Subtract{key, ..} => key,
            Self::Set{key, ..} => key,
        }
    }
}

impl Operation for CounterOp {
    type State = HashMap<Key, Value>;
    type ReadVal = Option<Value>;
    type QueryState = ();

    fn name(&self) -> String {
        match *self {
            Self::Read {..} => "Read",
            Self::Add {..} => "Add",
            Self::Subtract {..} => "Subtract",
            Self::Set {..} => "Set",
        }.to_string()
    }

    fn is_red(&self) -> bool {
        match *self {
            Self::Read{..} => false,
            Self::Add{..} => true,
            Self::Subtract{..} => true,
            Self::Set{..} => true,
        }
    }

    fn is_semiserializable_strong(&self) -> bool {
        match *self {
            Self::Read{..} => false,
            Self::Add{..} => false,
            Self::Subtract{..} => false,
            Self::Set{..} => true,
        }
    }

    fn is_conflicting(&self, other: &Self) -> bool {
        self.key() == other.key()
    }

    fn rollback_conflicting_state(&self, source: &Self::State, target: &mut Self::State) {
        let key = self.key();
        if let Some(val) = source.get(&key) {
            target.insert(key, *val);
        } else {
            target.remove(&key);
        }
    }

    fn is_writing(&self) -> bool {
        match *self {
            Self::Read{..} => false,
            Self::Add{..} => true,
            Self::Subtract{..} => true,
            Self::Set{..} => true,
        }
    }

    fn is_por_conflicting(&self, other: &Self) -> bool {
        match *self {
            Self::Read{..} => false,
            Self::Add{key: k1, ..} | Self::Subtract{key: k1, ..} => match *other {
                Self::Set{key: k2, ..} => k1 == k2,
                _ => false,
            },
            Self::Set{key: k1, ..} => match *other {
                Self::Add{key: k2, ..} | Self::Subtract{key: k2, ..} | Self::Set{key: k2, ..} => k1 == k2,
                _ => false,
            },
        }
    }

    fn parse(text: &str) -> anyhow::Result<Self> {
        if let Some((key, val)) = text.split_once("+") {
            let key = key.parse::<Key>()?;
            let val = val.parse::<Value>()?;
            Ok(CounterOp::Add{key, val})
        } else if let Some((key, val)) = text.split_once("-") {
            let key = key.parse::<Key>()?;
            let val = val.parse::<Value>()?;
            Ok(CounterOp::Subtract{key, val})
        } else if let Some((key, val)) = text.split_once("=") {
            let key = key.parse::<Key>()?;
            let val = val.parse::<Value>()?;
            Ok(CounterOp::Set{key, val})
        } else {
            let (op, operands) = text.split_at(1);
            match op {
                "r" => {
                    let key = operands.parse::<Key>()?;
                    Ok(CounterOp::Read{key})
                },
                _ => Err(anyhow!("bad query")),
            }
        }
    }

    /// All of the counter operations are their own shadow operations.
    fn generate_shadow(&self, state: &mut Self::State) -> Option<Self> {
        Some(self.clone())
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

    fn gen_query(settings: &crate::api::http::BenchSettings, _state: &mut Self::QueryState) -> Self {
        let mut rng = thread_rng();
        let key = rng.gen_range(0..settings.key_range) as Key;
        let read_ops = [Self::Read{key}];
        let weak_ops = [Self::Add{key, val: 1}, Self::Subtract{key, val: 1}];
        let strong_ops = [Self::Set{key, val: 1}];
        let strong = rng.gen_bool(settings.strong_ratio);
        let read = rng.gen_bool(settings.read_ratio);
        let query = if read {
            let i = rng.gen_range(0..read_ops.len());
            read_ops[i].to_owned()
        } else if strong {
            let i = rng.gen_range(0..strong_ops.len());
            strong_ops[i].to_owned()
        } else {
            let i = rng.gen_range(0..weak_ops.len());
            weak_ops[i].to_owned()
        };
        query
    }
}
