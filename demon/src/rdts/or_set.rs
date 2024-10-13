use std::collections::{HashMap, HashSet};

use anyhow::anyhow;
use rand::{random, thread_rng, Rng};
use serde::{Deserialize, Serialize};

use super::Operation;

pub type Item = u64;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ORSetOp {
    Insert(Item),
    Remove(Item),
    InsertShadow(Item, UniqueId),
    RemoveShadow(Item, UniqueId),
    Compact,
}

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Ord, Eq, Serialize, Deserialize, Hash)]
pub struct UniqueId(u64);

impl UniqueId {
    pub fn gen() -> Self {
        UniqueId(random())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct ORSet {
    pub items: HashMap<Item, (HashSet<UniqueId>, HashSet<UniqueId>)>
}

impl Operation for ORSetOp {
    type State = ORSet;
    /// The set's logical and physical size.
    type ReadVal = (u64, u64);
    type QueryState = ();

    fn is_red(&self) -> bool {
        false
    }

    fn is_strong(&self) -> bool {
        match self {
            Self::Compact => true,
            _ => false,
        }
    }

    fn is_conflicting(&self, _other: &Self) -> bool {
        match self {
            Self::Compact => true,
            _ => false,
        }
    }

    fn rollback_conflicting_state(&self, source: &Self::State, target: &mut Self::State) {
        *target = source.clone()
    }

    fn is_writing(&self) -> bool {
        true
    }

    fn is_por_conflicting(&self, _other: &Self) -> bool {
        false
    }

    fn parse(text: &str) -> anyhow::Result<Self> {
        if let Some((op, item)) = text.split_once(" ") {
            let item = item.parse::<Item>()?;
            match op {
                "i" => Ok(Self::Insert(item)),
                "r" => Ok(Self::Remove(item)),
                _ => Err(anyhow!("bad query")),
            }
        } else {
            Err(anyhow!("bad query"))
        }
    }

    fn apply(&self, state: &mut Self::State) -> Option<Self::ReadVal> {
        match *self {
            Self::Insert(_item) => panic!("can't execute directly, must generate shadow"),
            Self::Remove(_item) => panic!("can't execute directly, must generate shadow"),
            Self::InsertShadow(item, id) => {
                if !state.items.contains_key(&item) {
                    state.items.insert(item, Default::default());
                }
                if let Some((add, _remove)) = state.items.get_mut(&item) {
                    add.insert(id);
                }
            },
            Self::RemoveShadow(item, id) => {
                if !state.items.contains_key(&item) {
                    state.items.insert(item, Default::default());
                }
                if let Some((_add, remove)) = state.items.get_mut(&item) {
                    remove.insert(id);
                }
            },
            Self::Compact => {
                
            }
        }
        todo!("return logical and physical size")
    }

    fn generate_shadow(&self, _state: &Self::State) -> Option<Self> {
        match *self {
            Self::Insert(item) => Some(Self::InsertShadow(item, UniqueId::gen())),
            Self::Remove(item) => Some(Self::RemoveShadow(item, UniqueId::gen())),
            Self::InsertShadow(_item, _id) => Some(self.clone()),
            Self::RemoveShadow(_item, _id) => Some(self.clone()),
            Self::Compact => Some(self.clone()),
        }
    }

    fn gen_periodic_strong_op() -> Option<Self> {
        Some(Self::Compact)
    }
    
    fn name(&self) -> String {
        match *self {
            Self::Compact {..} => "Compact",
            Self::Insert {..} => "Insert",
            Self::Remove {..} => "Remove",
            _ => "",
        }.to_string()
    }

    fn gen_query(settings: &crate::api::http::BenchSettings, _state: &mut Self::QueryState) -> Self {
        let mut rng = thread_rng();
        let item = rng.gen_range(0..(settings.key_range as u64));
        if rng.gen_bool(settings.strong_ratio) {
            Self::Insert(item)
        } else {
            Self::Remove(item)
        }
    }
}
