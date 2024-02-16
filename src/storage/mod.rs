/// Prelim def of a key
pub type Key = u64;
/// Prelim def of a stored Value
pub type Value = u64;

/// A snapshot identifier that enables processes to construct the snapshot
pub struct Snapshot {
    vector_clock: Vec<u64>,
}

#[derive(Debug)]
pub enum DemonOp {
    /// weak
    Add{key: Key, val: Value},
    /// strong
    Set{key: Key, val: Value},
}

impl DemonOp {
    fn is_weak(&self) -> bool {
        match *self {
            Self::Add{..} => true,
            Self::Set{..} => false,
        }
    }
}

pub struct Transaction {
    ops: Vec<DemonOp>,
}

pub trait DemonStorage {
    fn weak_op(&mut self, op: DemonOp) -> Option<Value>;

    fn transaction(&mut self, t: Transaction, snapshot: Snapshot) -> Option<Value>;
}
