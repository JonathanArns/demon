use crate::storage::{counters::{self, CounterOp}, Query};
use anyhow::anyhow;

pub fn parse_counter_query(text: &str) -> anyhow::Result<Query<CounterOp>> {
    let (op, operands) = text.split_at(1);
    match op {
        "p" => {
            let (key, val) = operands.split_once(":").ok_or_else(|| anyhow!("only one operand"))?;
            let key = key.parse::<counters::Key>()?;
            let val = val.parse::<counters::Value>()?;
            Ok(Query{ops: vec![CounterOp::Add{key, val}]})
        },
        "m" => {
            let (key, val) = operands.split_once(":").ok_or_else(|| anyhow!("only one operand"))?;
            let key = key.parse::<counters::Key>()?;
            let val = val.parse::<counters::Value>()?;
            Ok(Query{ops: vec![CounterOp::Subtract{key, val}]})
        },
        "s" => {
            let (key, val) = operands.split_once(":").ok_or_else(|| anyhow!("only one operand"))?;
            let key = key.parse::<counters::Key>()?;
            let val = val.parse::<counters::Value>()?;
            Ok(Query{ops: vec![CounterOp::Set{key, val}]})
        },
        "r" => {
            let key = operands.parse::<counters::Key>()?;
            Ok(Query{ops: vec![CounterOp::Read{key}]})
        },
        _ => Err(anyhow!("bad query"))
    }
}
