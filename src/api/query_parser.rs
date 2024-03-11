use crate::storage::{counters::{self, CounterOp}, Query};
use anyhow::anyhow;

pub fn parse_counter_query(text: &str) -> anyhow::Result<Query<CounterOp>> {
    let parts = text.split(";");
    let mut ops = vec![];
    for part in parts {
        ops.push(parse_counter_op(part)?);
    }
    Ok(Query{ops})
}

pub fn parse_counter_op(text: &str) -> anyhow::Result<CounterOp> {
    let (op, operands) = text.split_at(1);
    match op {
        "p" => {
            let (key, val) = operands.split_once(":").ok_or_else(|| anyhow!("only one operand"))?;
            let key = key.parse::<counters::Key>()?;
            let val = val.parse::<counters::Value>()?;
            Ok(CounterOp::Add{key, val})
        },
        "m" => {
            let (key, val) = operands.split_once(":").ok_or_else(|| anyhow!("only one operand"))?;
            let key = key.parse::<counters::Key>()?;
            let val = val.parse::<counters::Value>()?;
            Ok(CounterOp::Subtract{key, val})
        },
        "s" => {
            let (key, val) = operands.split_once(":").ok_or_else(|| anyhow!("only one operand"))?;
            let key = key.parse::<counters::Key>()?;
            let val = val.parse::<counters::Value>()?;
            Ok(CounterOp::Set{key, val})
        },
        "r" => {
            let key = operands.parse::<counters::Key>()?;
            Ok(CounterOp::Read{key})
        },
        _ => Err(anyhow!("bad query"))
    }
}
