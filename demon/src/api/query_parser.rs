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
    if let Some((key, val)) = text.split_once("+") {
        let key = key.parse::<counters::Key>()?;
        let val = val.parse::<counters::Value>()?;
        Ok(CounterOp::Add{key, val})
    } else if let Some((key, val)) = text.split_once("-") {
        let key = key.parse::<counters::Key>()?;
        let val = val.parse::<counters::Value>()?;
        Ok(CounterOp::Subtract{key, val})
    } else if let Some((key, val)) = text.split_once("=") {
        let key = key.parse::<counters::Key>()?;
        let val = val.parse::<counters::Value>()?;
        Ok(CounterOp::Set{key, val})
    } else {
        let (op, operands) = text.split_at(1);
        match op {
            "r" => {
                let key = operands.parse::<counters::Key>()?;
                Ok(CounterOp::Read{key})
            },
            _ => Err(anyhow!("bad query"))
        }
    }
}
