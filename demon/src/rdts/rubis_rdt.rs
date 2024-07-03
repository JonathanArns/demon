use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use indexmap::IndexMap;

use super::Operation;

#[derive(Clone, Debug, Default)]
pub struct State {
    /// registered users
    users: HashSet<String>,
    /// item stock levels
    items: IndexMap<String, usize>,
    auctions: IndexMap<usize, Auction>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Auction {
    id: usize,
    description: String,
    bids: Vec<Bid>,
    winning_bid: Option<Bid>,
    closed: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Bid {
    amount: f64,
    user_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RubisOp {
    RegisterUser{
        username: String,
    },

    GetAuction,
    OpenAuction{
        auction_id: usize,
    },
    CloseAuction{
        auction_id: usize,
    },
    Bid{
        auction_id: usize,
        amount: f64,
        user: String
    },

    Sell{
        item: String,
        stock: usize,
    },
    BuyNow{
        item: String,
        amount: usize,
    },
    GetItem,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReturnVal {
    Success,
    Err(String),
    Auction(Auction),
    Winner(Bid),
    Item(String),
}

pub struct BenchState {
    user: String,
    auction: Auction,
    item: String,
}

impl Default for BenchState {
    fn default() -> Self {
        BenchState {
            user: "default_user".to_string(),
            auction: Default::default(),
            item: Default::default(),
        }
    }
}

impl Operation for RubisOp {
    type State = State;
    type ReadVal = ReturnVal;
    type QueryState = BenchState;

    fn is_red(&self) -> bool {
        match *self {
            Self::RegisterUser{..} => true,

            Self::GetAuction => false,
            Self::OpenAuction{..} => false,
            Self::CloseAuction{..} => true,
            Self::Bid{..} => true,

            Self::GetItem => false,
            Self::Sell{..} => false,
            Self::BuyNow{..} => true,
        }
    }

    fn is_semiserializable_strong(&self) -> bool {
        match *self {
            Self::RegisterUser{..} => true,

            Self::GetAuction => false,
            Self::OpenAuction{..} => false,
            Self::CloseAuction{..} => true,
            Self::Bid{..} => false,

            Self::GetItem => false,
            Self::Sell{..} => false,
            Self::BuyNow{..} => true,
        }
    }

    fn is_conflicting(&self, other: &Self) -> bool {
        match *self {
            Self::CloseAuction{auction_id: id1} => match *other {
                Self::Bid{auction_id: id2, ..} => id1 == id2,
                _ => false,
            },

            _ => false,
        }
    }

    fn rollback_conflicting_state(&self, source: &Self::State, target: &mut Self::State) {
        match *self {
            Self::CloseAuction{auction_id: id} => {
                if let Some(auction) = source.auctions.get(&id) {
                    target.auctions.insert(id, auction.clone());
                }
            },
            _ => (),
        }
    }

    fn is_writing(&self) -> bool {
        match *self {
            Self::RegisterUser{..} => true,

            Self::GetAuction => false,
            Self::OpenAuction{..} => true,
            Self::CloseAuction{..} => true,
            Self::Bid{..} => true,

            Self::GetItem => false,
            Self::Sell{..} => true,
            Self::BuyNow{..} => true,
        }
    }

    fn is_por_conflicting(&self, other: &Self) -> bool {
        match *self {
            Self::RegisterUser{..} => match *other {
                Self::RegisterUser {..} => true,
                _ => false,
            },

            Self::GetAuction => false,
            Self::OpenAuction{..} => false,
            Self::CloseAuction{auction_id: id1} => match *other {
                Self::Bid{auction_id: id2, ..} => id1 == id2,
                _ => false,
            },
            Self::Bid{auction_id: id1, ..} => match *other {
                Self::CloseAuction {auction_id: id2} => id1 == id2,
                _ => false,
            },

            Self::GetItem => false,
            Self::Sell{..} => false,
            Self::BuyNow{item: ref id1, ..} => match *other {
                Self::BuyNow{item: ref id2, ..} => *id1 == *id2,
                _ => false,
            },
        }
    }

    fn parse(_text: &str) -> anyhow::Result<Self> {
        unimplemented!("please use the bench endpoint")
    }

    fn generate_shadow(&self, _state: &Self::State) -> Option<Self> {
        match *self {
            Self::RegisterUser{..} => Some(self.clone()),

            Self::GetAuction{..} => None,
            Self::OpenAuction{..} => Some(self.clone()),
            Self::CloseAuction{..} => Some(self.clone()),
            Self::Bid{..} => Some(self.clone()),

            Self::GetItem => None,
            Self::Sell{..} => Some(self.clone()),
            Self::BuyNow{..} => Some(self.clone()),
        }
    }

    fn apply(&self, state: &mut Self::State) -> Option<Self::ReadVal> {
        match *self {
            Self::RegisterUser { ref username } => {
                if !state.users.contains(username) {
                    state.users.insert(username.clone());
                    Some(ReturnVal::Success)
                } else {
                    Some(ReturnVal::Err("username taken".to_string()))
                }
            },
            Self::Bid { ref user, auction_id, amount } => {
                if !state.auctions.contains_key(&auction_id) {
                    return Some(ReturnVal::Err("auction does not exist".to_string()))
                }
                if let Some(auction) = state.auctions.get_mut(&auction_id) {
                    if !auction.closed {
                        auction.bids.push(
                            Bid{ user_id: user.clone(), amount }
                        );
                        Some(ReturnVal::Success)
                    } else {
                        Some(ReturnVal::Err("auction already closed".to_string()))
                    }
                } else {
                    unreachable!()
                }
            },
            Self::CloseAuction { auction_id: id } => {
                if let Some(auction) = state.auctions.get_mut(&id) {
                    if !auction.closed {
                        auction.closed = true;
                        auction.bids.sort_by_key(|b| -(b.amount as i64));
                        auction.winning_bid = auction.bids.first().map(|x| x.to_owned());
                        if let Some(ref winner) = auction.winning_bid {
                            Some(ReturnVal::Winner(winner.clone()))
                        } else {
                            Some(ReturnVal::Success)
                        }
                    } else {
                        Some(ReturnVal::Err("auction already closed".to_string()))
                    }
                } else {
                    Some(ReturnVal::Err("auction does not exist".to_string()))
                }
            },
            Self::OpenAuction { auction_id } => {
                if !state.auctions.contains_key(&auction_id) {
                    state.auctions.insert(auction_id, Auction{ description: String::new(), bids: vec![], winning_bid: None, closed: false, id: rand::random()});
                    return Some(ReturnVal::Success)
                }
                Some(ReturnVal::Err("id in use".to_string()))
            },
            Self::Sell { ref item, stock } => {
                if !state.items.contains_key(item) {
                    state.items.insert(item.clone(), stock);
                } else {
                    *state.items.get_mut(item).unwrap() += stock;
                }
                Some(ReturnVal::Success)
            }
            Self::BuyNow { ref item, amount } => {
                if let Some(stock) = state.items.get_mut(item) {
                    if *stock >= amount {
                        *stock -= amount;
                        return Some(ReturnVal::Success)
                    }
                }
                Some(ReturnVal::Err("out of stock".to_string()))
            },
            Self::GetAuction => {
                if state.auctions.is_empty() {
                    return Some(ReturnVal::Err("no auctions yet".to_string()))
                }
                let idx = thread_rng().gen_range(0..state.auctions.len());
                state.auctions.get_index(idx).map(|(k, v)| ReturnVal::Auction(v.clone()))
            }
            Self::GetItem => {
                if state.items.is_empty() {
                    return Some(ReturnVal::Err("no auctions yet".to_string()))
                }
                let idx = thread_rng().gen_range(0..state.items.len());
                state.items.get_index(idx).map(|(k, v)| ReturnVal::Item(k.clone()))
            }
        }
    }

    fn gen_query(_settings: &crate::api::http::BenchSettings, state: &mut Self::QueryState) -> Self {
        let mut rng = thread_rng();
        let x = rng.gen_range(0..100);
        if x < 2 {
            Self::CloseAuction { auction_id: state.auction.id }
        } else if x < 2 + 3 {
            Self::OpenAuction { auction_id: state.auction.id }
        } else if x < 2 + 3 + 45 {
            Self::Bid { auction_id: state.auction.id, amount: rng.gen(), user: state.user.clone() }
        } else if x < 2 + 3 + 45 + 10 {
            Self::BuyNow { item: state.item.clone(), amount: rng.gen_range(1..10) }
        } else if x < 2 + 3 + 45 + 10 + 5 {
            Self::Sell { item: (&mut rng).sample_iter(&Alphanumeric).take(8).map(char::from).collect::<String>(), stock: rng.gen_range(100..100000) }
        } else if x < 2 + 3 + 45 + 10 + 5 + 5 {
            let username = rng.sample_iter(&Alphanumeric).take(8).map(char::from).collect::<String>();
            state.user = username.clone();
            Self::RegisterUser { username }
        } else if x < 2 + 3 + 45 + 10 + 5 + 5 + 20 {
            Self::GetAuction
        } else if x < 2 + 3 + 45 + 10 + 5 + 5 + 20 + 10 {
            Self::GetItem
        } else {
            unreachable!()
        }
    }

    fn update_query_state(state: &mut Self::QueryState, val: Self::ReadVal) {
        match val {
            ReturnVal::Auction(auction) => {
                state.auction = auction;
            },
            ReturnVal::Item(item) => {
                state.item = item;
            },
            _ => (),
        }
    }
}
