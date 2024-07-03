use std::collections::{HashMap, HashSet};

use anyhow::anyhow;
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};

use super::Operation;

#[derive(Clone, Debug, Default)]
pub struct DB {
    categories: HashMap<usize, Category>,
    regions: HashMap<usize, Region>,
    users: HashMap<usize, User>,
    user_idx: HashMap<String, usize>,
    items: HashMap<usize, Item>,
    old_items: HashMap<usize, Item>,
    bids: HashMap<usize, Bid>,
    comments: HashMap<usize, Comment>,
    buy_now: HashMap<usize, BuyNow>,
}

#[derive(Clone, Debug, Default)]
pub struct Category {
    id: usize,
    name: String,
}

#[derive(Clone, Debug, Default)]
pub struct Region {
    id: usize,
    name: String,
}

#[derive(Clone, Debug, Default)]
pub struct User {
    id: usize,
    nickname: String,
    first: String,
    last: String,
    password: String,
    email: String,
    rating: u16,
    balance: f64,
    creation_date: u64,
    region_fk: usize,
}

#[derive(Clone, Debug, Default)]
pub struct Item {
    id: usize,
    name: String,
    description: String,
    initial_price: f64,
    quantity: usize,
    reserve_price: f64,
    buy_now: f64,
    nb_of_bids: usize,
    max_bid: f64,
    start_date: u64,
    end_date: u64,
    seller_fk: usize,
    category_fk: usize,
}

#[derive(Clone, Debug, Default)]
pub struct Bid {
    id: usize,
    user_fk: usize,
    item_fk: usize,
    quantity: usize,
    bid: f64,
    max_bid: f64,
    date: u64,
}

#[derive(Clone, Debug, Default)]
pub struct Comment {
    id: usize,
    from_user_fk: usize,
    to_user_fk: usize,
    item_fk: usize,
    rating: usize,
    date: u64,
    comment: String,
}

#[derive(Clone, Debug, Default)]
pub struct BuyNow {
    id: usize,
    buyer_fk: usize,
    item_fk: usize,
    quantity: usize,
    date: u64,
}

// CREATE TABLE ids (
//    id        INTEGER UNSIGNED NOT NULL UNIQUE,
//    category  INTEGER UNSIGNED NOT NULL,
//    region    INTEGER UNSIGNED NOT NULL,
//    users     INTEGER UNSIGNED NOT NULL,
//    item      INTEGER UNSIGNED NOT NULL,
//    comment   INTEGER UNSIGNED NOT NULL,
//    bid       INTEGER UNSIGNED NOT NULL,
//    buyNow    INTEGER UNSIGNED NOT NULL,
//    PRIMARY KEY(id)
// );

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RubisOp {
    GetStatus,
    RegisterUser{},
    OpenAuction{},
    CloseAuction{},
    Bid{},
    BuyNow{},
    // Comment{},
}

impl Operation for RubisOp {
    /// (Users, Auctions)
    type State = DB;
    /// (ok?, winner)
    type ReadVal = ();

    fn is_red(&self) -> bool {
        match *self {
            Self::RegisterUser{..} => true,
            Self::Bid{..} => true,
            Self::CloseAuction{..} => true,
        }
    }

    fn is_semiserializable_strong(&self) -> bool {
        match *self {
            Self::RegisterUser{..} => true,
            Self::Bid{..} => false,
            Self::CloseAuction{..} => true,
        }
    }

    fn is_conflicting(&self, other: &Self) -> bool {
        match *self {
            Self::RegisterUser{id: ref id1} => match *other {
                Self::RegisterUser {id: ref id2} => *id1 == *id2,
                _ => false,
            },
            Self::Bid{auction: a1, ..} => match *other {
                Self::CloseAuction{id: a2} => a1 == a2,
                _ => false,
            },
            Self::CloseAuction{id: a1} => match *other {
                Self::Bid {auction: a2, ..} => a1 == a2,
                _ => false,
            },
        }
    }

    fn rollback_conflicting_state(&self, source: &Self::State, target: &mut Self::State) {
        match *self {
            Self::RegisterUser{id: ref id} => {
                todo!()
            },
            Self::Bid{auction: a1, ..} => {
                todo!()
            },
            Self::CloseAuction{id: a1} => {
                todo!()
            },
        }
    }

    fn is_writing(&self) -> bool {
        match *self {
            Self::RegisterUser{..} => true,
            Self::Bid{..} => true,
            Self::CloseAuction{..} => true,
        }
    }

    fn is_por_conflicting(&self, other: &Self) -> bool {
        match *self {
            Self::RegisterUser{id: ref id1} => match *other {
                Self::RegisterUser {id: ref id2} => *id1 == *id2,
                _ => false,
            },
            Self::Bid{auction: a1, ..} => match *other {
                Self::CloseAuction{id: a2} => a1 == a2,
                _ => false,
            },
            Self::CloseAuction{id: a1} => match *other {
                Self::Bid {auction: a2, ..} => a1 == a2,
                _ => false,
            },
        }
    }

    fn parse(text: &str) -> anyhow::Result<Self> {
        let parts = text.split(" ").collect::<Vec<_>>();
        match parts.len() {
            2 => match parts[0] {
                "register" => {
                    Ok(Self::RegisterUser { id: parts[1].to_string() })
                },
                "close" => {
                    let id: AuctionId = parts[1].parse()?;
                    Ok(Self::CloseAuction { id })
                },
                _ => Err(anyhow!("bad query")),
            },
            4 => match parts[0] {
                "bid" => {
                    let user = parts[1].to_owned();
                    let auction: AuctionId = parts[2].parse()?;
                    let val: Price = parts[3].parse()?;
                    Ok(Self::Bid { user, auction, val })
                },
                _ => Err(anyhow!("bad query")),
            },
            _ => Err(anyhow!("bad query")),
        }
    }

    fn generate_shadow(&self, state: &Self::State) -> Option<Self> {
        todo!()
    }

    fn apply(&self, state: &mut Self::State) -> Option<Self::ReadVal> {
        match *self {
            Self::RegisterUser { ref id } => {
                if !state.0.contains(id) {
                    state.0.insert(id.clone());
                    Some((true, None))
                } else {
                    Some((false, None))
                }
            },
            Self::Bid { ref user, auction, val } => {
                if !state.1.contains_key(&auction) {
                    state.1.insert(auction, (true, vec![]));
                }
                if let Some((active, bids)) = state.1.get_mut(&auction) {
                    if *active {
                        bids.push((user.clone(), val));
                        Some((true, None))
                    } else {
                        Some((false, None))
                    }
                } else {
                    unreachable!()
                }
            },
            Self::CloseAuction { id } => {
                if let Some((active, bids)) = state.1.get_mut(&id) {
                    if *active {
                        *active = false;
                        bids.sort_by_key(|b| -(b.1 as i64));
                        Some((true, bids.first().map(|b| b.0.clone())))
                    } else {
                        // the auction was already closed, just return the highest bid
                        Some((false, bids.first().map(|b| b.0.clone())))
                    }
                } else {
                    // the auction does not exist
                    Some((false, None))
                }
            },
        }
    }

    fn gen_query(settings: &crate::api::http::BenchSettings) -> Self {
        todo!()
    }
}


enum State {
    Home,
    Register,
    RegisterUser,
    Browse,
    Back,
    EndSession,
}

fn state_transition(from: State, previous_states: &mut Vec<State>) -> (RubisOp, State) {
    let random = thread_rng().gen_range(0..100);
    let next: State;
    match from {
        State::Home => {
            
        },
        State::Register => {

        },
    }


    previous_states.push(from);
    todo!()
}
