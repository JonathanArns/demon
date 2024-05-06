use std::collections::{HashMap, HashSet};

use anyhow::anyhow;
use serde::{Deserialize, Serialize};

use super::Operation;

pub type AuctionId = u64;
pub type Price = u64;
pub type UserId = String;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RubisOp {
    RegisterUser{id: UserId},
    Bid{user: UserId, auction: AuctionId, val: Price},
    CloseAuction{id: AuctionId},
    // BuyNow{id: AuctionId},
}

// impl Operation for RubisOp {
//     /// (Users, Auctions)
//     type State = (HashSet<UserId>, HashMap<AuctionId, (bool, Vec<(UserId, Price)>)>);
//     /// (ok?, winner)
//     type ReadVal = (bool, Option<UserId>);

//     fn is_red(&self) -> bool {
//         match *self {
//             Self::RegisterUser{..} => true,
//             Self::Bid{..} => true,
//             Self::CloseAuction{..} => true,
//         }
//     }

//     fn is_semiserializable_strong(&self) -> bool {
//         match *self {
//             Self::RegisterUser{..} => true,
//             Self::Bid{..} => false,
//             Self::CloseAuction{..} => true,
//         }
//     }

//     fn is_conflicting(&self, other: &Self) -> bool {
//         todo!()
//     }

//     fn rollback_conflicting_state(&self, source: &Self::State, target: &mut Self::State) {
//         todo!()
//     }

//     fn is_writing(&self) -> bool {
//         match *self {
//             Self::RegisterUser{..} => true,
//             Self::Bid{..} => true,
//             Self::CloseAuction{..} => true,
//         }
//     }

//     fn is_por_conflicting(&self, other: &Self) -> bool {
//         match *self {
//             Self::RegisterUser{id: ref id1} => match *other {
//                 Self::RegisterUser {id: ref id2} => *id1 == *id2,
//                 _ => false,
//             },
//             Self::Bid{auction: a1, ..} => match *other {
//                 Self::CloseAuction{id: a2} => a1 == a2,
//                 _ => false,
//             },
//             Self::CloseAuction{id: a1} => match *other {
//                 Self::Bid {auction: a2, ..} => a1 == a2,
//                 _ => false,
//             },
//         }
//     }

//     fn parse(text: &str) -> anyhow::Result<Self> {
//         let parts = text.split(" ").collect::<Vec<_>>();
//         match parts.len() {
//             2 => match parts[0] {
//                 "register" => {
//                     Ok(Self::RegisterUser { id: parts[1].to_string() })
//                 },
//                 "close" => {
//                     let id: AuctionId = parts[1].parse()?;
//                     Ok(Self::CloseAuction { id })
//                 },
//                 _ => Err(anyhow!("bad query")),
//             },
//             4 => match parts[0] {
//                 "bid" => {
//                     let user = parts[1].to_owned();
//                     let auction: AuctionId = parts[2].parse()?;
//                     let val: Price = parts[3].parse()?;
//                     Ok(Self::Bid { user, auction, val })
//                 },
//                 _ => Err(anyhow!("bad query")),
//             },
//             _ => Err(anyhow!("bad query")),
//         }
//     }

//     fn apply(&self, state: &mut Self::State) -> Option<Self::ReadVal> {
//         match *self {
//             Self::RegisterUser { ref id } => {
//                 if !state.0.contains(id) {
//                     state.0.insert(id.clone());
//                     Some((true, None))
//                 } else {
//                     Some((false, None))
//                 }
//             },
//             Self::Bid { ref user, auction, val } => {
//                 if !state.1.contains_key(&auction) {
//                     state.1.insert(auction, (true, vec![]));
//                 }
//                 if let Some((active, bids)) = state.1.get_mut(&auction) {
//                     if *active {
//                         bids.push((user.clone(), val));
//                         Some((true, None))
//                     } else {
//                         Some((false, None))
//                     }
//                 } else {
//                     unreachable!()
//                 }
//             },
//             Self::CloseAuction { id } => {
//                 if let Some((active, bids)) = state.1.get_mut(&id) {
//                     if *active {
//                         *active = false;
//                         bids.sort_by_key(|b| -(b.1 as i64));
//                         Some((true, bids.first().map(|b| b.0.clone())))
//                     } else {
//                         // the auction was already closed, just return the highest bid
//                         Some((false, bids.first().map(|b| b.0.clone())))
//                     }
//                 } else {
//                     // the auction does not exist
//                     Some((false, None))
//                 }
//             },
//         }
//     }
// }
