use std::collections::HashMap;

use rand::{Rng, thread_rng};
use serde::{Deserialize, Serialize};
use ropey::Rope;
use cola;

use super::Operation;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Role {
    Admin,
    Editor,
    Viewer,
    None,
}

impl Role {
    fn to_numeric(&self) -> usize {
        match *self {
            Self::None => 0,
            Self::Viewer => 1,
            Self::Editor => 2,
            Self::Admin => 3,
        }
    }
}

#[derive(Debug)]
pub struct TextReplica {
    replica: cola::Replica,
    buffer: Rope,
    user_roles: HashMap<String, Role>
}

impl Default for TextReplica {
    fn default() -> Self {
        Self {
            replica: cola::Replica::new(thread_rng().gen(), 0),
            buffer: Rope::new(),
            user_roles: Default::default(),
        }
    }
}

impl Clone for TextReplica {
    fn clone(&self) -> Self {
        Self {
            replica: self.replica.fork(self.replica.id() + 1000),
            buffer: self.buffer.clone(),
            user_roles: self.user_roles.clone(),
        }
    }
}

impl TextReplica {
    fn has_permission(&self, op: &EditorOp) -> bool {
        let user = op.get_user();
        if user == "root" {
            return true
        }
        if let Some(role) = self.user_roles.get(user) {
            role.to_numeric() >= op.min_role().to_numeric()
        } else {
            false
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EditorOp {
    Insert{at: usize, text: String, user: String},
    InsertShadow{crdt: cola::Insertion, text: String, user: String},

    Delete{at: usize, length: usize, user: String},
    DeleteShadow{crdt: cola::Deletion, user: String},

    ChangeRole{user: String, target_user: String, role: Role},

    CreateUser{user: String, target_user: String, role: Role},
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReadVal {
    /// Contains the current length of the buffer.
    Length(usize),
    Unauthorized,
}

impl EditorOp {
    fn min_role(&self) -> Role {
        match *self {
            Self::Insert{..} => Role::Editor,
            Self::InsertShadow{..} => Role::Editor,
            Self::Delete{..} => Role::Editor,
            Self::DeleteShadow{..} => Role::Editor,
            Self::ChangeRole{..} => Role::Admin,
            Self::CreateUser{..} => Role::Admin,
        }
    }

    fn get_user(&self) -> &str {
        match *self {
            Self::Insert{ref user, ..} => user,
            Self::InsertShadow{ref user, ..} => user,
            Self::Delete{ref user, ..} => user,
            Self::DeleteShadow{ref user, ..} => user,
            Self::ChangeRole{ref user, ..} => user,
            Self::CreateUser{ref user, ..} => user,
        }
    }
}

impl Operation for EditorOp {
    /// buffer length, edit-user, admin-user, can-edit
    type QueryState = (usize, Option<String>, Option<String>, bool);
    type State = TextReplica;
    type ReadVal = ReadVal;

    fn is_red(&self) -> bool {
        match *self {
            Self::Insert{..} => true,
            Self::InsertShadow{..} => true,
            Self::Delete{..} => true,
            Self::DeleteShadow{..} => true,
            Self::ChangeRole{..} => true,
            Self::CreateUser{..} => true,
        }
    }

    fn is_semiserializable_strong(&self) -> bool {
        match *self {
            Self::Insert{..} => false,
            Self::InsertShadow{..} => false,
            Self::Delete{..} => false,
            Self::DeleteShadow{..} => false,
            Self::ChangeRole{..} => true,
            Self::CreateUser{..} => true,
        }
    }

    fn is_conflicting(&self, _other: &Self) -> bool {
        true
    }

    fn rollback_conflicting_state(&self, source: &Self::State, target: &mut Self::State) {
        target.user_roles = source.user_roles.clone();
        target.buffer = source.buffer.clone();
        target.replica = source.replica.fork(source.replica.id() + 1000);
    }

    fn is_writing(&self) -> bool {
        match *self {
            Self::Insert{..} => true,
            Self::InsertShadow{..} => true,
            Self::Delete{..} => true,
            Self::DeleteShadow{..} => true,
            Self::ChangeRole{..} => true,
            Self::CreateUser{..} => true,
        }
    }

    fn is_por_conflicting(&self, other: &Self) -> bool {
        match self {
            Self::ChangeRole { target_user, ..} | Self::CreateUser { target_user, ..} => {
                match other {
                    Self::Insert { user, .. } | Self::InsertShadow { user, .. } | Self::Delete { user, .. } | Self::DeleteShadow { user, .. } => {
                        *user == *target_user
                    },
                    _ => true,
                }
            },
            Self::Insert { user, .. } | Self::InsertShadow { user, .. } | Self::Delete { user, .. } | Self::DeleteShadow { user, .. } => {
                match other {
                    Self::ChangeRole { target_user, ..} | Self::CreateUser { target_user, ..} => {
                        *user == *target_user
                    },
                    _ => false,
                }
            }
        }
    }

    fn parse(_text: &str) -> anyhow::Result<Self> {
        unimplemented!("please use the benchmark api for this data type")
    }

    fn apply(&self, state: &mut Self::State) -> Option<Self::ReadVal> {
        if !state.has_permission(self) {
            return Some(Self::ReadVal::Unauthorized)
        }
        match *self {
            Self::Insert { at, ref text, .. } => {
                state.buffer.insert(at, text);
                Some(Self::ReadVal::Length(state.buffer.len_chars()))
            },
            Self::InsertShadow { ref crdt, ref text, .. } => {
                if let Some(offset) = state.replica.integrate_insertion(crdt) {
                    state.buffer.insert(offset, text);
                }
                Some(Self::ReadVal::Length(state.buffer.len_chars()))
            },
            Self::Delete { at, ref length, .. } => {
                state.buffer.remove(at..(at+length));
                Some(Self::ReadVal::Length(state.buffer.len_chars()))
            },
            Self::DeleteShadow { ref crdt, .. } => {
                let ranges = state.replica.integrate_deletion(crdt);
                for range in ranges {
                    state.buffer.remove(range);
                }
                Some(Self::ReadVal::Length(state.buffer.len_chars()))
            },
            Self::ChangeRole { ref target_user, ref role, .. } | Self::CreateUser { ref target_user, ref role, .. } => {
                state.user_roles.insert(target_user.to_owned(), role.to_owned());
                None
            },
        }
    }

    fn generate_shadow_mut(&self, state: &mut Self::State) -> Option<Self> {
        match *self {
            Self::Insert { at, ref text, ref user } => Some(Self::InsertShadow {
                crdt: state.replica.create_insertion(at, text.len()),
                text: text.to_owned(),
                user: user.to_owned()
            }),
            Self::InsertShadow{..} => None,
            Self::Delete { at, length, ref user } => Some(Self::DeleteShadow {
                crdt: state.replica.create_deletion(at..(at+length)),
                user: user.to_owned()
            }),
            Self::DeleteShadow{..} => None,
            Self::ChangeRole{..} => Some(self.clone()),
            Self::CreateUser{..} => Some(self.clone()),
        }
    }
    
    fn name(&self) -> String {
        match *self {
            Self::Insert{..} => String::from("Insert"),
            Self::InsertShadow{..} => String::from("Insert"),
            Self::Delete{..} => String::from("Delete"),
            Self::DeleteShadow{..} => String::from("Delete"),
            Self::ChangeRole{..} => String::from("ChangeRole"),
            Self::CreateUser{..} => String::from("CreateUser"),
        }
    }

    fn gen_query(settings: &crate::api::http::BenchSettings, state: &mut Self::QueryState) -> Self {
        let mut rng = thread_rng();
        
        let (length, edit_user, admin_user, can_edit) = state;
        if *edit_user == None {
            *edit_user = Some(rng.gen_range(0..100000000).to_string());
            return Self::CreateUser {
                user: String::from("root"),
                target_user: edit_user.clone().unwrap(),
                role: Role::Editor,
            }
        }
        if *admin_user == None {
            *admin_user = Some(rng.gen_range(0..100000000).to_string());
            return Self::CreateUser {
                user: String::from("root"),
                target_user: admin_user.clone().unwrap(),
                role: Role::Editor,
            }
        }
        if !*can_edit {
            *can_edit = true;
            return Self::ChangeRole {
                user: admin_user.clone().unwrap(),
                target_user: edit_user.clone().unwrap(),
                role: Role::Editor,
            }
        }
        let x = rng.gen_range(0..10000);
        let pos = rng.gen_range(0..(31.max(*length) - 30));
        if x < (settings.strong_ratio * 10001.0).floor() as usize {
            *can_edit = false;
            Self::ChangeRole {
                user: admin_user.clone().unwrap(),
                target_user: edit_user.clone().unwrap(),
                role: Role::Viewer,
            }
        } else if x < 3000 && *length > 100 {
            Self::Delete { at: pos, length: 3, user: edit_user.clone().unwrap() }
        } else {
            Self::Insert { at: pos, text: String::from("abc"), user: edit_user.clone().unwrap() }
        }

    }

    fn update_query_state(state: &mut Self::QueryState, val: Self::ReadVal) {
        if let Self::ReadVal::Length(len) = val {
            state.0 = len;
        }
    }
}
