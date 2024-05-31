use std::collections::{HashMap, HashSet, VecDeque};

use anyhow::anyhow;
use serde::{Deserialize, Serialize};

use super::Operation;

#[derive(Clone, Debug, Default)]
pub struct DB {
    /// w_id -> Warehouse
    warehouses: HashMap<u16, Warehouse>,
    /// i_id -> Item
    items: HashMap<usize, Item>,
    /// (s_w_id, s_i_id) -> Stock
    stock: HashMap<(u16, usize), Stock>,
    /// some tables are locally partitioned by warehouse
    by_warehouse: HashMap<u16, ByWarehouse>,
}

impl DB {
    fn wh(&self, w_id: &u16) -> &ByWarehouse {
        self.by_warehouse.get(w_id).unwrap()
    }

    fn wh_mut(&mut self, w_id: &u16) -> &mut ByWarehouse {
        if !self.by_warehouse.contains_key(w_id) {
            self.by_warehouse.insert(*w_id, Default::default());
        }
        self.by_warehouse.get_mut(w_id).unwrap()
    }
}

#[derive(Clone, Debug, Default)]
pub struct ByWarehouse {
    /// (d_w_id, d_id) -> District
    districts: HashMap<u8, District>,
    /// (o_w_id, o_d_id, o_id) -> Order
    orders: HashMap<(u8, usize), Order>,
    /// (no_w_id, no_d_id, no_o_id) -> NewOrder
    new_orders: HashMap<(u8, usize), NewOrder>,
    /// (no_w_id, no_d_id) -> no_o_id
    new_order_index: HashMap<u8, VecDeque<usize>>,
    /// (ol_w_id,ol_d_id,ol_o_id,ol_number) -> OrderLine
    order_lines: HashMap<(u8, usize, usize), OrderLine>,
    /// History
    history: Vec<History>,
    /// (c_w_id, c_d_id, c_id) -> Customer
    customers: HashMap<(u8, usize), Customer>,
    /// c_last -> [Customer]
    customers_by_name: HashMap<String, Vec<Customer>>,
}

impl ByWarehouse {
    fn get_customer_by_name(&self, c_last: &str) -> &Customer {
        let customers = self.customers_by_name.get(c_last).unwrap();
        let count = customers.len();
        &customers[count / 2]
    }

    fn get_customer_by_name_mut(&mut self, c_last: &str) -> &mut Customer {
        let customers = self.customers_by_name.get_mut(c_last).unwrap();
        let count = customers.len();
        &mut customers[count / 2]
    }
}

#[derive(Clone, Debug, Default)]
pub struct Warehouse {
    w_id: u16,
    w_name: String,
    w_street_1: String,
    w_street_2: String,
    w_city: String,
    w_state: String,
    w_zip: String,
    w_tax: f64,
    w_ytd: f64,
}

// CREATE TABLE WAREHOUSE (
//   W_ID SMALLINT DEFAULT '0' NOT NULL,
//   W_NAME VARCHAR(16) DEFAULT NULL,
//   W_STREET_1 VARCHAR(32) DEFAULT NULL,
//   W_STREET_2 VARCHAR(32) DEFAULT NULL,
//   W_CITY VARCHAR(32) DEFAULT NULL,
//   W_STATE VARCHAR(2) DEFAULT NULL,
//   W_ZIP VARCHAR(9) DEFAULT NULL,
//   W_TAX FLOAT DEFAULT NULL,
//   W_YTD FLOAT DEFAULT NULL,
//   CONSTRAINT W_PK_ARRAY PRIMARY KEY (W_ID)
// );

#[derive(Clone, Debug, Default)]
pub struct District {
    d_id: u8,
    d_w_id: u16,
    d_name: String,
    d_street_1: String,
    d_street_2: String,
    d_city: String,
    d_state: String,
    d_zip: String,
    d_tax: f64,
    d_ytd: f64,
    d_next_o_id: usize,
}

// CREATE TABLE DISTRICT (
//   D_ID TINYINT DEFAULT '0' NOT NULL,
//   D_W_ID SMALLINT DEFAULT '0' NOT NULL REFERENCES WAREHOUSE (W_ID),
//   D_NAME VARCHAR(16) DEFAULT NULL,
//   D_STREET_1 VARCHAR(32) DEFAULT NULL,
//   D_STREET_2 VARCHAR(32) DEFAULT NULL,
//   D_CITY VARCHAR(32) DEFAULT NULL,
//   D_STATE VARCHAR(2) DEFAULT NULL,
//   D_ZIP VARCHAR(9) DEFAULT NULL,
//   D_TAX FLOAT DEFAULT NULL,
//   D_YTD FLOAT DEFAULT NULL,
//   D_NEXT_O_ID INT DEFAULT NULL,
//   PRIMARY KEY (D_W_ID,D_ID)
// );

#[derive(Clone, Debug, Default)]
pub struct Item {
    i_id: usize,
    i_im_id: usize,
    i_name: String,
    i_price: f64,
    i_data: String,
}

// CREATE TABLE ITEM (
//   I_ID INTEGER DEFAULT '0' NOT NULL,
//   I_IM_ID INTEGER DEFAULT NULL,
//   I_NAME VARCHAR(32) DEFAULT NULL,
//   I_PRICE FLOAT DEFAULT NULL,
//   I_DATA VARCHAR(64) DEFAULT NULL,
//   CONSTRAINT I_PK_ARRAY PRIMARY KEY (I_ID)
// );

#[derive(Clone, Debug, Default)]
pub struct Customer {
    c_id: usize,
    c_d_id: u8,
    c_w_id: u16,
    c_first: String,
    c_middle: String,
    c_last: String,
    c_street_1: String,
    c_street_2: String,
    c_city: String,
    c_state: String,
    c_zip: String,
    c_phone: String,
    c_since: String,
    c_credit: String,
    c_credit_lim: f64,
    c_discount: f64,
    c_balance: f64,
    c_ytd_payment: f64,
    c_payment_cnt: usize,
    c_delivery_cnt: usize,
    c_data: String,
}

// CREATE TABLE CUSTOMER (
//   C_ID INTEGER DEFAULT '0' NOT NULL,
//   C_D_ID TINYINT DEFAULT '0' NOT NULL,
//   C_W_ID SMALLINT DEFAULT '0' NOT NULL,
//   C_FIRST VARCHAR(32) DEFAULT NULL,
//   C_MIDDLE VARCHAR(2) DEFAULT NULL,
//   C_LAST VARCHAR(32) DEFAULT NULL,
//   C_STREET_1 VARCHAR(32) DEFAULT NULL,
//   C_STREET_2 VARCHAR(32) DEFAULT NULL,
//   C_CITY VARCHAR(32) DEFAULT NULL,
//   C_STATE VARCHAR(2) DEFAULT NULL,
//   C_ZIP VARCHAR(9) DEFAULT NULL,
//   C_PHONE VARCHAR(32) DEFAULT NULL,
//   C_SINCE TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
//   C_CREDIT VARCHAR(2) DEFAULT NULL,
//   C_CREDIT_LIM FLOAT DEFAULT NULL,
//   C_DISCOUNT FLOAT DEFAULT NULL,
//   C_BALANCE FLOAT DEFAULT NULL,
//   C_YTD_PAYMENT FLOAT DEFAULT NULL,
//   C_PAYMENT_CNT INTEGER DEFAULT NULL,
//   C_DELIVERY_CNT INTEGER DEFAULT NULL,
//   C_DATA VARCHAR(500),
//   PRIMARY KEY (C_W_ID,C_D_ID,C_ID),
//   UNIQUE (C_W_ID,C_D_ID,C_LAST,C_FIRST),
//   CONSTRAINT C_FKEY_D FOREIGN KEY (C_D_ID, C_W_ID) REFERENCES DISTRICT (D_ID, D_W_ID)
// );
// CREATE INDEX IDX_CUSTOMER ON CUSTOMER (C_W_ID,C_D_ID,C_LAST);

#[derive(Clone, Debug, Default)]
pub struct History {
    h_c_id: usize,
    h_c_d_id: u8,
    h_c_w_id: u16,
    h_d_id: u8,
    h_w_id: u16,
    h_date: String,
    h_amount: f64,
    h_data: String,
}

// CREATE TABLE HISTORY (
//   H_C_ID INTEGER DEFAULT NULL,
//   H_C_D_ID TINYINT DEFAULT NULL,
//   H_C_W_ID SMALLINT DEFAULT NULL,
//   H_D_ID TINYINT DEFAULT NULL,
//   H_W_ID SMALLINT DEFAULT '0' NOT NULL,
//   H_DATE TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
//   H_AMOUNT FLOAT DEFAULT NULL,
//   H_DATA VARCHAR(32) DEFAULT NULL,
//   CONSTRAINT H_FKEY_C FOREIGN KEY (H_C_ID, H_C_D_ID, H_C_W_ID) REFERENCES CUSTOMER (C_ID, C_D_ID, C_W_ID),
//   CONSTRAINT H_FKEY_D FOREIGN KEY (H_D_ID, H_W_ID) REFERENCES DISTRICT (D_ID, D_W_ID)
// );

#[derive(Clone, Debug, Default)]
pub struct Stock {
    s_i_id: usize,
    s_w_id: u16,
    s_quantity: usize,
    s_dist_01: String,
    s_dist_02: String,
    s_dist_03: String,
    s_dist_04: String,
    s_dist_05: String,
    s_dist_06: String,
    s_dist_07: String,
    s_dist_08: String,
    s_dist_09: String,
    s_dist_10: String,
    s_ytd: usize,
    s_order_cnt: usize,
    s_remote_cnt: usize,
    s_data: String,
}

// CREATE TABLE STOCK (
//   S_I_ID INTEGER DEFAULT '0' NOT NULL REFERENCES ITEM (I_ID),
//   S_W_ID SMALLINT DEFAULT '0 ' NOT NULL REFERENCES WAREHOUSE (W_ID),
//   S_QUANTITY INTEGER DEFAULT '0' NOT NULL,
//   S_DIST_01 VARCHAR(32) DEFAULT NULL,
//   S_DIST_02 VARCHAR(32) DEFAULT NULL,
//   S_DIST_03 VARCHAR(32) DEFAULT NULL,
//   S_DIST_04 VARCHAR(32) DEFAULT NULL,
//   S_DIST_05 VARCHAR(32) DEFAULT NULL,
//   S_DIST_06 VARCHAR(32) DEFAULT NULL,
//   S_DIST_07 VARCHAR(32) DEFAULT NULL,
//   S_DIST_08 VARCHAR(32) DEFAULT NULL,
//   S_DIST_09 VARCHAR(32) DEFAULT NULL,
//   S_DIST_10 VARCHAR(32) DEFAULT NULL,
//   S_YTD INTEGER DEFAULT NULL,
//   S_ORDER_CNT INTEGER DEFAULT NULL,
//   S_REMOTE_CNT INTEGER DEFAULT NULL,
//   S_DATA VARCHAR(64) DEFAULT NULL,
//   PRIMARY KEY (S_W_ID,S_I_ID)
// );

#[derive(Clone, Debug, Default)]
pub struct Order {
    o_id: usize,
    o_c_id: usize,
    o_d_id: u8,
    o_w_id: u16,
    o_entry_d: String,
    o_carrier_id: usize,
    o_ol_cnt: usize,
    o_all_local: usize,
}

// CREATE TABLE ORDERS (
//   O_ID INTEGER DEFAULT '0' NOT NULL,
//   O_C_ID INTEGER DEFAULT NULL,
//   O_D_ID TINYINT DEFAULT '0' NOT NULL,
//   O_W_ID SMALLINT DEFAULT '0' NOT NULL,
//   O_ENTRY_D TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
//   O_CARRIER_ID INTEGER DEFAULT NULL,
//   O_OL_CNT INTEGER DEFAULT NULL,
//   O_ALL_LOCAL INTEGER DEFAULT NULL,
//   PRIMARY KEY (O_W_ID,O_D_ID,O_ID),
//   UNIQUE (O_W_ID,O_D_ID,O_C_ID,O_ID),
//   CONSTRAINT O_FKEY_C FOREIGN KEY (O_C_ID, O_D_ID, O_W_ID) REFERENCES CUSTOMER (C_ID, C_D_ID, C_W_ID)
// );
// CREATE INDEX IDX_ORDERS ON ORDERS (O_W_ID,O_D_ID,O_C_ID);

#[derive(Clone, Debug, Default)]
pub struct NewOrder {
    no_o_id: usize,
    no_d_id: u8,
    no_w_id: u16,
}

// CREATE TABLE NEW_ORDER (
//   NO_O_ID INTEGER DEFAULT '0' NOT NULL,
//   NO_D_ID TINYINT DEFAULT '0' NOT NULL,
//   NO_W_ID SMALLINT DEFAULT '0' NOT NULL,
//   CONSTRAINT NO_PK_TREE PRIMARY KEY (NO_D_ID,NO_W_ID,NO_O_ID),
//   CONSTRAINT NO_FKEY_O FOREIGN KEY (NO_O_ID, NO_D_ID, NO_W_ID) REFERENCES ORDERS (O_ID, O_D_ID, O_W_ID)
// );

#[derive(Clone, Debug, Default)]
pub struct OrderLine {
    ol_o_id: usize,
    ol_d_id: u8,
    ol_w_id: u16,
    ol_number: usize,
    ol_i_id: usize,
    ol_supply_w_id: u16,
    ol_delivery_d: String,
    ol_quantity: usize,
    ol_amount: f64,
    ol_dist_info: String,
}

// CREATE TABLE ORDER_LINE (
//   OL_O_ID INTEGER DEFAULT '0' NOT NULL,
//   OL_D_ID TINYINT DEFAULT '0' NOT NULL,
//   OL_W_ID SMALLINT DEFAULT '0' NOT NULL,
//   OL_NUMBER INTEGER DEFAULT '0' NOT NULL,
//   OL_I_ID INTEGER DEFAULT NULL,
//   OL_SUPPLY_W_ID SMALLINT DEFAULT NULL,
//   OL_DELIVERY_D TIMESTAMP DEFAULT NULL,
//   OL_QUANTITY INTEGER DEFAULT NULL,
//   OL_AMOUNT FLOAT DEFAULT NULL,
//   OL_DIST_INFO VARCHAR(32) DEFAULT NULL,
//   PRIMARY KEY (OL_W_ID,OL_D_ID,OL_O_ID,OL_NUMBER),
//   CONSTRAINT OL_FKEY_O FOREIGN KEY (OL_O_ID, OL_D_ID, OL_W_ID) REFERENCES ORDERS (O_ID, O_D_ID, O_W_ID),
//   CONSTRAINT OL_FKEY_S FOREIGN KEY (OL_I_ID, OL_SUPPLY_W_ID) REFERENCES STOCK (S_I_ID, S_W_ID)
// );




#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TpccOp {
    LoadTuples{
        table: String,
        tuples: Vec<String>,
    },
    Delivery{
        w_id: u16,
        o_carrier_id: usize,
        ol_delivery_d: String,
    },
    NewOrder{
        w_id: u16,
        d_id: u8,
        c_id: usize,
        o_entry_d: String,
        i_ids: Vec<usize>,
        i_w_ids: Vec<u16>,
        i_qtys: Vec<usize>,
    },
    OrderStatus{
        w_id: u16,
        d_id: u8,
        c_id: Option<usize>,
        c_last: String,
    },
    Payment{
        w_id: u16,
        d_id: u8,
        h_amount: f64,
        c_w_id: u16,
        c_d_id: u8,
        c_id: Option<usize>,
        c_last: String,
        h_date: String,
    },
    StockLevel{
        w_id: u16,
        d_id: u8,
        threshold: usize,
    },
}

impl Operation for TpccOp {
    type State = DB;
    type ReadVal = ();

    fn is_red(&self) -> bool {
        match *self {
            Self::LoadTuples {..} => true,
            Self::Delivery {..} => true,
            Self::NewOrder {..} => true,
            Self::OrderStatus {..} => false,
            Self::Payment {..} => false,
            Self::StockLevel {..} => false,
        }
    }

    fn is_semiserializable_strong(&self) -> bool {
        match *self {
            Self::LoadTuples {..} => true,
            Self::Delivery {..} => false,
            Self::NewOrder {..} => true,
            Self::OrderStatus {..} => false,
            Self::Payment {..} => false,
            Self::StockLevel {..} => false,
        }
    }

    fn is_writing(&self) -> bool {
        match *self {
            Self::LoadTuples {..} => true,
            Self::Delivery {..} => true,
            Self::NewOrder {..} => true,
            Self::OrderStatus {..} => false,
            Self::Payment {..} => true,
            Self::StockLevel {..} => false,
        }
    }

    fn is_por_conflicting(&self, other: &Self) -> bool {
        match *self {
            Self::Delivery{..} | Self::NewOrder {..} => match *other {
                Self::Delivery{..} | Self::NewOrder {..} => true,
                _ => false,
            },
            Self::LoadTuples {..} => false,
            Self::OrderStatus {..} => false,
            Self::Payment {..} => false,
            Self::StockLevel {..} => false,
        }
    }

    fn is_conflicting(&self, other: &Self) -> bool {
        match *self {
            Self::LoadTuples {..} => match other {
                Self::LoadTuples {..} => false,
                _ => {println!("{:?}", other); false},
            },
            Self::Delivery {..} => true,
            Self::NewOrder { w_id: no_w_id, .. } => {
                match other {
                    Self::Payment { w_id, .. } => {
                        no_w_id == *w_id
                    },
                    Self::Delivery { w_id, .. } => {
                        no_w_id == *w_id
                    },
                    _ => true,
                }
            },
            Self::OrderStatus {..} => true,
            Self::Payment {..} => true,
            Self::StockLevel {..} => true,
        }
    }

    fn rollback_conflicting_state(&self, source: &Self::State, target: &mut Self::State) {
        match self {
            Self::NewOrder { w_id, .. } => {
                *target.wh_mut(w_id) = source.wh(w_id).clone();
            },
            Self::LoadTuples { .. } => {
                *target = source.clone();
            },
            _ => unimplemented!("NewOrder is the only strong op"),
        }
    }

    fn parse(text: &str) -> anyhow::Result<Self> {
        let parts = text.split(" ").collect::<Vec<_>>();
        match parts[0] {
            "load_tuples" => {
                Ok(Self::LoadTuples {
                    table: parts[1].to_string(),
                    tuples: parts[2].split(";").map(|s| s.to_string()).collect(),
                })
            },
            "delivery" => {
                Ok(Self::Delivery{
                    w_id: parts[1].parse()?,
                    o_carrier_id: parts[2].parse()?,
                    ol_delivery_d: parts[2].parse()?,
                })
            },
            "new_order" => {
                Ok(Self::NewOrder {
                    w_id: parts[1].parse()?,
                    d_id: parts[2].parse()?,
                    c_id: parts[3].parse()?,
                    o_entry_d: parts[4].parse()?,
                    i_ids: parts[5].split(",").map(|s| s.parse().unwrap()).collect(),
                    i_w_ids: parts[6].split(",").map(|s| s.parse().unwrap()).collect(), 
                    i_qtys: parts[7].split(",").map(|s| s.parse().unwrap()).collect(),
                })
            },
            "order_status" => {
                Ok(Self::OrderStatus{
                    w_id: parts[1].parse()?,
                    d_id: parts[2].parse()?,
                    c_id: parts[3].parse::<usize>().ok(),
                    c_last: parts[4].parse()?,
                })
            },
            "payment" => {
                Ok(Self::Payment{
                    w_id: parts[1].parse()?,
                    d_id: parts[2].parse()?,
                    h_amount: parts[3].parse()?,
                    c_w_id: parts[4].parse()?,
                    c_d_id: parts[5].parse()?,
                    c_id: parts[6].parse::<usize>().ok(),
                    c_last: parts[7].parse()?,
                    h_date: parts[8].parse()?,
                })
            },
            "stock_level" => {
                Ok(Self::StockLevel{
                    w_id: parts[1].parse()?,
                    d_id: parts[2].parse()?,
                    threshold: parts[3].parse()?,
                })
            },
            _ => Err(anyhow!("bad query")),
        }
    }

    fn apply(&self, state: &mut Self::State) -> Option<Self::ReadVal> {
        match self {
            Self::LoadTuples{
                table,
                tuples,
            } => {
                match &table[..] {
                    "WAREHOUSE" => {
                        for tuple in tuples {
                            let mut values = tuple.split(",").into_iter();
                            let w_id: u16 = values.next().unwrap().parse().unwrap();
                            let w_name: String = values.next().unwrap().parse().unwrap();
                            let w_street_1: String = values.next().unwrap().parse().unwrap();
                            let w_street_2: String = values.next().unwrap().parse().unwrap();
                            let w_city: String = values.next().unwrap().parse().unwrap();
                            let w_state: String = values.next().unwrap().parse().unwrap();
                            let w_zip: String = values.next().unwrap().parse().unwrap();
                            let w_tax: f64 = values.next().unwrap().parse().unwrap();
                            let w_ytd: f64 = values.next().unwrap().parse().unwrap();
                            let val = Warehouse{w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd};
                            state.warehouses.insert(w_id, val);
                            state.wh_mut(&w_id); // inserts if not there yet
                        }
                    },
                    "DISTRICT" => {
                        for tuple in tuples {
                            let mut values = tuple.split(",").into_iter();
                            let d_id: u8 = values.next().unwrap().parse().unwrap();
                            let d_w_id: u16 = values.next().unwrap().parse().unwrap();
                            let d_name: String = values.next().unwrap().parse().unwrap();
                            let d_street_1: String = values.next().unwrap().parse().unwrap();
                            let d_street_2: String = values.next().unwrap().parse().unwrap();
                            let d_city: String = values.next().unwrap().parse().unwrap();
                            let d_state: String = values.next().unwrap().parse().unwrap();
                            let d_zip: String = values.next().unwrap().parse().unwrap();
                            let d_tax: f64 = values.next().unwrap().parse().unwrap();
                            let d_ytd: f64 = values.next().unwrap().parse().unwrap();
                            let d_next_o_id: usize = values.next().unwrap().parse().unwrap();
                            let val = District{d_id, d_w_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip, d_tax, d_ytd, d_next_o_id};
                            state.wh_mut(&d_w_id).districts.insert(d_id, val);
                        }
                    },
                    "ITEM" => {
                        for tuple in tuples {
                            let mut values = tuple.split(",").into_iter();
                            let i_id: usize = values.next().unwrap().parse().unwrap();
                            let i_im_id: usize = values.next().unwrap().parse().unwrap();
                            let i_name: String = values.next().unwrap().parse().unwrap();
                            let i_price: f64 = values.next().unwrap().parse().unwrap();
                            let i_data: String = values.next().unwrap().parse().unwrap();
                            let val = Item{i_id, i_im_id, i_name, i_price, i_data};
                            state.items.insert(i_id, val);
                        }
                    },
                    "CUSTOMER" => {
                        for tuple in tuples {
                            let mut values = tuple.split(",").into_iter();
                            let c_id: usize = values.next().unwrap().parse().unwrap();
                            let c_d_id: u8 = values.next().unwrap().parse().unwrap();
                            let c_w_id: u16 = values.next().unwrap().parse().unwrap();
                            let c_first: String = values.next().unwrap().parse().unwrap();
                            let c_middle: String = values.next().unwrap().parse().unwrap();
                            let c_last: String = values.next().unwrap().parse().unwrap();
                            let c_street_1: String = values.next().unwrap().parse().unwrap();
                            let c_street_2: String = values.next().unwrap().parse().unwrap();
                            let c_city: String = values.next().unwrap().parse().unwrap();
                            let c_state: String = values.next().unwrap().parse().unwrap();
                            let c_zip: String = values.next().unwrap().parse().unwrap();
                            let c_phone: String = values.next().unwrap().parse().unwrap();
                            let c_since: String = values.next().unwrap().parse().unwrap();
                            let c_credit: String = values.next().unwrap().parse().unwrap();
                            let c_credit_lim: f64 = values.next().unwrap().parse().unwrap();
                            let c_discount: f64 = values.next().unwrap().parse().unwrap();
                            let c_balance: f64 = values.next().unwrap().parse().unwrap();
                            let c_ytd_payment: f64 = values.next().unwrap().parse().unwrap();
                            let c_payment_cnt: usize = values.next().unwrap().parse().unwrap();
                            let c_delivery_cnt: usize = values.next().unwrap().parse().unwrap();
                            let c_data: String = values.next().unwrap().parse().unwrap();
                            let val = Customer{c_id, c_d_id, c_w_id, c_first, c_middle, c_last: c_last.clone(), c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data};
                            state.wh_mut(&c_w_id).customers.insert((c_d_id, c_id), val.clone());
                            if !state.wh(&c_w_id).customers_by_name.contains_key(&c_last) {
                                state.wh_mut(&c_w_id).customers_by_name.insert(c_last, vec![val]);
                            } else {
                                state.wh_mut(&c_w_id).customers_by_name.get_mut(&c_last).unwrap().push(val);
                            }
                        }
                    },
                    "HISTORY" => {
                        for tuple in tuples {
                            let mut values = tuple.split(",").into_iter();
                            let h_c_id: usize = values.next().unwrap().parse().unwrap();
                            let h_c_d_id: u8 = values.next().unwrap().parse().unwrap();
                            let h_c_w_id: u16 = values.next().unwrap().parse().unwrap();
                            let h_d_id: u8 = values.next().unwrap().parse().unwrap();
                            let h_w_id: u16 = values.next().unwrap().parse().unwrap();
                            let h_date: String = values.next().unwrap().parse().unwrap();
                            let h_amount: f64 = values.next().unwrap().parse().unwrap();
                            let h_data: String = values.next().unwrap().parse().unwrap();
                            let val = History{h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data};
                            state.wh_mut(&h_w_id).history.push(val);
                        }
                    },
                    "STOCK" => {
                        for tuple in tuples {
                            let mut values = tuple.split(",").into_iter();
                            let s_i_id: usize = values.next().unwrap().parse().unwrap();
                            let s_w_id: u16 = values.next().unwrap().parse().unwrap();
                            let s_quantity: usize = values.next().unwrap().parse().unwrap();
                            let s_dist_01: String = values.next().unwrap().parse().unwrap();
                            let s_dist_02: String = values.next().unwrap().parse().unwrap();
                            let s_dist_03: String = values.next().unwrap().parse().unwrap();
                            let s_dist_04: String = values.next().unwrap().parse().unwrap();
                            let s_dist_05: String = values.next().unwrap().parse().unwrap();
                            let s_dist_06: String = values.next().unwrap().parse().unwrap();
                            let s_dist_07: String = values.next().unwrap().parse().unwrap();
                            let s_dist_08: String = values.next().unwrap().parse().unwrap();
                            let s_dist_09: String = values.next().unwrap().parse().unwrap();
                            let s_dist_10: String = values.next().unwrap().parse().unwrap();
                            let s_ytd: usize = values.next().unwrap().parse().unwrap();
                            let s_order_cnt: usize = values.next().unwrap().parse().unwrap();
                            let s_remote_cnt: usize = values.next().unwrap().parse().unwrap();
                            let s_data: String = values.next().unwrap().parse().unwrap();
                            let val = Stock{s_i_id, s_w_id, s_quantity, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, s_ytd, s_order_cnt, s_remote_cnt, s_data};
                            state.stock.insert((s_w_id, s_i_id), val);
                        }
                    },
                    "ORDERS" => {
                        for tuple in tuples {
                            let mut values = tuple.split(",").into_iter();
                            let o_id: usize = values.next().unwrap().parse().unwrap();
                            let o_c_id: usize = values.next().unwrap().parse().unwrap();
                            let o_d_id: u8 = values.next().unwrap().parse().unwrap();
                            let o_w_id: u16 = values.next().unwrap().parse().unwrap();
                            let o_entry_d: String = values.next().unwrap().parse().unwrap();
                            let o_carrier_id: usize = values.next().unwrap().parse().unwrap();
                            let o_ol_cnt: usize = values.next().unwrap().parse().unwrap();
                            let o_all_local: usize = values.next().unwrap().parse().unwrap();
                            let val = Order{o_id, o_c_id, o_d_id, o_w_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local};
                            state.wh_mut(&o_w_id).orders.insert((o_d_id, o_id), val);
                        }
                    },
                    "NEW_ORDER" => {
                        for tuple in tuples {
                            let mut values = tuple.split(",").into_iter();
                            let no_o_id: usize = values.next().unwrap().parse().unwrap();
                            let no_d_id: u8 = values.next().unwrap().parse().unwrap();
                            let no_w_id: u16 = values.next().unwrap().parse().unwrap();
                            let val = NewOrder{no_o_id, no_d_id, no_w_id};
                            state.wh_mut(&no_w_id).new_orders.insert((no_d_id, no_o_id), val);
                            if let Some(dq) = state.wh_mut(&no_w_id).new_order_index.get_mut(&no_d_id) {
                                dq.push_front(no_o_id);
                                dq.make_contiguous().sort_unstable();
                            } else {
                                let mut dq = VecDeque::new();
                                dq.push_front(no_o_id);
                                state.wh_mut(&no_w_id).new_order_index.insert(no_d_id, dq);
                            }
                        }
                    },
                    "ORDER_LINE" => {
                        for tuple in tuples {
                            let mut values = tuple.split(",").into_iter();
                            let ol_o_id: usize = values.next().unwrap().parse().unwrap();
                            let ol_d_id: u8 = values.next().unwrap().parse().unwrap();
                            let ol_w_id: u16 = values.next().unwrap().parse().unwrap();
                            let ol_number: usize = values.next().unwrap().parse().unwrap();
                            let ol_i_id: usize = values.next().unwrap().parse().unwrap();
                            let ol_supply_w_id: u16 = values.next().unwrap().parse().unwrap();
                            let ol_delivery_d: String = values.next().unwrap().parse().unwrap();
                            let ol_quantity: usize = values.next().unwrap().parse().unwrap();
                            let ol_amount: f64 = values.next().unwrap().parse().unwrap();
                            let ol_dist_info: String = values.next().unwrap().parse().unwrap();
                            let val = OrderLine{ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info};
                            state.wh_mut(&ol_w_id).order_lines.insert((ol_d_id, ol_o_id, ol_number), val);
                        }
                    },
                    _ => unreachable!("bad table name: {:?}", table),
                }
                None
            },
            Self::Delivery{
                w_id,
                o_carrier_id,
                ol_delivery_d,
            } => {
                for d_id in 1..=10 {
                    if let Some(o_ids) = state.wh_mut(w_id).new_order_index.get_mut(&d_id) {
                        if o_ids.is_empty() {
                            continue
                        }
                        let o_id = o_ids.pop_front().unwrap();
                        let _new_order = state.wh_mut(w_id).new_orders.remove(&(d_id, o_id)).unwrap();
                        let order = state.wh_mut(w_id).orders.get_mut(&(d_id, o_id)).unwrap();
                        order.o_carrier_id = *o_carrier_id;
                        let o_c_id = order.o_c_id;
                        let ol_total = state.wh_mut(w_id).order_lines.iter_mut().filter(|(key, _ol)| {
                            key.0 == d_id && key.1 == o_id
                        }).map(|(_, ol)| {
                            // update OL
                            ol.ol_delivery_d = ol_delivery_d.to_owned();
                            // and return amount
                            ol.ol_amount
                        }).sum::<f64>();
                        let customer = state.wh_mut(w_id).customers.get_mut(&(d_id, o_c_id)).unwrap();
                        customer.c_balance += ol_total;
                    }
                }
                None
            },
            Self::NewOrder{
                w_id,
                d_id,
                c_id,
                o_entry_d,
                i_ids,
                i_w_ids,
                i_qtys,
            } => {
                let mut all_local = true;
                let mut items = vec![];
                for i in 0..i_ids.len() {
                    all_local = all_local && i_w_ids[i] == *w_id;
                    if let Some(item) = state.items.get(&i_ids[i]) {
                        items.push(item.clone());
                    } else {
                        // abort on non-existent item id
                        // happens on purpose with 1% of transactions
                        return None
                    }
                }
                let w_tax = state.warehouses.get(w_id).unwrap().w_tax;
                let district = state.wh_mut(w_id).districts.get_mut(d_id).unwrap();
                let d_tax = district.d_tax;
                let d_next_o_id = district.d_next_o_id;
                district.d_next_o_id += 1;
                let customer = state.wh_mut(w_id).customers.get(&(*d_id, *c_id)).unwrap();
                let c_discount = customer.c_discount;

                let o_ol_cnt = i_ids.len();
                let o_carrier_id = 0;
                
                let order = Order {
                    o_id: d_next_o_id,
                    o_c_id: *c_id,
                    o_d_id: *d_id,
                    o_w_id: *w_id,
                    o_entry_d: o_entry_d.to_owned(),
                    o_carrier_id,
                    o_ol_cnt,
                    o_all_local: all_local as usize,
                };
                state.wh_mut(w_id).orders.insert((*d_id, d_next_o_id), order);
                state.wh_mut(w_id).new_orders.insert(
                    (*d_id, d_next_o_id),
                    NewOrder { no_o_id: d_next_o_id, no_d_id: *d_id, no_w_id: *w_id }
                );
                state.wh_mut(w_id).new_order_index.get_mut(&(*d_id)).unwrap().push_back(d_next_o_id);

                let mut total = 0.0;
                for i in 0..i_ids.len() {
                    let ol_number = i + 1;
                    let ol_supply_w_id = i_w_ids[i];
                    let ol_i_id = i_ids[i];
                    let ol_quantity = i_qtys[i];
                    let i_info = &items[i];

                    let stock_info = if let Some(s) = state.stock.get_mut(&(ol_supply_w_id, ol_i_id)) {
                        s
                    } else {
                        // no stock record found
                        continue
                    };
                    let s_dist_xx = match *d_id {
                        1 => stock_info.s_dist_01.to_owned(),
                        2 => stock_info.s_dist_02.to_owned(),
                        3 => stock_info.s_dist_03.to_owned(),
                        4 => stock_info.s_dist_04.to_owned(),
                        5 => stock_info.s_dist_05.to_owned(),
                        6 => stock_info.s_dist_06.to_owned(),
                        7 => stock_info.s_dist_07.to_owned(),
                        8 => stock_info.s_dist_08.to_owned(),
                        9 => stock_info.s_dist_09.to_owned(),
                        10 => stock_info.s_dist_10.to_owned(),
                        _ => panic!("bad d_id"),
                    };

                    stock_info.s_ytd += ol_quantity;
                    if stock_info.s_quantity >= ol_quantity + 10 {
                        stock_info.s_quantity -= ol_quantity;
                    } else {
                        stock_info.s_quantity = stock_info.s_quantity + 91 - ol_quantity;
                    }
                    stock_info.s_order_cnt += 1;
                    if ol_supply_w_id != *w_id {
                        stock_info.s_remote_cnt += 1;
                    }

                    let brand_generic = if i_info.i_data.contains("ORIGINAL") && stock_info.s_data.contains("ORIGINAL") {
                        "B".to_string()
                    } else {
                        "G".to_string()
                    };

                    let ol_amount = ol_quantity as f64 * i_info.i_price;
                    total += ol_amount;

                    state.wh_mut(w_id).order_lines.insert(
                        (*d_id, d_next_o_id, ol_number),
                        OrderLine { ol_o_id: d_next_o_id, ol_d_id: *d_id, ol_w_id: *w_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d: o_entry_d.to_owned(), ol_quantity, ol_amount, ol_dist_info: s_dist_xx },
                    );
                }
                // TODO: return item_data and total
                total *= (1.0 - c_discount) * (1.0 + w_tax + d_tax);
                None
            },
            Self::OrderStatus{
                w_id,
                d_id,
                c_id,
                c_last,
            } => {
                let customer = if let Some(c_id) = c_id {
                    state.wh(w_id).customers.get(&(*d_id, *c_id)).unwrap()
                } else {
                    state.wh(w_id).get_customer_by_name(c_last)
                };
                let order = state.wh(w_id).orders.values()
                    .filter(|o| o.o_d_id == *d_id && o.o_c_id == customer.c_id)
                    .max_by_key(|o| o.o_id);
                let order_lines = if let Some(order) = order {
                    state.wh(w_id).order_lines.values()
                        .filter(|ol| ol.ol_d_id == *d_id && ol.ol_o_id == order.o_id)
                        .collect()
                } else {
                    vec![]
                };
                // TODO: return actual data
                None
            },
            Self::Payment{
                w_id,
                d_id,
                h_amount,
                c_w_id,
                c_d_id,
                c_id,
                c_last,
                h_date,
            } => {
                let customer = if let Some(c_id) = c_id {
                    state.wh_mut(w_id).customers.get_mut(&(*d_id, *c_id)).unwrap()
                } else {
                    state.wh_mut(w_id).get_customer_by_name_mut(c_last)
                };
                customer.c_balance -= h_amount;
                customer.c_ytd_payment += h_amount;
                customer.c_payment_cnt += 1;
                if customer.c_credit == "BC" {
                    // TODO: use a convergent string type for c_data
                    customer.c_data = format!("{} {} {} {} {} {}|{}", customer.c_id, c_d_id, c_w_id, d_id, w_id, h_amount, customer.c_data);
                    if customer.c_data.len() > 500 {
                        customer.c_data = customer.c_data[0..500].to_owned();
                    }
                }
                let h_c_id = customer.c_id;
                let h_c_w_id = customer.c_w_id;
                let h_c_d_id = customer.c_d_id;

                let warehouse = state.warehouses.get_mut(w_id).unwrap();
                warehouse.w_ytd += *h_amount;
                let w_name = warehouse.w_name.clone();
                let district = state.wh_mut(w_id).districts.get_mut(d_id).unwrap();
                district.d_ytd += *h_amount;
                let d_name = district.d_name.clone();

                state.wh_mut(w_id).history.push(History {
                    h_c_id,
                    h_c_w_id,
                    h_c_d_id,
                    h_w_id: *w_id,
                    h_d_id: *d_id,
                    h_amount: *h_amount,
                    h_date: h_date.to_owned(),
                    h_data: format!("{}    {}", w_name, d_name),
                });

                None
            },
            Self::StockLevel{
                w_id,
                d_id,
                threshold,
            } => {
                let district = state.wh(w_id).districts.get(d_id).unwrap();
                let o_id = district.d_next_o_id;
                let item_ids = state.wh(w_id).order_lines.values().filter_map(|ol| {
                    if ol.ol_d_id == *d_id && ol.ol_o_id < o_id && ol.ol_o_id >= o_id - 20 {
                        Some(ol.ol_i_id)
                    } else {
                        None
                    }
                }).collect::<Vec<_>>();
                let result = state.stock.values().filter(|s| {
                    s.s_w_id == *w_id && s.s_quantity < *threshold && item_ids.contains(&s.s_i_id)
                }).map(|s| s.s_i_id).collect::<HashSet<_>>().len();
                // TODO: return result
                None
            },
        }
    }

    fn gen_query(_settings: &crate::api::http::BenchSettings) -> Self {
        unimplemented!("please use the py-tpcc driver implementation")
    }

    fn generate_shadow(&self, state: &Self::State) -> Option<Self> {
        todo!()
    }
}
