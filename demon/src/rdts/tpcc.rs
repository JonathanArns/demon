use std::collections::HashMap;

use anyhow::anyhow;
use serde::{Deserialize, Serialize};

use super::Operation;

#[derive(Clone, Debug, Default)]
pub struct DB {
    /// w_id -> Warehouse
    warehouses: HashMap<u16, Warehouse>,
    /// (d_w_id, d_id) -> District
    districts: HashMap<(u16, u8), District>,
    /// i_id -> Item
    items: HashMap<usize, Item>,
    /// (c_w_id, c_d_id, c_id) -> Customer
    customers: HashMap<(u16, u8, usize), Customer>,
    /// unique_id -> History
    history: HashMap<usize, History>,
    /// (s_w_id, s_i_id) -> Stock
    stock: HashMap<(u16, usize), Stock>,
    /// (o_w_id, o_d_id, o_id) -> Order
    orders: HashMap<(u16, u8, usize), Order>,
    /// (no_w_id, no_d_id, no_id) -> NewOrder
    new_orders: HashMap<(u16, u8, usize), NewOrder>,
    /// (ol_w_id,ol_d_id,ol_o_id,ol_number) -> OrderLine
    order_lines: HashMap<(u16, u8, usize, usize), OrderLine>,
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
        c_id: usize,
        c_last: String,
    },
    Payment{
        w_id: u16,
        d_id: u8,
        h_amount: f64,
        c_w_id: u16,
        c_d_id: u8,
        c_id: usize,
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
            Self::LoadTuples {..} => false,
            Self::Delivery {..} => todo!(),
            Self::NewOrder {..} => todo!(),
            Self::OrderStatus {..} => todo!(),
            Self::Payment {..} => todo!(),
            Self::StockLevel {..} => todo!(),
        }
    }

    fn is_semiserializable_strong(&self) -> bool {
        match *self {
            Self::LoadTuples {..} => false,
            Self::Delivery {..} => todo!(),
            Self::NewOrder {..} => todo!(),
            Self::OrderStatus {..} => todo!(),
            Self::Payment {..} => todo!(),
            Self::StockLevel {..} => todo!(),
        }
    }

    fn is_writing(&self) -> bool {
        match *self {
            Self::LoadTuples {..} => true,
            Self::Delivery {..} => todo!(),
            Self::NewOrder {..} => todo!(),
            Self::OrderStatus {..} => todo!(),
            Self::Payment {..} => todo!(),
            Self::StockLevel {..} => todo!(),
        }
    }

    fn is_por_conflicting(&self, other: &Self) -> bool {
        match *self {
            Self::LoadTuples {..} => false,
            Self::Delivery {..} => todo!(),
            Self::NewOrder {..} => todo!(),
            Self::OrderStatus {..} => todo!(),
            Self::Payment {..} => todo!(),
            Self::StockLevel {..} => todo!(),
        }
    }

    fn is_conflicting(&self, other: &Self) -> bool {
        match *self {
            Self::LoadTuples {..} => false,
            Self::Delivery {..} => todo!(),
            Self::NewOrder {..} => todo!(),
            Self::OrderStatus {..} => todo!(),
            Self::Payment {..} => todo!(),
            Self::StockLevel {..} => todo!(),
        }
    }

    fn rollback_conflicting_state(&self, source: &Self::State, target: &mut Self::State) {
        match *self {
            Self::LoadTuples {..} => (),
            Self::Delivery {..} => todo!(),
            Self::NewOrder {..} => todo!(),
            Self::OrderStatus {..} => todo!(),
            Self::Payment {..} => todo!(),
            Self::StockLevel {..} => todo!(),
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
                    c_id: parts[3].parse()?,
                    c_last: parts[3].parse()?,
                })
            },
            "payment" => {
                Ok(Self::Payment{
                    w_id: parts[1].parse()?,
                    d_id: parts[2].parse()?,
                    h_amount: parts[3].parse()?,
                    c_w_id: parts[4].parse()?,
                    c_d_id: parts[5].parse()?,
                    c_id: parts[6].parse()?,
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
                            state.districts.insert((d_w_id, d_id), val);
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
                            let val = Customer{c_id, c_d_id, c_w_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data};
                            state.customers.insert((c_w_id, c_d_id, c_id), val);
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
                            state.history.insert(state.history.len(), val);
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
                            state.orders.insert((o_w_id, o_d_id, o_id), val);
                        }
                    },
                    "NEW_ORDER" => {
                        for tuple in tuples {
                            let mut values = tuple.split(",").into_iter();
                            let no_o_id: usize = values.next().unwrap().parse().unwrap();
                            let no_d_id: u8 = values.next().unwrap().parse().unwrap();
                            let no_w_id: u16 = values.next().unwrap().parse().unwrap();
                            let val = NewOrder{no_o_id, no_d_id, no_w_id};
                            state.new_orders.insert((no_w_id, no_d_id, no_o_id), val);
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
                            state.order_lines.insert((ol_w_id, ol_d_id, ol_o_id, ol_number), val);
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
                todo!()
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
                todo!()
            },
            Self::OrderStatus{
                w_id,
                d_id,
                c_id,
                c_last,
            } => {
                todo!()
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
                todo!()
            },
            Self::StockLevel{
                w_id,
                d_id,
                threshold,
            } => {
                todo!()
            },
        }
    }
}
