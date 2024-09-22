use std::{borrow::Cow, io::Read};
mod trade;
use once_cell::sync::Lazy;

use mysql::*;
use mysql::prelude::*;
use serde::{Deserialize, Serialize};
use trade::{TransferStatus, TransferType};
use chrono::NaiveDateTime;
use chrono::{DateTime, Utc, TimeZone};
pub fn mysql_to_Utc(dt: &str)-> Option<DateTime<Utc>> {
    DateTime::parse_from_str(dt, "%Y-%m-%d %H:%M:%S").ok().map(|dt| dt.to_utc() )
}

static ADDRS: Lazy<Vec<Cow<'static, str>>> = Lazy::new(|| {
    let mut fs = std::fs::File::open("addr.txt").unwrap();
    let mut lines = String::new();
    fs.read_to_string(&mut lines).unwrap();
    lines.split('\n').map(|s| Cow::from(s.trim().trim_start_matches("\"").trim_end_matches("\"").to_string()) ).collect()
});

fn display() {
    let mut total = vec![(0, 0), (0, 0), (0, 0), (0, 0), (0, 0), (0, 0), (0, 0), (0, 0)];
    for i in 0..ADDRS.len() {
        get_trades(&ADDRS[i]);
        if let Some(amount) = get_amount(&ADDRS[i]) {
            for asset in 0.. trade::ASSET_NUM {
                total[asset].0 += amount[asset].0;
                total[asset].1 += amount[asset].1;
            }
        }
    }
    println!("total {:?}", total);
}

use trade::{Trade, get_amount, get_trades, TRADES};
use anyhow::Result;
use trade::TradeStore;
const STATUSS: [(&'static str, TransferStatus); 5] = [("Approving", TransferStatus::Approving), ("WaitBroadcast", TransferStatus::WaitBroadcast), ("Pending", TransferStatus::Pending), ("Succeeded", TransferStatus::Succeeded), ("Failed", TransferStatus::Failed)];
const TYPES: [(&'static str, TransferType); 6] = [("NodeFund", TransferType::NodeFund), ("Fund", TransferType::Fund), ("Withdraw", TransferType::Withdraw), ("NodeWithdraw", TransferType::NodeWithdraw), ("Pay", TransferType::Pay), ("Gas", TransferType::Gas)];

pub fn get_status(key: String)-> Option<TransferStatus> {
    STATUSS.iter().find(|s| s.0 == key ).map(|s| s.1.clone() )
}

pub fn get_type(key: String)-> Option<TransferType> {
    TYPES.iter().find(|s| s.0 == key ).map(|s| s.1.clone() )
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TransferLN {
    pub id: Option<u64>,
    pub transfer_id: Option<String>,
    pub from_address: Option<String>,
    pub withdraw_address: Option<String>,
    pub to_address: Option<String>,
    pub from_node_id: Option<String>,
    pub to_node_id: Option<String>,
    pub channel_id: Option<String>,
    pub transfer_asset_id: Option<String>,
    pub transfer_amount: Option<u64>,
    pub transfer_hash: Option<String>,
    pub gas_amount: Option<u64>,
    pub transfer_status: Option<TransferStatus>,
    pub transfer_type: Option<TransferType>,
    pub check_withdraw: Option<u8>,
    pub withdraw_txid: Option<String>,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
}

impl Default for TransferLN {
    fn default() -> Self {
        Self{id: None, transfer_id: None, from_address: None, withdraw_address: None, to_address: None, from_node_id: None, to_node_id: None, channel_id: None, transfer_asset_id: None,
            transfer_amount: None, transfer_hash: None, gas_amount: None, transfer_status: None, transfer_type: None, check_withdraw: None, withdraw_txid: None, created_at: None, updated_at: None}
    }
}

fn main()-> Result<()> {
    fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "{}[{}][{}] {} line: {}",
                chrono::Local::now().format("[%Y-%m-%d][%H:%M:%S]"),
                record.target(),
                record.level(),
                message,
                record.line().unwrap_or_default()
            ))
        }).level(log::LevelFilter::Info).chain(fern::log_file("account.log")?).apply()?;

    let url = "mysql://marketplace-readonly:Ag3e5eyERjOWuEkhjlG1@127.0.0.1:9001/wallet-online-db";
    let pool = Pool::new(url)?;
    let mut conn = pool.get_conn()?;
            // Let's create a table for payments.
    let trades = conn.query_map("SELECT transfer_id, transfer_type, transfer_status, created_at, updated_at from t_ln_transfer limit 10",
    |(transfer_id, transfer_type, transfer_status, created_at, updated_at)| {
            let mut tx = TransferLN::default();
            tx.transfer_id = transfer_id;
            tx.transfer_type = get_type(transfer_type);
            tx.transfer_status = get_status(transfer_status);
            tx.created_at = created_at;
            tx.updated_at = updated_at;
            tx
        }
/*             |(id, transfer_id, from_address, withdraw_address, to_address, from_node_id, to_node_id, channel_id, transfer_asset_id, transfer_amount,
                transfer_hash, gas_amount, transfer_status, transfer_type, check_withdraw, withdraw_txid, created_at, updated_at)| {
                    TransferLN{id, transfer_id, from_address, withdraw_address, to_address, from_node_id, to_node_id, channel_id, transfer_asset_id, transfer_amount,
                        transfer_hash, gas_amount, transfer_status, transfer_type, check_withdraw, withdraw_txid, created_at, updated_at}
            },*/
    )?;
    for t in trades {
        println!("{:?}", t);
    }

/*     for asset in 0..trade::ASSET_NUM {
        TRADES[asset].store.load_all(|id, trade: Trade| {
            trade::add_trade(asset as u32, id.clone(), trade.clone());
            TRADES[asset].add_trade(id, trade);
        }).unwrap();         
    }*/
    Ok(())
}
