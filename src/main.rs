use core::panic;
use std::{borrow::Cow, io::Read};
mod trade;
use once_cell::sync::Lazy;

use mysql::*;
use mysql::prelude::*;
use serde::{Deserialize, Serialize};
use trade::{add_fund, TransferStatus, TransferType};
use chrono::NaiveDateTime;
use chrono::{DateTime, Utc, TimeZone};

use trade::{Trade, get_amount, get_trades, TRADES};
use anyhow::{anyhow, Result};
use trade::TradeStore;
const STATUSS: [(&'static str, TransferStatus); 5] = [("Approving", TransferStatus::Approving), ("WaitBroadcast", TransferStatus::WaitBroadcast), ("Pending", TransferStatus::Pending), ("Succeeded", TransferStatus::Succeeded), ("Failed", TransferStatus::Failed)];
const TYPES: [(&'static str, TransferType); 6] = [("NodeFund", TransferType::NodeFund), ("Fund", TransferType::Fund), ("Withdraw", TransferType::Withdraw), ("NodeWithdraw", TransferType::NodeWithdraw), ("Pay", TransferType::Pay), ("Gas", TransferType::Gas)];

pub fn get_status(key: &str)-> Option<TransferStatus> {
    STATUSS.iter().find(|s| s.0 == key ).map(|s| s.1.clone() )
}

pub fn get_type(key: &str)-> Option<TransferType> {
    TYPES.iter().find(|s| s.0 == key ).map(|s| s.1.clone() )
}

fn main()-> Result<()> {
    fern::Dispatch::new().format(|out, message, record| {
        out.finish(format_args!(
            "{}[{}][{}] {} line: {}",
            chrono::Local::now().format("[%Y-%m-%d][%H:%M:%S]"),
            record.target(),
            record.level(),
            message,
            record.line().unwrap_or_default()
        ))
    }).level(log::LevelFilter::Info).chain(fern::log_file("account.log")?).apply()?;

    let start = std::time::Instant::now();
    let mut threads = Vec::new();
    for asset in 0..trade::ASSET_NUM {
        threads.push(std::thread::spawn(move || {
            TRADES[asset].store.load_all(|id, trade: Trade| {
                trade::add_trade(asset as u32, id.clone(), trade.clone());
                TRADES[asset].add_trade(id, trade);
            }).unwrap();         
        }));
    }
    
    for t in threads {
        println!("{:?}", t.join());
    }
    
    println!("{:?}", std::time::Instant::now().duration_since(start));

    return Ok(());
    trade::WARNINGS.scan(|k| {
        if k.0 != 2 {
            println!("Asset - {} Account - {}", k.0, k.1);
            for trade in trade::get_trades(k.0, &k.1) {
                println!("{} : {} - {} - {}", trade.0, trade.1.from, trade.1.to, trade.1.amount );
            }
        }
    });

    let url = "mysql://marketplace-readonly:Ag3e5eyERjOWuEkhjlG1@127.0.0.1:9001/wallet-online-db";
    let pool = Pool::new(url)?;
    let mut conn = pool.get_conn()?;
    //let rows: Vec<Row> = conn.query("SELECT * from t_ln_transfer WHERE created_at >= \"2024-09-15 00:00:00\" and created_at < \"2024-09-23 00:00:00\" ")?;
    //let rows: Vec<Row> = conn.query("SELECT * from t_ln_transfer WHERE created_at < \"2024-08-20 00:00:00\" ")?;
    //let rows: Vec<Row> = conn.query("SELECT * from t_ln_transfer WHERE created_at >= \"2024-08-20 00:00:00\" AND created_at < \"2024-08-25 00:00:00\" ")?;
    //let rows: Vec<Row> = conn.query("SELECT * from t_ln_transfer WHERE created_at >= \"2024-08-25 00:00:00\" AND created_at < \"2024-08-30 00:00:00\" ")?;
    //let rows: Vec<Row> = conn.query("SELECT * from t_ln_transfer WHERE created_at >= \"2024-08-30 00:00:00\" AND created_at < \"2024-09-10 00:00:00\" ")?;
    let rows: Vec<Row> = conn.query("SELECT * from t_ln_transfer WHERE created_at >= \"2024-09-10 00:00:00\" AND created_at < \"2024-09-25 00:00:00\" ")?;
    println!("total {}", rows.len());
    let mut count = 0;
    for row in rows {
        let tid = row.get::<String, &str>("transfer_id").ok_or(anyhow!("no transfer_id"))?;
        let asset = row.get::<String, &str>("transfer_asset_id").ok_or(anyhow!("no asset_id")).and_then(|asset_name| trade::get_asset_id(&asset_name) )?;
        let created = row.get::<String, &str>("created_at").and_then(|dt| NaiveDateTime::parse_from_str(&dt, "%Y-%m-%d %H:%M:%S").ok() ).map(|dt| dt.and_utc().timestamp() ).unwrap_or(0);
        let updated = row.get::<String, &str>("updated_at").and_then(|dt| NaiveDateTime::parse_from_str(&dt, "%Y-%m-%d %H:%M:%S").ok() ).map(|dt| dt.and_utc().timestamp() ).unwrap_or(0);
        let status = row.get::<String, &str>("transfer_status").and_then(|t| get_status(&t) ).ok_or(anyhow!("unknow status"))?;
        let amount = row.get::<u64, &str>("transfer_amount").ok_or(anyhow!("no transfer_amount"))?;
        let hash = row.get::<Option<String>, &str>("transfer_hash").unwrap_or(Some(String::new())).unwrap_or(String::new());
        if let Some(transfer_type) = row.get::<String, &str>("transfer_type").and_then(|t| get_type(&t) ) {
            match transfer_type {
                TransferType::Fund=> {
                    let from = row.get::<String, &str>("from_address").ok_or(anyhow!("no from_address"))?;      //这个是存入的地址 我的天啊!@!!@!!
                    let to = row.get::<String, &str>("to_address").ok_or(anyhow!("no to_address"))?;            //这个没有使用
                    let mut trade = Trade::fund(Cow::from(to), Cow::from(from), amount, Cow::from(hash));
                    trade.update_tick = updated;
                    trade.create_tick = created;
                    trade.status = status;
                    if trade::import_trade(asset as u32, Cow::from(tid.clone()), trade) {
                        println!("{} fund {} - {}", count, asset, tid);
                        count += 1;
                    }
                }
                TransferType::Pay=> {
                    let from = row.get::<String, &str>("from_address").ok_or(anyhow!("no from_address"))?;
                    let to = row.get::<String, &str>("to_address").ok_or(anyhow!("no to_address"))?;
                    let mut trade = Trade::pay(Cow::from(from), Cow::from(to), amount, Vec::new(), Cow::from(hash));
                    trade.update_tick = updated;
                    trade.create_tick = created;
                    trade.status = status;
                    if trade::import_trade(asset as u32, Cow::from(tid.clone()), trade) {
                        println!("{} pay {} - {}", count, asset, tid);
                        count += 1;
                    }
                }
                TransferType::Gas=> {
                    let from = row.get::<String, &str>("from_address").ok_or(anyhow!("no from_address"))?;
                    let to = row.get::<String, &str>("to_address").ok_or(anyhow!("no to_address"))?;
                    let mut trade = Trade::pay(Cow::from(from), Cow::from(to), amount, Vec::new(), Cow::from(hash));
                    trade.update_tick = updated;
                    trade.create_tick = created;
                    trade.status = status;
                    if trade::import_trade(asset as u32, Cow::from(tid.clone()), trade) {
                        println!("{} gas {} - {}", count, asset, tid);
                        count += 1;
                    }
                }
                TransferType::Withdraw=> {
                    let from = row.get::<String, &str>("from_address").ok_or(anyhow!("no from_address"))?;
                    let to = row.get::<String, &str>("to_address").ok_or(anyhow!("no to_address"))?;
                    let mut trade = Trade::withdraw(Cow::from(from), Cow::from(to), amount, Vec::new(), Cow::from(hash));
                    trade.update_tick = updated;
                    trade.create_tick = created;
                    trade.status = status;
                    if trade::import_trade(asset as u32, Cow::from(tid.clone()), trade) {
                        println!("{} gas {} - {}", count, asset, tid);
                        count += 1;
                    }
                }
                _=> {
                    panic!("ohh---{:?}", row);
                }    
            }
        }
    }
    Ok(())
}
