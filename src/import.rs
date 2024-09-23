use crate::trade;

use super::trade::{TransferType, TransferStatus, Trade, StaticStr, TRADES};

const STATUSS: [(&'static str, TransferStatus); 5] = [("Approving", TransferStatus::Approving), ("WaitBroadcast", TransferStatus::WaitBroadcast), ("Pending", TransferStatus::Pending), ("Succeeded", TransferStatus::Succeeded), ("Failed", TransferStatus::Failed)];
const TYPES: [(&'static str, TransferType); 6] = [("NodeFund", TransferType::NodeFund), ("Fund", TransferType::Fund), ("Withdraw", TransferType::Withdraw), ("NodeWithdraw", TransferType::NodeWithdraw), ("Pay", TransferType::Pay), ("Gas", TransferType::Gas)];

pub fn get_status(key: &str)-> Option<TransferStatus> {
    STATUSS.iter().find(|s| s.0 == key ).map(|s| s.1.clone() )
}

pub fn get_type(key: &str)-> Option<TransferType> {
    TYPES.iter().find(|s| s.0 == key ).map(|s| s.1.clone() )
}

use anyhow::{anyhow, Result};
use chrono::NaiveDateTime;
use std::borrow::Cow;

pub fn import_trade(asset: u32, trade_id: StaticStr, trade: Trade)-> bool {
    if !TRADES[asset as usize].store.contains(&trade_id) {
        TRADES[asset as usize].store.insert(&trade_id, &trade);
        true
    } else { false }
}

pub fn load_air_drop(row: mysql::Row)-> Result<bool> {
    let id = row.get::<u64, &str>("id").ok_or(anyhow!("no id"))?;
    let address = Cow::from(row.get::<String, &str>("address").ok_or(anyhow!("no address"))?);
    let number = row.get::<u64, &str>("had_drop_number").ok_or(anyhow!("no had_drop_number"))?;
    let trade = Trade::airdrop(address.clone(), number);
    let _= import_trade(trade::ASSET_JERRY, Cow::from(format!("air_drop_jerry-{}", id)), trade);
    let gas = row.get::<u64, &str>("had_drop_gas_number").ok_or(anyhow!("no had_drop_gas_number"))?;
    let trade = Trade::airdrop(address, gas);
    Ok(import_trade(trade::ASSET_RNA, Cow::from(format!("air_drop_rna-{}", id)), trade))
}

pub fn load_mysql_row(row: mysql::Row)-> Result<bool> {
    let tid = row.get::<String, &str>("transfer_id").ok_or(anyhow!("no transfer_id"))?;
    let asset = row.get::<String, &str>("transfer_asset_id").ok_or(anyhow!("no asset_id")).and_then(|asset_name| super::get_asset_id(&asset_name) )?;
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
                if import_trade(asset as u32, Cow::from(tid.clone()), trade) {
                    return Ok(true);
                }
            }
            TransferType::Pay=> {
                let from = row.get::<String, &str>("from_address").ok_or(anyhow!("no from_address"))?;
                let to = row.get::<String, &str>("to_address").ok_or(anyhow!("no to_address"))?;
                let mut trade = Trade::pay(Cow::from(from), Cow::from(to), amount, Vec::new(), Cow::from(hash));
                trade.update_tick = updated;
                trade.create_tick = created;
                trade.status = status;
                if import_trade(asset as u32, Cow::from(tid.clone()), trade) {
                    return Ok(true);
                }
            }
            TransferType::Gas=> {
                let from = row.get::<String, &str>("from_address").ok_or(anyhow!("no from_address"))?;
                let to = row.get::<String, &str>("to_address").ok_or(anyhow!("no to_address"))?;
                let mut trade = Trade::pay(Cow::from(from), Cow::from(to), amount, Vec::new(), Cow::from(hash));
                trade.update_tick = updated;
                trade.create_tick = created;
                trade.status = status;
                if import_trade(asset as u32, Cow::from(tid.clone()), trade) {
                    return Ok(true);
                }
            }
            TransferType::Withdraw=> {
                let from = row.get::<String, &str>("from_address").ok_or(anyhow!("no from_address"))?;
                let to = row.get::<String, &str>("to_address").ok_or(anyhow!("no to_address"))?;
                let mut trade = Trade::withdraw(Cow::from(from), Cow::from(to), amount, Vec::new(), Cow::from(hash));
                trade.update_tick = updated;
                trade.create_tick = created;
                trade.status = status;
                if import_trade(asset as u32, Cow::from(tid.clone()), trade) {
                    return Ok(true);
                }
            }
            _=> {
                panic!("ohh---{:?}", row);
            }    
        }
    }
    Ok(false)
}

pub fn clean_up() {         //清除所有 key 谨慎使用
    TRADES.iter().for_each(|t| t.store.clean_up() );   
}