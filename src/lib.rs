pub mod trade;
pub mod import;
use trade::{StaticStr, Trade, GasInfo};
use scc::{HashMap, HashSet};

#[derive(Clone, Debug)]
pub struct Account {
    amounts: [(u64, u64); ASSET_NUM],
    trades: Vec<(u32, StaticStr)>
}

impl Default for Account {
    fn default()-> Self {
        Self{amounts: [(0,0), (0,0), (0,0), (0,0), (0,0), (0,0), (0,0), (0,0)], trades: Vec::new()}
    }
}

impl Account {
    pub fn lock(&mut self, asset: usize, trade: &Trade)-> bool {    //锁定资金 开始提现或者转出
        if trade.gas.iter().find(|g| 
            if self.amounts[asset].0 < g.amount + if g.asset == asset as u32 { trade.amount } else { 0 }  { true }
            else { false }
        ).is_some() { return false }            //存在不够的 gas
        if self.amounts[asset].0 >= trade.amount {
            self.amounts[asset].0 -= trade.amount;
            self.amounts[asset].1 += trade.amount;
            for g in &trade.gas {
                self.amounts[g.asset as usize].0 -= g.amount;
                self.amounts[g.asset as usize].1 += g.amount;
            }
            true
        } else { false }
    }

    pub fn confirm(&mut self, asset: usize, trade: &Trade)-> bool {        //确认转出 或者确认提现
        self.amounts[asset].1 -= trade.amount;
        for g in &trade.gas {
            self.amounts[g.asset as usize].1 -= g.amount;
        }
        true
    }
    pub fn rollback(&mut self, asset: usize, trade: &Trade)-> bool {       //用于转账失败或者 提现失败的回滚
        self.amounts[asset].1 -= trade.amount;
        self.amounts[asset].0 += trade.amount;
        for g in &trade.gas {
            self.amounts[g.asset as usize].1 -= g.amount;
            self.amounts[g.asset as usize].0 += g.amount;
        }
        true
    }

    pub fn income(&mut self, asset: usize, amount: u64)-> bool {        //仅用于充值到账 以及转账接收方到账
        self.amounts[asset].0 += amount;
        true
    }
    pub fn decrease(&mut self, asset: usize, trade: &Trade)-> bool {      //减少 asset 仅用于重新加载的时候 没有锁定直接减少
        if self.amounts[asset].0 < trade.amount {
            return false;
        }
        self.amounts[asset].0 -= trade.amount;
        for g in &trade.gas {
            self.amounts[g.asset as usize].0 -= g.amount;
        }
       true
    }
}

use once_cell::sync::Lazy;
use std::sync::Arc;
static ACCOUNTS: Lazy<Arc<HashMap<StaticStr, Account>>> = Lazy::new(|| Arc::new(HashMap::default()) );
pub static WARNINGS: Lazy<Arc<HashSet<(u32, StaticStr)>>> = Lazy::new(|| Arc::new(HashSet::default()) );

async fn account_modify<F: FnOnce(&mut Account)-> bool>(account: &StaticStr, f: F)-> bool {
    ACCOUNTS.update_async(account, |_, account| f(account) ).await.unwrap_or(false)
}

async fn account_add(account: StaticStr, asset: u32, trade_id: StaticStr, amount: Option<u64>) {       //用于转账接收方或者充值方 如果账号不存在则创建一个
    ACCOUNTS.entry_async(account).await.and_modify(|account| {
        amount.map(|amount| account.amounts[asset as usize].0 += amount );
        account.trades.push((asset, trade_id.clone()));
    }).or_insert({
        let mut account = Account{amounts: [(0,0), (0,0), (0,0), (0,0), (0,0), (0,0), (0,0), (0,0)], trades: vec![(asset, trade_id)]};
        amount.map(|amount| account.amounts[asset as usize].0 += amount );
        account
    });
}

async fn account_start(asset: u32, trade_id: StaticStr, trade: &Trade)-> bool {       //创建一笔转账或者提现交易
    account_modify(&trade.from, |account| 
        if account.lock(asset as usize, &trade) {
            account.trades.push((asset, trade_id));
            true
        } else { false }
    ).await
}

async fn account_success(asset: u32, trade: &Trade, with_lock: bool)-> bool {            //成功完成一笔交易
    if account_modify(&trade.from, |account| {
        if with_lock {
            account.confirm(asset as usize, &trade)
        } else {
            if !account.decrease(asset as usize, &trade) {
                log::error!("debit {} {:?}", ASSET_NAMES[asset as usize], trade);
                let _ = WARNINGS.insert((asset, trade.from.clone()));
            }
            true
        }
    }).await {
        for g in &trade.gas {
            account_modify(&g.to, |account| account.income(g.asset as usize, g.amount) ).await;
        }    
        account_modify(&trade.to, |account| account.income(asset as usize, trade.amount) ).await
    } else { false }           
}

use anyhow::{Result, anyhow};
use trade::{ASSET_NAMES, ASSET_NUM, TRADES, TransferType, TransferStatus};

pub fn get_asset_id(asset_name: &str)-> Result<usize> {
    ASSET_NAMES.iter().position(|a| *a == asset_name ).ok_or(anyhow!("unknow asset {}", asset_name) )
}

pub async fn get_amount(account: &StaticStr)-> Option<[(u64, u64); ASSET_NUM]>{
    ACCOUNTS.get_async(account).await.map(|account| account.amounts )
}

pub async fn get_trades(asset: u32, account: &StaticStr)-> Vec<(StaticStr, Trade)>{
    let ids = ACCOUNTS.get(account).map(|account| {
        account.trades.iter().filter_map(|t| if t.0 == asset { Some(t.1.clone()) } else { None }).collect()
    }).unwrap_or(Vec::new());
    let mut trades = Vec::new();
    for id in ids {
        TRADES[asset as usize].trade(&id).await.map(|t| trades.push((id.clone(), t)) );
    }
    trades
}

pub async fn add_fund(asset: u32, trade_id: StaticStr, from: StaticStr, to: StaticStr, amount: u64, hash: StaticStr)-> Result<()> {
    if TRADES[asset as usize].contains(&trade_id).await { return Err(anyhow!("trade {} existed", trade_id )); }
    let trade = Trade::fund(from, to.clone(), amount, hash);
    let _ = TRADES[asset as usize].insert(trade_id.clone(), trade.clone()).await;
    account_add(to, asset, trade_id, None).await;
    Ok(())
}

pub async fn complete_fund(asset: u32, trade_id: StaticStr, success: bool)-> bool {
    if let Some(old) = TRADES[asset as usize].update(trade_id, |mut trade| if trade.success() { Some(trade) } else { None } ).await {      //update success
        if success { account_modify(&old.to, |account| account.income(asset as usize, old.amount) ).await }
        else { true }
    } else { false}
}


pub async fn add_pay(asset: u32, trade_id: StaticStr, from: StaticStr, to: StaticStr, amount: u64, gas: Vec<GasInfo>, hash: StaticStr)-> Result<()> {
    if TRADES[asset as usize].contains(&trade_id).await { return Err(anyhow!("trade {} existed", trade_id )); }
    let trade = Trade::pay(from, to, amount, gas, hash);
    if account_start(asset, trade_id.clone(), &trade).await {
        account_add(trade.to.clone(), asset, trade_id.clone(), None).await;
        let _ = TRADES[asset as usize].insert(trade_id, trade);
        Ok(())
    } else { Err(anyhow!("{} have no enough amount", trade.from)) }
}

pub async fn complete_pay(asset: u32, trade_id: StaticStr, success: bool)-> bool {
    if let Some(old) = TRADES[asset as usize].update(trade_id.clone(), |mut trade| if trade.modify(success) { Some(trade) } else { None } ).await {
        if success {
            account_success(asset, &old, true).await
        } else {
            account_modify(&old.from, |account| {
                account.rollback(asset as usize, &old)
            }).await
        }
    } else { false }
}

pub async fn add_withdraw(asset: u32, trade_id: StaticStr, from: StaticStr, to: StaticStr, amount: u64, gas: Vec<GasInfo>, hash: StaticStr)-> Result<()> {
    if TRADES[asset as usize].contains(&trade_id).await { return Err(anyhow!("trade {} existed", trade_id )); }
    let trade = Trade::withdraw(from, to, amount, gas, hash);
    if account_start(asset, trade_id.clone(), &trade).await {
        let _ = TRADES[asset as usize].insert(trade_id, trade).await;
        Ok(())        
    } else { Err(anyhow!("{} have no enough amount", trade.from)) }
}

pub async fn complete_withdraw(asset: u32, trade_id: StaticStr, success: bool)-> bool {
    if let Some(old) = TRADES[asset as usize].update(trade_id.clone(), |mut trade| {
        if trade.modify(success) { Some(trade) } else { None } 
    }).await {
        if success {
            account_success(asset, &old, true).await
        } else {
            account_modify(&old.from, |account| {
                account.rollback(asset as usize, &old) 
            }).await
        }
    } else { false }
}

pub(crate) async fn add_trade(asset: u32, trade_id: StaticStr, trade: Trade) {           //加载初始化的数据, 
    match trade.r#type {
        TransferType::Fund=> {
            account_add(trade.to.clone(), asset, trade_id.clone(), None).await;
            if trade.status == TransferStatus::Succeeded {
                account_modify(&trade.to, |account| account.income(asset as usize, trade.amount) ).await;
            }
        }
        TransferType::Pay=> {
            account_add(trade.from.clone(), asset, trade_id.clone(), None).await;
            account_add(trade.to.clone(), asset, trade_id.clone(), None).await;
            if trade.status == TransferStatus::Succeeded {
                account_success(asset, &trade, false).await;
            } else if trade.status != TransferStatus::Failed {
                account_start(asset, trade_id, &trade).await;
            }
        }
        TransferType::Withdraw=> {
            account_add(trade.from.clone(), asset, trade_id.clone(), None).await;
            account_add(trade.to.clone(), asset, trade_id.clone(), None).await;
            if trade.status == TransferStatus::Succeeded {
                account_success(asset, &trade, false).await;
            } else if trade.status != TransferStatus::Failed {
                account_start(asset, trade_id, &trade).await;
            }
        }
        TransferType::AirDrop=> {
            account_add(trade.to, asset, trade_id.clone(), Some(trade.amount)).await;
        }
        _=> {}
    }
}

pub fn load_all()-> std::time::Duration {
    let start = std::time::Instant::now();
    let mut tasks = Vec::new();
    for asset in 0..trade::ASSET_NUM {
        tasks.push(std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            TRADES[asset].store.load_all(move |id, trade: Trade| {
                //let id = id.clone();
                //let trade = trade.clone();
                rt.block_on(async move {            //同一个 asset 的插入顺序需要保证 所以创建一个 runtime
                    TRADES[asset].add_trade(id.clone(), trade.clone()).await;
                    add_trade(asset as u32, id, trade).await;
                });
            }).unwrap();
        }));
    }
    
    for t in tasks {
        let _ = t.join();
    }
    std::time::Instant::now().duration_since(start)
}