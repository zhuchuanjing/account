use scc::HashMap;
use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

const TRADE_NONE: u8 = 0;
const TRADE_CHARGE: u8 = 1;
const TRADE_TRANSFER: u8 = 2;
const TRADE_WITHDRAW: u8 = 3;

#[derive(Clone, Debug)]
pub enum Status {
    Idle = 0,
    Processing,
    Fail,
    Success
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Trade {
    r#type: u8,
    status: u8,
    create_tick: i64,
    update_tick: i64,
    amount: u64,
    gas: u64,
    fee: u64,
    from: Cow<'static, str>,
    to: Cow<'static, str>,
}

impl Trade {
    pub fn charge(from: Cow<'static, str>, to: Cow<'static, str>, amount: u64, gas: u64, fee: u64)-> Self {
        Self{r#type: TRADE_CHARGE, status: Status::Processing as u8, create_tick: chrono::Utc::now().timestamp(), update_tick: 0, amount, gas, fee, from, to}
    }
    pub fn complete_charge(&mut self, success: bool)-> bool {
        if self.r#type == TRADE_CHARGE && self.status == Status::Processing as u8 { 
            if success { self.status = Status::Success as u8; }
            else { self.status = Status::Fail as u8; }
            self.update_tick = chrono::Utc::now().timestamp();
            true
        } else { false }
    }

    pub fn transfer(from: Cow<'static, str>, to: Cow<'static, str>, amount: u64, gas: u64, fee: u64)-> Self {
        Self{r#type: TRADE_TRANSFER, status: Status::Idle as u8, create_tick: chrono::Utc::now().timestamp(), update_tick: 0, amount, gas, fee, from, to}
    }
    pub fn start_transfer(&mut self)-> bool {
        if self.r#type == TRADE_TRANSFER && self.status == Status::Idle as u8 { 
            self.status = Status::Processing as u8;
            self.update_tick = chrono::Utc::now().timestamp();
            true
        } else { false }
    }
    pub fn complete_transfer(&mut self, success: bool)-> bool {
        if self.r#type == TRADE_TRANSFER && self.status == Status::Processing as u8 { 
            if success { self.status = Status::Success as u8; }
            else { self.status = Status::Fail as u8; }
            self.update_tick = chrono::Utc::now().timestamp();
            true
        } else { false }
    }

    pub fn with_draw(from: Cow<'static, str>, to: Cow<'static, str>, amount: u64, gas: u64, fee: u64)-> Self {
        Self{r#type: TRADE_WITHDRAW, status: Status::Processing as u8, create_tick: chrono::Utc::now().timestamp(), update_tick: 0, amount, gas, fee, from, to}
    }
    pub fn complete_withdraw(&mut self, success: bool)-> bool {
        if self.r#type == TRADE_WITHDRAW && self.status == Status::Processing as u8 { 
            if success { self.status = Status::Success as u8; }
            else { self.status = Status::Fail as u8; }
            self.update_tick = chrono::Utc::now().timestamp();
            true
        } else { false }
    }
}


pub static TRADES: Lazy<Arc<TradeManager>> = Lazy::new(|| Arc::new(TradeManager::new("sled")) );

pub struct TradeManager {
    trades: HashMap<Cow<'static, str>, Trade>,
    tree: sled::Tree,
}

impl TradeManager {
    pub fn new(path: &str)-> Self {
        let db = sled::open(path).unwrap();
        let tree = db.open_tree("TRADES").unwrap();
        Self{trades: HashMap::default(), tree}
    }
    pub fn trade(&self, id: &Cow<'static, str>)-> Option<Trade> {
        self.trades.get(id).map(|t| t.clone() )
    }

    pub fn contains(&self, trade_id: &Cow<'static, str>)-> bool {
        self.trades.contains(trade_id)
    }
    pub fn insert(&self, trade_id: Cow<'static, str>, trade: Trade)-> Result<()> {
        self.tree.insert(trade_id.as_bytes(), rmp_serde::to_vec(&trade).unwrap())?;
        let _ = self.trades.insert(trade_id, trade);
        Ok(())
    }
    pub fn update<F: FnMut(Trade)-> Option<Trade>>(&self, trade_id: Cow<'static, str>, mut f: F)-> Option<Trade> {
        if let Ok(Some(old)) = self.tree.update_and_fetch(trade_id.as_bytes(), |old| {
            old.and_then(|old| {
                let trade = rmp_serde::from_slice::<Trade>(old).unwrap();
                f(trade).and_then(|trade| rmp_serde::to_vec(&trade).ok() )
            })
        }) {
            rmp_serde::from_slice::<Trade>(&old).ok()  
        } else { None }
    }

    pub fn load(&self)-> Result<()> {
        let mut iter = self.tree.iter();
        while let Some(Ok(kv)) = iter.next() {
            let key = String::from_utf8(kv.0.to_vec())?;
            let trade: Trade = rmp_serde::from_slice(&kv.1.to_vec())?;
            add_trade(Cow::from(key), trade);
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct Account {
    amount: u64,
    locked: u64,
    trades: Vec<Cow<'static, str>>
}

impl Default for Account {
    fn default()-> Self {
        Self{amount: 0, locked: 0, trades: Vec::new()}
    }
}

use once_cell::sync::Lazy;
use std::sync::Arc;
static ACCOUNTS: Lazy<Arc<HashMap<Cow<'static, str>, Account>>> = Lazy::new(|| Arc::new(HashMap::default()) );

pub fn add_with_create(account: Cow<'static, str>, trade_id: Cow<'static, str>, amount: u64) {
    ACCOUNTS.entry(account).and_modify(|account| {
        account.trades.push(trade_id.clone());
        account.amount += amount;
    }).or_insert(Account{amount: amount, locked: 0, trades: vec![trade_id]});
}

pub fn get_amount(account: Cow<'static, str>)-> Option<(u64, u64)>{
    ACCOUNTS.get(&account).map(|account| (account.amount, account.locked) )
}

pub fn get_trades(account: Cow<'static, str>)-> Vec<Trade>{
    let ids = ACCOUNTS.get(&account).map(|account| account.trades.clone() ).unwrap_or(Vec::new());
    let mut trades = Vec::new();
    for id in ids {
        TRADES.trade(&id).map(|t| trades.push(t) );
    }
    trades
}

pub fn add_charge(trade_id: Cow<'static, str>, from: Cow<'static, str>, to: Cow<'static, str>, amount: u64, gas: u64, fee: u64)-> Result<()> {
    if TRADES.contains(&trade_id) { return Err(anyhow!("trade {} existed", trade_id )); }
    let trade = Trade::charge(from, to.clone(), amount, gas, fee);
    let _ = TRADES.insert(trade_id.clone(), trade.clone());
    add_with_create(to, trade_id, 0);                       //目标充值地址可能不存在 需要创建
    Ok(())
}

pub fn complete_charge(trade_id: Cow<'static, str>, success: bool)-> bool {
    if let Some(old) = TRADES.update(trade_id, |mut trade| if trade.complete_charge(success) { Some(trade) } else { None } ) {      //update success
        ACCOUNTS.update(&old.to, |_, account| { 
            if success { account.amount += old.amount; }
        }).is_some()
    } else { false}
}


pub fn add_transfer(trade_id: Cow<'static, str>, from: Cow<'static, str>, to: Cow<'static, str>, amount: u64, gas: u64, fee: u64)-> Result<()> {
    if TRADES.contains(&trade_id) { return Err(anyhow!("trade {} existed", trade_id )); }
    if !ACCOUNTS.update(&from, |_, account| {
        if account.amount < amount { false }
        else {
            account.amount -= amount;
            account.locked += amount;
            account.trades.push(trade_id.clone());
            true
        }
    }).ok_or(anyhow!("no account {}", from))? { return Err(anyhow!("{} have no enough amount", from)); }
    add_with_create(to.clone(), trade_id.clone(), 0);   //目标转账地址可能不存在 需要创建
    let trade = Trade::transfer(from, to, amount, gas, fee);
    let _ = TRADES.insert(trade_id, trade);
    Ok(())
}

pub fn start_transfer(trade_id: Cow<'static, str>)-> bool {
    TRADES.update(trade_id, |mut trade| if trade.start_transfer() { Some(trade) } else { None } ).is_some()
}

pub fn complete_transfer(trade_id: Cow<'static, str>, success: bool)-> bool {
    if let Some(old) = TRADES.update(trade_id, |mut trade| if trade.complete_transfer(success) { Some(trade) } else { None } ) {
        if success {
            ACCOUNTS.update(&old.from, |_, account| account.locked -= old.amount );
            ACCOUNTS.update(&old.to, |_, account| account.amount += old.amount );
        } else {
            ACCOUNTS.update(&old.from, |_, account| {
                account.locked -= old.amount;
                account.amount += old.amount;
            });
        }
        true
    } else {
        false
    }
}

pub fn add_withdraw(trade_id: Cow<'static, str>, from: Cow<'static, str>, to: Cow<'static, str>, amount: u64, gas: u64, fee: u64)-> Result<()> {
    if TRADES.contains(&trade_id) { return Err(anyhow!("trade {} existed", trade_id )); }
    if !ACCOUNTS.update(&from, |_, account| {
        if account.amount < amount { false }
        else {
            account.amount -= amount;
            account.trades.push(trade_id.clone());
            true
        }
    }).ok_or(anyhow!("no account {}", from))? {
        return Err(anyhow!("{} have no enough amount", from));
    }
    let trade = Trade::with_draw(from, to, amount, gas, fee);
    let _ = TRADES.insert(trade_id, trade);
    Ok(())
}

pub fn complete_withdraw(trade_id: Cow<'static, str>, success: bool)-> bool {
    if let Some(old) = TRADES.update(trade_id, |mut trade| {
        if trade.complete_withdraw(success) { Some(trade) } else { None } }) {
        if !success {
            ACCOUNTS.update(&old.from, |_, account| account.amount += old.amount );
        }
        true
    } else {
        false
    }
}

pub fn add_trade(trade_id: Cow<'static, str>, trade: Trade) {           //加载初始化的数据, 仅处理所有成功的交易，保证账户余额正确性, 其他交易可以重新发起
    if trade.status != Status::Success as u8 {
    } else {
        match trade.r#type {
            TRADE_CHARGE=> {
                add_with_create(trade.to.clone(), trade_id.clone(), trade.amount);
            }
            TRADE_TRANSFER=> {
                ACCOUNTS.update(&trade.from, |_, account| {
                    account.trades.push(trade_id.clone());
                    account.amount -= trade.amount;
                });
                add_with_create(trade.to.clone(), trade_id.clone(), trade.amount);
            }
            TRADE_WITHDRAW=> {
                ACCOUNTS.update(&trade.from, |_, account| {
                    account.trades.push(trade_id.clone());
                    account.amount -= trade.amount;
                });
            }
            _=> {}
        }
        let _ = TRADES.insert(trade_id, trade);
    }
}
