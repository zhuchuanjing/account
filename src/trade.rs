use scc::HashMap;
use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;
use std::borrow::Cow;

const TRADE_NONE: u8 = 0;
const TRADE_CHARGE: u8 = 1;
const TRADE_TRANSFER: u8 = 2;
const TRADE_WITHDRAW: u8 = 3;

#[derive(Clone, Debug)]
pub enum Status {                       //状态只包括开始 成功 和失败 如果可能有多个环节的事务可以拆成多个事务进行 以保证处理的简单性
    Start,
    Fail,
    Success
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Trade {
    pub r#type: u8,
    pub status: u8,
    pub create_tick: i64,
    pub update_tick: i64,
    pub amount: u64,
    pub gas: u64,
    pub from: Cow<'static, str>,
    pub to: Cow<'static, str>,
    pub from_node: Option<Cow<'static, str>>,
    pub to_node: Option<Cow<'static, str>>,
    pub channel: Option<Cow<'static, str>>,
    pub hash: Cow<'static, str>,
}

impl Trade {
    pub fn modify(&mut self, success: bool)-> bool {
        if self.status == Status::Start as u8 { 
            self.status = if success { Status::Success as u8 } else { Status::Fail as u8 };
            self.update_tick = chrono::Utc::now().timestamp();
            true
        } else { false }
    }
    pub fn success(&mut self)-> bool {
        self.modify(true)
    }
    pub fn fail(&mut self)-> bool {
        self.modify(false)
    }
}

impl Trade {
    pub fn charge(from: Cow<'static, str>, to: Cow<'static, str>, amount: u64, gas: u64, hash: Cow<'static, str>)-> Self {
        Self{r#type: TRADE_CHARGE, status: Status::Start as u8, create_tick: chrono::Utc::now().timestamp(), update_tick: 0,
            amount, gas, from, to, hash, from_node: None, to_node: None, channel: None}
    }

    pub fn transfer(from: Cow<'static, str>, to: Cow<'static, str>, amount: u64, gas: u64, hash: Cow<'static, str>)-> Self {
        Self{r#type: TRADE_TRANSFER, status: Status::Start as u8, create_tick: chrono::Utc::now().timestamp(), update_tick: 0,
            amount, gas, from, to, hash, from_node: None, to_node: None, channel: None}
    }

    pub fn with_draw(from: Cow<'static, str>, to: Cow<'static, str>, amount: u64, gas: u64, fee: u64, hash: Cow<'static, str>)-> Self {
        Self{r#type: TRADE_WITHDRAW, status: Status::Start as u8, create_tick: chrono::Utc::now().timestamp(), update_tick: 0,
            amount, gas, from, to, hash, from_node: None, to_node: None, channel: None}
    }
}

pub static TRADES: Lazy<Vec<Arc<TradeManager>>> = Lazy::new(|| {
    let db = sled::open("sled").unwrap();
    let mut trades = Vec::new();
    for name in ASSET_NAMES {
        trades.push(Arc::new(TradeManager::new(&db, Cow::from(name))) );
    }
    trades
});

pub trait TradeStore<T: Serialize + DeserializeOwned + Clone + std::fmt::Debug> {
    fn contains(&self, id: &Cow<'static, str>)-> bool;
    fn insert(&self, id: &Cow<'static, str>, t: &T)-> bool;                          //增加一条交易,如果已经存在则返回 false
    fn update<F: Fn(T)-> Option<T>>(&self, id: &Cow<'static, str>, f: F)-> Option<T>;
    fn get(&self, id: &Cow<'static, str>)-> Option<T>;
    fn load_all<F: Fn(Cow<'static, str>, T)>(&self, f: F)-> Result<()>;
}

pub struct SledStore {
    name: Cow<'static, str>,
    tree: sled::Tree,
}

impl SledStore {
    pub fn new(db: &sled::Db, name: Cow<'static, str>) -> Self {
        let tree = db.open_tree(name.as_ref()).unwrap();
        Self{name: Cow::from(name), tree}
    }
}

impl<T: Serialize + DeserializeOwned + Clone + std::fmt::Debug> TradeStore<T> for SledStore {
    fn contains(&self, id: &Cow<'static, str>)-> bool {
        self.tree.contains_key(id.as_bytes()).unwrap_or(false)
    }
    fn insert(&self, id: &Cow<'static, str>, t: &T)-> bool {
        if !self.tree.contains_key(id.as_bytes()).unwrap_or(false) {
            log::info!("insert {} {} {:?}", self.name, id, t);
            self.tree.insert(id.as_bytes(), rmp_serde::to_vec(&t).unwrap()).is_ok()
        } else { false }
    }
    fn update<F: Fn(T)-> Option<T>>(&self, id: &Cow<'static, str>, f: F)-> Option<T> {
        self.tree.update_and_fetch(id.as_bytes(), |old|
            old.and_then(|old| {
                let t = rmp_serde::from_slice::<T>(old).unwrap();
                if let Some(new_value) = f(t) {
                    log::info!("update {} {} {:?}", self.name, id, new_value);
                    rmp_serde::to_vec(&new_value).ok() 
                }
                else { Some(old.to_vec()) }
            })
        ).map(|old| old.and_then(|old| rmp_serde::from_slice::<T>(&old).ok() )).unwrap_or(None) 
    }
    fn get(&self, id: &Cow<'static, str>)-> Option<T> {
        self.tree.get(id.as_bytes()).map(|t| t.and_then(|t| rmp_serde::from_slice::<T>(&t).ok() ) ).unwrap_or(None)
    }
    fn load_all<F: FnMut(Cow<'static, str>, T)>(&self, mut f: F)-> Result<()> {
        let mut iter = self.tree.iter();
        while let Some(Ok(kv)) = iter.next() {
            let id = String::from_utf8(kv.0.to_vec())?;
            let trade: T = rmp_serde::from_slice(&kv.1.to_vec())?;
            f(Cow::from(id), trade);
        }
        Ok(())
    }
}

pub struct TradeManager {
    pub trades: HashMap<Cow<'static, str>, Trade>,                      //内存中保存的所有交易的列表
    pub store: SledStore,
}

impl TradeManager {
    pub fn new(db: &sled::Db, name: Cow<'static, str>)-> Self {
        Self{trades: HashMap::default(), store: SledStore::new(&db, name)}
    }
    pub fn trade(&self, id: &Cow<'static, str>)-> Option<Trade> {
        self.trades.get(id).map(|t| t.clone() )
    }

    pub fn contains(&self, trade_id: &Cow<'static, str>)-> bool {
        self.trades.contains(trade_id)
    }
    pub fn insert(&self, trade_id: Cow<'static, str>, trade: Trade)-> Result<()> {
        if self.store.insert(&trade_id, &trade) {
            let _ = self.trades.insert(trade_id, trade);
        }
        Ok(())
    }
    pub fn update<F: Fn(Trade)-> Option<Trade>>(&self, trade_id: Cow<'static, str>, f: F)-> Option<Trade> {
        self.store.update(&trade_id, f)
    }
}

pub const ASSET_NUM: usize = 8;             //暂时支持最多8个资产
pub const ASSET_NAMES: [&'static str; ASSET_NUM] = ["btc", "rna", "jerry", "tom", "zhu", "pig", "godess", "none"];

#[derive(Clone, Debug)]
pub struct Account {
    amounts: [(u64, u64); ASSET_NUM],
    trades: Vec<(u32, Cow<'static, str>)>
}

impl Default for Account {
    fn default()-> Self {
        Self{amounts: [(0,0), (0,0), (0,0), (0,0), (0,0), (0,0), (0,0), (0,0)], trades: Vec::new()}
    }
}

impl Account {
    pub fn lock(&mut self, asset: usize, amount: u64)-> bool {    //锁定资金 开始提现或者转出
        if self.amounts[asset].0 >= amount {
            self.amounts[asset].0 -= amount;
            self.amounts[asset].1 += amount;
            true
        } else { false }
    }
    pub fn confirm(&mut self, asset: usize, amount: u64)-> bool {        //确认转出 或者确认提现
        self.amounts[asset].1 -= amount;
        true
    }
    pub fn rollback(&mut self, asset: usize, amount: u64)-> bool {       //用于转账失败或者 提现失败的回滚
        self.amounts[asset].1 -= amount;
        self.amounts[asset].0 += amount;
        true
    }
    pub fn income(&mut self, asset: usize, amount: u64)-> bool {        //仅用于充值到账 以及转账接收方到账
        self.amounts[asset].0 += amount;
        true
    }
    pub fn decrease(&mut self, asset: usize, amount: u64)-> bool {      //减少 asset 仅用于重新加载的时候 没有锁定直接减少
        self.amounts[asset].0 -= amount;
        true
    }
}

use once_cell::sync::Lazy;
use std::sync::Arc;
static ACCOUNTS: Lazy<Arc<HashMap<Cow<'static, str>, Account>>> = Lazy::new(|| Arc::new(HashMap::default()) );

fn account_modify<F: FnOnce(&mut Account)-> bool>(account: &Cow<'static, str>, f: F)-> bool {                                           //充值或者转账到账
    ACCOUNTS.update(account, |_, account| f(account) ).unwrap_or(false)
}

fn account_add(account: Cow<'static, str>, asset: u32, trade_id: Cow<'static, str>) {       //用于转账接收方或者充值方 如果账号不存在则创建一个
    ACCOUNTS.entry(account).and_modify(|account| {
        account.trades.push((asset, trade_id.clone()));
    }).or_insert(Account{amounts: [(0,0), (0,0), (0,0), (0,0), (0,0), (0,0), (0,0), (0,0)], trades: vec![(asset, trade_id)]});
}

pub fn get_amount(account: &Cow<'static, str>)-> Option<[(u64, u64); ASSET_NUM]>{
    ACCOUNTS.get(account).map(|account| account.amounts )
}

pub fn get_trades(account: &Cow<'static, str>)-> Vec<(u32, Cow<'static, str>, Trade)>{
    let ids = ACCOUNTS.get(account).map(|account| account.trades.clone() ).unwrap_or(Vec::new());
    let mut trades = Vec::new();
    for (asset, id) in ids {
        TRADES[asset as usize].trade(&id).map(|t| trades.push((asset, id, t)) );
    }
    trades
}

pub fn add_charge(asset: u32, trade_id: Cow<'static, str>, from: Cow<'static, str>, to: Cow<'static, str>, amount: u64, gas: u64, fee: u64)-> Result<()> {
    if TRADES[asset as usize].contains(&trade_id) { return Err(anyhow!("trade {} existed", trade_id )); }
    let trade = Trade::charge(from, to.clone(), amount, gas, fee);
    let _ = TRADES[asset as usize].insert(trade_id.clone(), trade.clone());
    account_add(to, asset, trade_id);
    Ok(())
}

pub fn complete_charge(asset: u32, trade_id: Cow<'static, str>, success: bool)-> bool {
    if let Some(old) = TRADES[asset as usize].update(trade_id, |mut trade| if trade.success() { Some(trade) } else { None } ) {      //update success
        if success { account_modify(&old.to, |account| account.income(asset as usize, old.amount) ) }
        else { true }
    } else { false}
}


pub fn add_transfer(asset: u32, trade_id: Cow<'static, str>, from: Cow<'static, str>, to: Cow<'static, str>, amount: u64, gas: u64, fee: u64)-> Result<()> {
    if TRADES[asset as usize].contains(&trade_id) { return Err(anyhow!("trade {} existed", trade_id )); }
    if account_modify(&from, |account| 
        if account.lock(asset as usize, amount) { 
            account.trades.push((asset, trade_id.clone()));
            true
        } else { false }
    ) {
        account_add(to.clone(), asset, trade_id.clone());
        let trade = Trade::transfer(from, to, amount, gas, fee);
        let _ = TRADES[asset as usize].insert(trade_id, trade);
        Ok(())
    } else { Err(anyhow!("{} have no enough amount", from)) }
}

pub fn complete_transfer(asset: u32, trade_id: Cow<'static, str>, success: bool)-> bool {
    if let Some(old) = TRADES[asset as usize].update(trade_id, |mut trade| if trade.modify(success) { Some(trade) } else { None } ) {
        if success {
            account_modify(&old.from, |account| account.confirm(asset as usize, old.amount) );
            account_modify(&old.to, |account| account.income(asset as usize, old.amount) );
        } else {
            account_modify(&old.from, |account| account.rollback(asset as usize, old.amount) );
        }
        true
    } else {
        false
    }
}

pub fn add_withdraw(asset: u32, trade_id: Cow<'static, str>, from: Cow<'static, str>, to: Cow<'static, str>, amount: u64, gas: u64, fee: u64)-> Result<()> {
    if TRADES[asset as usize].contains(&trade_id) { return Err(anyhow!("trade {} existed", trade_id )); }
    if account_modify(&from, |account| 
        if account.lock(asset as usize, amount) { 
            account.trades.push((asset, trade_id.clone()));
            true
        } else { false }
    ) {
        let trade = Trade::with_draw(from, to, amount, gas, fee);
        let _ = TRADES[asset as usize].insert(trade_id, trade);
        Ok(())        
    } else { Err(anyhow!("{} have no enough amount", from)) }
}

pub fn complete_withdraw(asset: u32, trade_id: Cow<'static, str>, success: bool)-> bool {
    if let Some(old) = TRADES[asset as usize].update(trade_id, |mut trade| {
        if trade.modify(success) { Some(trade) } else { None } 
    }) {
        account_modify(&old.from, |account| if success { account.confirm(asset as usize, old.amount) } else { account.rollback(asset as usize, old.amount) } );
        true
    } else {
        false
    }
}

pub fn add_trade(asset: u32, trade_id: Cow<'static, str>, trade: Trade) {           //加载初始化的数据, 
    match trade.r#type {
        TRADE_CHARGE=> {
            account_add(trade.to.clone(), asset, trade_id.clone());
            if trade.status == Status::Success as u8 {
                account_modify(&trade.to, |account| account.income(asset as usize, trade.amount) );
            }
        }
        TRADE_TRANSFER=> {
            account_add(trade.from.clone(), asset, trade_id.clone());
            account_add(trade.to.clone(), asset, trade_id.clone());
            if trade.status == Status::Success as u8 {
                account_modify(&trade.from, |account| account.decrease(asset as usize, trade.amount) );
                account_modify(&trade.to, |account| account.income(asset as usize, trade.amount) );
            } else if trade.status == Status::Start as u8 {
                account_modify(&trade.from, |account| account.lock(asset as usize, trade.amount) );
            }
        }
        TRADE_WITHDRAW=> {
            account_add(trade.from.clone(), asset, trade_id.clone());
            if trade.status == Status::Success as u8 {
                account_modify(&trade.from, |account| account.decrease(asset as usize, trade.amount) );
            } else if trade.status == Status::Start as u8 {
                account_modify(&trade.from, |account| account.lock(asset as usize, trade.amount) );
            }
        }
        _=> {}
    }
}
