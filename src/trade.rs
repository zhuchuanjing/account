use scc::{HashMap, HashSet};
use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;
use std::borrow::Cow;

pub type StaticStr = Cow<'static, str>;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum TransferType {
    NodeFund,
    Fund,
    Withdraw,
    NodeWithdraw,
    Pay,
    Gas,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum TransferStatus {
    Approving,                                  //增加审核中状态 
    WaitBroadcast,
    Pending,
    Succeeded,
    Failed,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct GasInfo {
    asset: u32,
    amount: u64,
    to: StaticStr,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Trade {
    pub r#type: TransferType,
    pub status: TransferStatus,
    pub create_tick: i64,
    pub update_tick: i64,
    pub amount: u64,
    pub gas: Vec<GasInfo>,
    pub from: StaticStr,
    pub to: StaticStr,
    pub from_node: Option<StaticStr>,
    pub to_node: Option<StaticStr>,
    pub channel: Option<StaticStr>,
    pub hash: StaticStr,
}

impl Trade {
    pub fn start(&mut self)-> bool {
        if self.status == TransferStatus::WaitBroadcast {
            self.status = TransferStatus::Pending;
            true
        } else { false }
    }
    pub fn modify(&mut self, success: bool)-> bool {
        if self.status == TransferStatus::Pending { 
            self.status = if success { TransferStatus::Succeeded } else { TransferStatus::Failed };
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
    pub fn pay(from: StaticStr, to: StaticStr, amount: u64, gas: Vec<GasInfo>, hash: StaticStr)-> Self {
        Self{r#type: TransferType::Pay, status: TransferStatus::Pending, create_tick: chrono::Utc::now().timestamp(), update_tick: 0,
            amount, gas, from, to, hash, from_node: None, to_node: None, channel: None}
    }
//充值订单 没有手续费 目的地是平台地址
    pub fn fund(from: StaticStr, to: StaticStr, amount: u64, hash: StaticStr)-> Self {
        Self{r#type: TransferType::Fund, status: TransferStatus::WaitBroadcast, create_tick: chrono::Utc::now().timestamp(), update_tick: 0,
            amount, gas: Vec::new(), from, to, hash, from_node: None, to_node: None, channel: None}
    }
//生成 withdraw 交易 之前是需要分别生成 交易 rna 手续费 其他手续费三条订单记录 现在放在一条订单里面
    pub fn withdraw(from: StaticStr, to: StaticStr, amount: u64, gas: Vec<GasInfo>, hash: StaticStr)-> Self {
        Self{r#type: TransferType::Withdraw, status: TransferStatus::Pending, create_tick: chrono::Utc::now().timestamp(), update_tick: 0,
            amount, gas, from, to, hash, from_node: None, to_node: None, channel: None}
    }
}

static REDIS_URL: &'static str = "redis://127.0.0.1";

pub static TRADES: Lazy<Vec<Arc<TradeManager>>> = Lazy::new(|| {
    let pool = Arc::new(LinearObjectPool::<Connection>::new(move || {
        let client = redis::Client::open(REDIS_URL).map_err(|e| log::error!("{:?}", e) ).unwrap();
        client.get_connection().map_err(|e| log::error!("{:?}", e) ).unwrap()
    }, move |con| {}));

    let mut trades = Vec::new();
    for name in ASSET_NAMES {
        trades.push(Arc::new(TradeManager::new(pool.clone(), Cow::from(name))) );
    }
    trades
});

pub trait TradeStore<T: Serialize + DeserializeOwned + Clone + std::fmt::Debug> {
    fn contains(&self, id: &StaticStr)-> bool;
    fn insert(&self, id: &StaticStr, t: &T)-> bool;                                     //增加一条交易,如果已经存在则返回 false
    fn update(&self, id: &StaticStr, value: &T)-> bool;
    fn get(&self, id: &StaticStr)-> Option<T>;
    fn load_all<F: Fn(StaticStr, T)>(&self, f: F)-> Result<()>;
}

use lockfree_object_pool::LinearObjectPool;
use redis::{Connection, Commands};

pub struct RedisStore {
    list_key: StaticStr,
    trades_key: StaticStr,
    pool: Arc<LinearObjectPool::<Connection>>,
}

impl RedisStore {
    pub fn new(name: StaticStr, pool: Arc<LinearObjectPool::<Connection>>)-> Self {
        let list_key = Cow::from(format!("@list::{}", name));
        let trades_key = Cow::from(format!("@trades::{}", name));
        Self{list_key, trades_key, pool}
    }
}

impl<T: Serialize + DeserializeOwned + Clone + std::fmt::Debug> TradeStore<T> for RedisStore {
    fn contains(&self, id: &StaticStr)-> bool {
        let mut c = self.pool.pull();
        c.hexists(self.trades_key.as_ref(), id).unwrap_or(false)
    }

    fn insert(&self, id: &StaticStr, t: &T)-> bool {
        let mut c = self.pool.pull();
        if c.hexists(self.trades_key.as_ref(), id).unwrap_or(false) {
            return false;
        }
        if c.hset(self.trades_key.as_ref(), id, rmp_serde::to_vec(&t).unwrap()).unwrap_or(false) {
            c.rpush(self.list_key.as_ref(), id).unwrap_or(false)
        } else {
            false
        }
    }

    fn update(&self, id: &StaticStr, value: &T)-> bool {       //内存保证多个线程不会同时更新
        let mut c = self.pool.pull();
        c.hset(self.trades_key.as_ref(), id, rmp_serde::to_vec(&value).unwrap()).unwrap_or(false)
    }

    fn get(&self, id: &StaticStr)-> Option<T> {
        let mut c = self.pool.pull();
        c.hget::<&str, &str, Vec<u8>>(self.trades_key.as_ref(), id).ok().and_then(|buf| rmp_serde::from_slice::<T>(&buf).ok() )
    }

    fn load_all<F: FnMut(StaticStr, T)>(&self, mut f: F)-> Result<()> {
        let mut c = self.pool.pull();
        let keys: Vec<String> = c.lrange(self.list_key.as_ref(), 0, -1)?;
        for key in keys {
            if let Some(trade) = c.hget::<&str, &str, Vec<u8>>(self.trades_key.as_ref(), &key).ok().and_then(|buf| rmp_serde::from_slice::<T>(&buf).ok() ) {
                f(Cow::from(key), trade);    
            }
        }
        Ok(())
    }
}

pub struct TradeManager {
    pub trades: HashMap<StaticStr, Trade>,                      //内存中保存的所有交易的列表
    pub approving: HashSet<StaticStr>,
    pub store: RedisStore,
}

impl TradeManager {
    pub fn new(pool: Arc<LinearObjectPool::<Connection>>, name: StaticStr)-> Self {
        Self{trades: HashMap::default(), approving: HashSet::default(), store: RedisStore::new( name, pool)}
    }
    pub fn trade(&self, id: &StaticStr)-> Option<Trade> {
        self.trades.get(id).map(|t| t.clone() )
    }

    pub fn contains(&self, trade_id: &StaticStr)-> bool {
        self.trades.contains(trade_id)
    }
    pub fn add_trade(&self, trade_id: StaticStr, trade: Trade) {
        if trade.status == TransferStatus::Approving {
            self.approving.insert(trade_id.clone()).unwrap();
        }
        let _ = self.trades.insert(trade_id, trade);
    }
    pub fn insert(&self, trade_id: StaticStr, trade: Trade)-> Result<()> {
        if self.store.insert(&trade_id, &trade) {
            self.add_trade(trade_id, trade);
        }
        Ok(())
    }
    pub fn update<F: Fn(Trade)-> Option<Trade>>(&self, trade_id: StaticStr, f: F)-> Option<Trade> {
        self.trades.update(&trade_id, |k, v| {
            if let Some(updated) = f(v.clone()) {
                if self.store.update(&trade_id, &updated) {
                    if v.status == TransferStatus::Approving && updated.status != TransferStatus::Approving {
                        let _ = self.approving.insert(k.clone());
                        return std::mem::replace(v, updated);
                    }
                }
            }
            v.clone()
        })
    }
}

pub const ASSET_NUM: usize = 8;             //暂时支持最多8个资产
pub const ASSET_NAMES: [&'static str; ASSET_NUM] = ["BTC_ASSET_ID", "rgb:7Yjbbk!p-Dl4GOJG-Z2ct!BU-yJ2Ji8I-z13MdSL-QAklonM",
    "rgb:o2PKHzYo-YVviDw7-LKUJAPH-ARrmVW0-aQndBsH-WJJ2540", "rgb:P1Jy$7jt-5ezm74W-SSlIuCW-axO9dfV-$9TPimE-gex6l$8",
    "rgb:!BmcPbfz-BpQWa0Q-qsmVlp0-VV12tvx-I2WkNz3-D!dGFmw", "rgb:RspPWEW9-mzuSNHQ-dGCb054-bLjHPYi-$I9$Ih2-Fy9vxFU",
    "rgb:VNyUso5w-6rx1FoB-kODxlFs-$Ej0BJP-aIsyDMs-acdufQs", "_reserved_2"];

pub fn get_asset_id(asset_name: &str)-> Result<usize> {
    ASSET_NAMES.iter().position(|a| *a == asset_name ).ok_or(anyhow!("unknow asset {}", asset_name) )
}

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
        for g in &trade.gas {
            if g.asset as usize == asset {
                if self.amounts[asset].0 < g.amount + trade.amount { return false; }
                else if self.amounts[asset].0 < g.amount { return false; }
            }
        }
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

fn account_modify<F: FnOnce(&mut Account)-> bool>(account: &StaticStr, f: F)-> bool {                                           //充值或者转账到账
    ACCOUNTS.update(account, |_, account| f(account) ).unwrap_or(false)
}

fn account_add(account: StaticStr, asset: u32, trade_id: StaticStr) {       //用于转账接收方或者充值方 如果账号不存在则创建一个
    ACCOUNTS.entry(account).and_modify(|account| {
        account.trades.push((asset, trade_id.clone()));
    }).or_insert(Account{amounts: [(0,0), (0,0), (0,0), (0,0), (0,0), (0,0), (0,0), (0,0)], trades: vec![(asset, trade_id)]});
}

fn account_start(asset: u32, trade_id: StaticStr, trade: &Trade)-> bool {       //创建一笔转账或者提现交易
    account_modify(&trade.from, |account| 
        if account.lock(asset as usize, &trade) {
            account.trades.push((asset, trade_id));
            true
        } else { false }
    )
}

fn account_success(asset: u32, trade: &Trade, with_lock: bool)-> bool {            //成功完成一笔交易
    if account_modify(&trade.from, |account| {
        if with_lock {
            account.confirm(asset as usize, &trade)
        } else {
            if !account.decrease(asset as usize, &trade) {
                log::error!("debit {} {} {}", ASSET_NAMES[asset as usize], trade.from, trade.amount);
                let _ = WARNINGS.insert((asset, trade.from.clone()));
            }
            true
        }
    }) {
        for g in &trade.gas {
            account_modify(&g.to, |account| account.income(g.asset as usize, g.amount) );
        }    
        account_modify(&trade.to, |account| account.income(asset as usize, trade.amount) )
    } else { false }           
}

pub fn get_amount(account: &StaticStr)-> Option<[(u64, u64); ASSET_NUM]>{
    ACCOUNTS.get(account).map(|account| account.amounts )
}

pub fn get_trades(asset: u32, account: &StaticStr)-> Vec<(StaticStr, Trade)>{
    let ids = ACCOUNTS.get(account).map(|account| {
        account.trades.iter().filter_map(|t| if t.0 == asset { Some(t.1.clone()) } else { None }).collect()
    }).unwrap_or(Vec::new());
    let mut trades = Vec::new();
    for id in ids {
        TRADES[asset as usize].trade(&id).map(|t| trades.push((id.clone(), t)) );
    }
    trades
}

pub fn add_fund(asset: u32, trade_id: StaticStr, from: StaticStr, to: StaticStr, amount: u64, hash: StaticStr)-> Result<()> {
    if TRADES[asset as usize].contains(&trade_id) { return Err(anyhow!("trade {} existed", trade_id )); }
    let trade = Trade::fund(from, to.clone(), amount, hash);
    let _ = TRADES[asset as usize].insert(trade_id.clone(), trade.clone());
    account_add(to, asset, trade_id);
    Ok(())
}

pub fn complete_fund(asset: u32, trade_id: StaticStr, success: bool)-> bool {
    if let Some(old) = TRADES[asset as usize].update(trade_id, |mut trade| if trade.success() { Some(trade) } else { None } ) {      //update success
        if success { account_modify(&old.to, |account| account.income(asset as usize, old.amount) ) }
        else { true }
    } else { false}
}


pub fn add_pay(asset: u32, trade_id: StaticStr, from: StaticStr, to: StaticStr, amount: u64, gas: Vec<GasInfo>, hash: StaticStr)-> Result<()> {
    if TRADES[asset as usize].contains(&trade_id) { return Err(anyhow!("trade {} existed", trade_id )); }
    let trade = Trade::pay(from, to, amount, gas, hash);
    if account_start(asset, trade_id.clone(), &trade) {
        account_add(trade.to.clone(), asset, trade_id.clone());
        let _ = TRADES[asset as usize].insert(trade_id, trade);
        Ok(())
    } else { Err(anyhow!("{} have no enough amount", trade.from)) }
}

pub fn complete_pay(asset: u32, trade_id: StaticStr, success: bool)-> bool {
    if let Some(old) = TRADES[asset as usize].update(trade_id.clone(), |mut trade| if trade.modify(success) { Some(trade) } else { None } ) {
        if success {
            account_success(asset, &old, true)
        } else {
            account_modify(&old.from, |account| {
                account.rollback(asset as usize, &old) 
            })
        }
    } else { false }
}

pub fn add_withdraw(asset: u32, trade_id: StaticStr, from: StaticStr, to: StaticStr, amount: u64, gas: Vec<GasInfo>, hash: StaticStr)-> Result<()> {
    if TRADES[asset as usize].contains(&trade_id) { return Err(anyhow!("trade {} existed", trade_id )); }
    let trade = Trade::withdraw(from, to, amount, gas, hash);
    if account_start(asset, trade_id.clone(), &trade) {
        let _ = TRADES[asset as usize].insert(trade_id, trade);
        Ok(())        
    } else { Err(anyhow!("{} have no enough amount", trade.from)) }
}

pub fn complete_withdraw(asset: u32, trade_id: StaticStr, success: bool)-> bool {
    if let Some(old) = TRADES[asset as usize].update(trade_id.clone(), |mut trade| {
        if trade.modify(success) { Some(trade) } else { None } 
    }) {
        if success {
            account_success(asset, &old, true)
        } else {
            account_modify(&old.from, |account| {
                account.rollback(asset as usize, &old) 
            })
        }
    } else { false }
}

pub fn add_trade(asset: u32, trade_id: StaticStr, trade: Trade) {           //加载初始化的数据, 
    match trade.r#type {
        TransferType::Fund=> {
            account_add(trade.to.clone(), asset, trade_id.clone());
            if trade.status == TransferStatus::Succeeded {
                account_modify(&trade.to, |account| account.income(asset as usize, trade.amount) );
            }
        }
        TransferType::Pay=> {
            account_add(trade.from.clone(), asset, trade_id.clone());
            account_add(trade.to.clone(), asset, trade_id.clone());
            if trade.status == TransferStatus::Succeeded {
                account_success(asset, &trade, false);
            } else if trade.status != TransferStatus::Failed {
                account_start(asset, trade_id, &trade);
            }
        }
        TransferType::Withdraw=> {
            account_add(trade.from.clone(), asset, trade_id.clone());
            account_add(trade.to.clone(), asset, trade_id.clone());
            if trade.status == TransferStatus::Succeeded {
                account_success(asset, &trade, false);
            } else if trade.status != TransferStatus::Failed {
                account_start(asset, trade_id, &trade);
            }
        }
        _=> {}
    }
}

pub fn import_trade(asset: u32, trade_id: StaticStr, trade: Trade)-> bool {
    if !TRADES[asset as usize].contains(&trade_id) {
        TRADES[asset as usize].store.insert(&trade_id, &trade);
        add_trade(asset as u32, trade_id.clone(), trade.clone());
        TRADES[asset as usize].add_trade(trade_id, trade);
        true
    } else { false }
}