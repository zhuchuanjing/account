use scc::{HashMap, HashSet};
use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};
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
    AirDrop,                                    //空投类型 仅作为历史需要保留 没有来源的入账 
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
    pub asset: u32,
    pub amount: u64,
    pub to: StaticStr,
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
    pub fn airdrop(to: StaticStr, amount: u64)-> Self {
        Self{r#type: TransferType::AirDrop, status: TransferStatus::Succeeded, create_tick: chrono::Utc::now().timestamp(), update_tick: 0,
            amount, gas: Vec::new(), from: Cow::from(""), to, hash: Cow::from(""), from_node: None, to_node: None, channel: None}
    }
    pub fn pay(from: StaticStr, to: StaticStr, amount: u64, gas: Vec<GasInfo>, hash: StaticStr)-> Self {
        Self{r#type: TransferType::Pay, status: TransferStatus::Pending, create_tick: chrono::Utc::now().timestamp(), update_tick: 0,
            amount, gas, from, to, hash, from_node: None, to_node: None, channel: None}
    }
    pub fn fund(from: StaticStr, to: StaticStr, amount: u64, hash: StaticStr)-> Self {  //充值订单 没有手续费 目的地是平台地址
        Self{r#type: TransferType::Fund, status: TransferStatus::WaitBroadcast, create_tick: chrono::Utc::now().timestamp(), update_tick: 0,
            amount, gas: Vec::new(), from, to, hash, from_node: None, to_node: None, channel: None}
    }
    pub fn withdraw(from: StaticStr, to: StaticStr, amount: u64, gas: Vec<GasInfo>, hash: StaticStr)-> Self {   //生成 withdraw 交易 之前是需要分别生成 交易 rna 手续费 其他手续费三条订单记录 现在放在一条订单里面
        Self{r#type: TransferType::Withdraw, status: TransferStatus::Pending, create_tick: chrono::Utc::now().timestamp(), update_tick: 0,
            amount, gas, from, to, hash, from_node: None, to_node: None, channel: None}
    }
}

static REDIS_URL: &'static str = "redis://127.0.0.1";
use once_cell::sync::Lazy;
use std::sync::Arc;
use lockfree_object_pool::LinearObjectPool;
use redis::{Connection, Commands};

pub const ASSET_NUM: usize = 8;             //暂时支持最多8个资产
pub const ASSET_NAMES: [&'static str; ASSET_NUM] = ["BTC_ASSET_ID", "rgb:7Yjbbk!p-Dl4GOJG-Z2ct!BU-yJ2Ji8I-z13MdSL-QAklonM",
    "rgb:o2PKHzYo-YVviDw7-LKUJAPH-ARrmVW0-aQndBsH-WJJ2540", "rgb:P1Jy$7jt-5ezm74W-SSlIuCW-axO9dfV-$9TPimE-gex6l$8",
    "rgb:!BmcPbfz-BpQWa0Q-qsmVlp0-VV12tvx-I2WkNz3-D!dGFmw", "rgb:RspPWEW9-mzuSNHQ-dGCb054-bLjHPYi-$I9$Ih2-Fy9vxFU",
    "rgb:VNyUso5w-6rx1FoB-kODxlFs-$Ej0BJP-aIsyDMs-acdufQs", "_reserved_2"];

pub static ASSET_JERRY: u32 = 5;
pub static ASSET_RNA: u32 = 2;

pub static TRADES: Lazy<Vec<Arc<TradeManager>>> = Lazy::new(|| {
    let pool = Arc::new(LinearObjectPool::<Connection>::new(move || {
        let client = redis::Client::open(REDIS_URL).map_err(|e| log::error!("{:?}", e) ).unwrap();
        client.get_connection().unwrap()
    }, move |_| {}));

    let mut trades = Vec::new();
    for name in ASSET_NAMES {
        trades.push(Arc::new(TradeManager::new(pool.clone(), Cow::from(name))) );
    }
    trades
});

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

    pub(crate) fn clean_up(&self) {
        let mut c = self.pool.pull();
        let _ = c.del::<&str, bool>(self.list_key.as_ref());
        let _ = c.del::<&str, bool>(self.trades_key.as_ref());
    }

    pub(crate) fn contains(&self, id: &StaticStr)-> bool {
        let mut c = self.pool.pull();
        c.hexists(self.trades_key.as_ref(), id).unwrap_or(false)
    }

    pub(crate) fn insert(&self, id: &StaticStr, t: &Trade)-> bool {
        let mut c = self.pool.pull();
        if c.hset(self.trades_key.as_ref(), id, rmp_serde::to_vec(&t).unwrap()).unwrap_or(false) {
            c.rpush(self.list_key.as_ref(), id).unwrap_or(false)
        } else {
            false
        }
    }

    fn update(&self, id: &StaticStr, value: &Trade)-> bool {       //内存保证多个线程不会同时更新
        let mut c = self.pool.pull();
        c.hset(self.trades_key.as_ref(), id, rmp_serde::to_vec(&value).unwrap()).unwrap_or(false)
    }

    fn get(&self, id: &StaticStr)-> Option<Trade> {
        let mut c = self.pool.pull();
        c.hget::<&str, &str, Vec<u8>>(self.trades_key.as_ref(), id).ok().and_then(|buf| rmp_serde::from_slice::<Trade>(&buf).ok() )
    }

    pub(crate) fn load_all<F: FnMut(StaticStr, Trade)>(&self, mut f: F)-> Result<()> {
        let mut c = self.pool.pull();
        let keys: Vec<String> = c.lrange(self.list_key.as_ref(), 0, -1)?;
        log::info!("{} len {}", self.list_key, keys.len());
        let kvs: std::collections::BTreeMap<String, Vec<u8>> = c.hgetall(&self.trades_key)?;
        log::info!("{} len {}", self.trades_key, kvs.len());
        for key in keys {
            if let Some(trade) = kvs.get(&key).and_then(|buf| rmp_serde::from_slice::<Trade>(&buf).ok() ) {
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
    pub async fn trade(&self, id: &StaticStr)-> Option<Trade> {
        self.trades.get_async(id).await.map(|t| t.clone() )
    }

    pub async fn contains(&self, trade_id: &StaticStr)-> bool {
        self.trades.contains_async(trade_id).await
    }
    pub(crate) async fn add_trade(&self, trade_id: StaticStr, trade: Trade) {
        if trade.status == TransferStatus::Approving {
            self.approving.insert(trade_id.clone()).unwrap();
        }
        let _ = self.trades.insert_async(trade_id, trade).await;
    }
    pub async fn insert(&self, trade_id: StaticStr, trade: Trade)-> Result<()> {
        if self.store.insert(&trade_id, &trade) {
            self.add_trade(trade_id, trade).await;
        }
        Ok(())
    }
    pub async fn update<F: Fn(Trade)-> Option<Trade>>(&self, trade_id: StaticStr, f: F)-> Option<Trade> {
        self.trades.update_async(&trade_id, |k, v| {
            if let Some(updated) = f(v.clone()) {
                if self.store.update(&trade_id, &updated) {
                    if v.status == TransferStatus::Approving && updated.status != TransferStatus::Approving {
                        let _ = self.approving.remove(k);
                    }
                    return std::mem::replace(v, updated);
                }
            }
            v.clone()
        }).await
    }
}
