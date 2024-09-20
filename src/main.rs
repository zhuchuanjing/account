use std::{borrow::Cow, io::Read};
mod trade;
use once_cell::sync::Lazy;


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

use trade::{Trade, get_amount, get_trades, SledStore, Status, TRADES};
use anyhow::Result;
use trade::TradeStore;
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

    for asset in 0..trade::ASSET_NUM {
        TRADES[asset].store.load_all(|id, trade: Trade| {
            trade::add_trade(asset as u32, id.clone(), trade.clone());
            TRADES[asset].trades.insert(id, trade).unwrap();
        }).unwrap();         
    }

    /*for i in 0..10 {
        let tid = Cow::from(snowflaker::next_id_string().unwrap());
        let from = Cow::from(ADDRS[i].clone());
        let to = Cow::from(ADDRS[200 + i].clone());
        for asset in 0..trade::ASSET_NUM {
            if asset % 2 == 0 {
                let _ = trade::add_transfer(asset as u32, tid.clone(), from.clone(), to.clone(), 100, 10, 10).map_err(|e| log::error!("{:?}", e));
            }
        }        
    }*/
    /*for i in 0..10 {
        let trades = get_trades(&ADDRS[i]);
        for trade in trades {
            if trade.2.status == Status::Start as u8 {
                println!("{:?}", trade);
                if i % 2 == 0 {
                    trade::complete_transfer(trade.0, trade.1, true);
                } else {
                    trade::complete_transfer(trade.0, trade.1, false);
                }
            }
        }
    }
    display();

    return Ok(());*/
    /*      从老的数据库迁移，只需要 读取所有已经完成的交易(成功的) 然后使用 add_trade 写入数据库就可以
            for trade in [all success trade] {
                add_trade(trade_id, trade_info);
            }
     */
    let now = std::time::Instant::now();
    
    std::thread::spawn(|| {
        let mut charge = 0;
        for id in 0..120 {
            let mut tids = Vec::new();
            for i in 0..200 {
                let tid = Cow::from(snowflaker::next_id_string().unwrap());
                let to = Cow::from(ADDRS[i].clone());
                for asset in 0..trade::ASSET_NUM {
                    trade::add_charge(asset as u32, tid.clone(), Cow::from(""), to.clone(), 1000, 10, 10).unwrap();
                }
                std::thread::sleep(std::time::Duration::from_millis(1));
                tids.push(tid);
            }
            
            for tid in tids {
                for asset in 0..trade::ASSET_NUM {
                    if id % 3 == 0 { trade::complete_charge(asset as u32, tid.clone(), false); }
                    else { 
                        if trade::complete_charge(asset as u32, tid.clone(), true) {
                            charge += 1000;
                        } 
                    }
                }
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
        }
        println!("charge {}", charge);
    });

    std::thread::spawn(move || {
        let mut trans = 0;
        for id in 0..30 {
            let mut tids = Vec::new();
            for i in 0..200 {
                let tid = Cow::from(snowflaker::next_id_string().unwrap());
                let from = Cow::from(ADDRS[i].clone());
                let to = Cow::from(ADDRS[200 + i].clone());
                for asset in 0..trade::ASSET_NUM {
                    let _ = trade::add_transfer(asset as u32, tid.clone(), from.clone(), to.clone(), 100, 10, 10).map_err(|e| log::error!("{:?}", e));
                }
                std::thread::sleep(std::time::Duration::from_millis(1));
                tids.push(tid);
            }
            /*for tid in &tids {
                trade::start_transfer(tid.clone());
                std::thread::sleep(std::time::Duration::from_millis(1));
            }*/
            for tid in &tids {
                std::thread::sleep(std::time::Duration::from_millis(1));
                for asset in 0..trade::ASSET_NUM {
                    if id % 2 == 0 { 
                        if trade::complete_transfer(asset as u32, tid.clone(), true) {
                            trans += 100;
                        } 
                    }
                    else { trade::complete_transfer(asset as u32, tid.clone(), false); }
                }
            }
        }
        println!("transfer {}", trans);
        let elaspe = std::time::Instant::now().duration_since(now);
        println!("{:?}", elaspe);
    });
    
    std::thread::spawn(|| {
        let mut withdraw = 0;
        for id in 0..100 {
            let mut tids = Vec::new();
            for i in 0..200 {
                let tid = Cow::from(snowflaker::next_id_string().unwrap());
                let from = Cow::from(ADDRS[i].clone());
                for asset in 0..trade::ASSET_NUM {
                    let _ = trade::add_withdraw(asset as u32, tid.clone(), from.clone(), Cow::from(""), 100, 10, 10).map_err(|e| { log::error!("{:?}", e); });
                }
                tids.push(tid);
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            for tid in &tids {
                for asset in 0..trade::ASSET_NUM {
                    if id % 2 == 0 { if trade::complete_withdraw(asset as u32, tid.clone(), true) {
                        withdraw += 100;
                    } } else { trade::complete_withdraw(asset as u32, tid.clone(), false); }
                }
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
        }
        println!("withdraw {}", withdraw);
    });

    for _ in 0..20 {
        display();
        std::thread::sleep(std::time::Duration::from_secs(15));
    }
    Ok(())
}
