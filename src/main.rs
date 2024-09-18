use std::{borrow::{Borrow, Cow}, default, io::Read, ptr::addr_eq};
mod trade;
use once_cell::sync::Lazy;


static ADDRS: Lazy<Vec<Cow<'static, str>>> = Lazy::new(|| {
    let mut fs = std::fs::File::open("addr.txt").unwrap();
    let mut lines = String::new();
    fs.read_to_string(&mut lines).unwrap();
    lines.split('\n').map(|s| Cow::from(s.trim().trim_start_matches("\"").trim_end_matches("\"").to_string()) ).collect()
});

use trade::{TRADES, get_trades, get_amount};
use anyhow::Result;

fn main()-> Result<()> {
    TRADES.load().unwrap();

    let mut total = (0, 0);
    for i in 0..ADDRS.len() {
        get_trades(ADDRS[i].clone());
        let amount = get_amount(ADDRS[i].clone()).unwrap();
        total.0 += amount.0;
        total.1 += amount.1;
    }
    println!("total {:?}", total);

    std::thread::spawn(|| {
        let mut charge = 0;
        for id in 0..100 {
            let mut tids = Vec::new();
            for i in 0..200 {
                let tid = Cow::from(snowflaker::next_id_string().unwrap());
                let to = Cow::from(ADDRS[i].clone());
                trade::add_charge(tid.clone(), Cow::from(""), to, 1000, 10, 10).unwrap();
                std::thread::sleep(std::time::Duration::from_millis(1));
                tids.push(tid);
            }
            
            for tid in tids {
                if id % 2 == 0 { trade::complete_charge(tid.clone(), false); }
                else { 
                    if trade::complete_charge(tid.clone(), true) {
                        charge += 1000;
                    } 
                }
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            //println!("charge count {}", id);
        }
        println!("charge {}", charge);
    });

    std::thread::spawn(|| {
        let mut trans = 0;
        for id in 0..100 {
            let mut tids = Vec::new();
            for i in 0..200 {
                let tid = Cow::from(snowflaker::next_id_string().unwrap());
                let from = Cow::from(ADDRS[i].clone());
                let to = Cow::from(ADDRS[200 + i].clone());
                trade::add_transfer(tid.clone(), from, to, 100, 10, 10).unwrap();
                std::thread::sleep(std::time::Duration::from_millis(1));
                tids.push(tid);
            }
            for tid in &tids {
                trade::start_transfer(tid.clone());
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            for tid in &tids {
                std::thread::sleep(std::time::Duration::from_millis(1));
                if id % 2 == 0 { 
                    if trade::complete_transfer(tid.clone(), true) {
                        trans += 100;
                    } 
                }
                else { trade::complete_transfer(tid.clone(), false); }
            }
            //println!("transfer count {}", id);
        }
        println!("transfer {}", trans);
    });
    
    std::thread::spawn(|| {
        let mut withdraw = 0;
        for id in 0..100 {
            let mut tids = Vec::new();
            for i in 0..200 {
                let tid = Cow::from(snowflaker::next_id_string().unwrap());
                let from = Cow::from(ADDRS[i].clone());
                let _ = trade::add_withdraw(tid.clone(), from, Cow::from(""), 100, 10, 10).map_err(|e| { println!("{:?}", e); });
                tids.push(tid);
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            for tid in &tids {
                if id % 2 == 0 { if trade::complete_withdraw(tid.clone(), true) {
                    withdraw += 1000;
                } } else { trade::complete_withdraw(tid.clone(), false); }
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            //println!("withdraw count {}", id);
        }
        println!("withdraw {}", withdraw);
    });

    for _ in 0..10 {
        let mut total = (0, 0);
        for i in 0..ADDRS.len() {
            get_trades(ADDRS[i].clone());
            let amount = get_amount(ADDRS[i].clone()).unwrap();
            total.0 += amount.0;
            total.1 += amount.1;
        }
        println!("total {:?}", total);
            std::thread::sleep(std::time::Duration::from_secs(15));
    }
    Ok(())
}
