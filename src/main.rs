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

use trade::{Trade, get_amount, get_trades, TRADES};
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
            TRADES[asset].add_trade(id, trade);
        }).unwrap();         
    }
    Ok(())
}
