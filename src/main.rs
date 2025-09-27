use ethers::{
    contract::{abigen, EthLogDecode},
    utils::format_units,
    core::types::{Address, Filter, Log, U64, H256, U256}, 
    providers::{Provider, Ws, StreamExt, Middleware}, 
    abi::RawLog, 
};
use sqlx::{sqlite::SqlitePool, FromRow, Executor, Row, query_scalar};
use clap::Parser;
use dotenv::dotenv;
use std::{env, collections::HashSet, str::FromStr};
use eyre::Result;
use serde; 

// --- 1. Constants and Configuration ---

const POL_CONTRACT_ADDRESS: &str = "0x0000000000000000000000000000000000001010"; 
const POL_DECIMALS: &str = "18";
const TRANSFER_EVENT_TOPIC: &str = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

const BINANCE_ADDRS_STR: [&str; 6] = [
    "0xF977814e90dA44bFA03b6295A0616a897441aceC", "0xe7804c37c13166fF0b37F5aE0BB07A3aEbb6e245", 
    "0x505e71695E9bc45943c58adEC1650577BcA68fD9", "0x290275e3db66394C52272398959845170E4DCb88", 
    "0xD5C08681719445A5Fdce2Bda98b341A49050d821", "0x082489A616aB4D46d1947eE3F912e080815b08DA",
];

abigen!(
    POLTokenContract,
    r#"[
        event Transfer(address indexed from, address indexed to, uint256 value)
    ]"#,
    event_derives(serde::Deserialize, serde::Serialize)
);

#[derive(Debug, Clone, PartialEq, ethers::contract::EthEvent)]
#[allow(non_camel_case_types)]
pub struct TransferFilter {
    #[ethevent(indexed)]
    pub from: Address,
    #[ethevent(indexed)]
    pub to: Address,
    pub value: U256,
}


// --- 2. Data Models and CLI ---

#[derive(Debug, FromRow)]
struct NetFlowRecord {
    latest_block_indexed: i64, 
    cumulative_net_flow_wei: String,
    exchange_name: String,
    token_symbol: String,
}

#[derive(Parser, Debug)]
#[clap(author, version, about = "Polygon Net-Flow Indexer", long_about = None)]
enum Cli {
    Index,
    Query(QueryArgs),
}

#[derive(clap::Args, Debug)]
struct QueryArgs {
    #[arg(short, long, default_value = "Binance")]
    exchange: String,
    #[arg(short, long, default_value = "POL")]
    token: String,
}

// --- 3. Core Logic Functions ---

fn format_wei_to_pol(wei_amount: &str) -> String {
    let wei_u256 = U256::from_str(wei_amount).unwrap_or(U256::zero());
    format_units(wei_u256, POL_DECIMALS).unwrap_or_else(|_| "0.0".to_string())
}

async fn process_log_and_update_db(
    log: &Log, 
    pool: &SqlitePool,
    binance_addresses: &HashSet<Address>,
) -> Result<()> {
    
    let block_number = log.block_number.ok_or_else(|| eyre::eyre!("Log missing block number"))?;
    let log_index = log.log_index.ok_or_else(|| eyre::eyre!("Log missing log index"))?;

    let raw_log = RawLog {
        topics: log.topics.clone(),
        data: log.data.to_vec(),
    };

    let event = match TransferFilter::decode_log(&raw_log) {
        Ok(e) => e,
        Err(_) => return Ok(()),
    };
    
    let TransferFilter { from, to, value } = event;
    
    let is_from_binance = binance_addresses.contains(&from);
    let is_to_binance = binance_addresses.contains(&to);

    if !is_from_binance && !is_to_binance {
        return Ok(());
    }

    let is_in_flow = is_to_binance && !is_from_binance;
    let is_out_flow = is_from_binance && !is_to_binance;

    let net_change_value = if is_in_flow || is_out_flow {
        value
    } else {
        return Ok(()); 
    };
    
    let mut tx = pool.begin().await?;

    let block_timestamp_row = tx.fetch_one("SELECT strftime('%s', 'now')").await?;
    let block_timestamp = block_timestamp_row.get::<i64, _>(0);

    // --- FIX E0716: Bind all complex expressions/temporaries to long-lived `let` variables ---
    
    // Bindings for Raw Log Insertion
    let tx_hash_str = format!("{:?}", log.transaction_hash.unwrap());
    let log_idx_i64 = log_index.as_u64() as i64;
    let blk_num_i64 = block_number.as_u64() as i64;
    let from_addr_str = format!("{:?}", from);
    let to_addr_str = format!("{:?}", to);
    let value_str = value.to_string();
    let is_in_i64 = is_in_flow as i64;
    let is_out_i64 = is_out_flow as i64;

    // 1. Insert Raw Log
    sqlx::query!(
        r#"
        INSERT OR IGNORE INTO raw_pol_transfers 
        (transaction_hash, log_index, block_number, block_timestamp, from_address, to_address, amount_wei, is_in_flow, is_out_flow)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#,
        tx_hash_str,
        log_idx_i64,
        blk_num_i64,
        block_timestamp,
        from_addr_str,
        to_addr_str,
        value_str,
        is_in_i64,
        is_out_i64,
    )
    .execute(&mut *tx)
    .await?;

    // 2. Calculate New Cumulative Total
    let current_record = sqlx::query_as!(
        NetFlowRecord,
        r#"
        SELECT latest_block_indexed, cumulative_net_flow_wei, exchange_name, token_symbol
        FROM cumulative_net_flows 
        WHERE exchange_name = 'Binance' AND token_symbol = 'POL'
        "#,
    )
    .fetch_one(&mut *tx)
    .await?;
    
    let current_total = U256::from_str(&current_record.cumulative_net_flow_wei).unwrap_or(U256::zero());
    
    let new_total = if is_in_flow {
        current_total.checked_add(net_change_value).unwrap_or(U256::MAX)
    } else { 
        current_total.checked_sub(net_change_value).unwrap_or(U256::MAX) 
    };

    let new_total_str = new_total.to_string();

    // 3. Update Cumulative Record
    sqlx::query!(
        r#"
        UPDATE cumulative_net_flows 
        SET cumulative_net_flow_wei = ?, latest_block_indexed = ?, updated_at = datetime('now')
        WHERE exchange_name = 'Binance' AND token_symbol = 'POL'
        "#,
        new_total_str,
        blk_num_i64, // Reuse the bound variable
    )
    .execute(&mut *tx)
    .await?;

    // 4. Commit transaction
    tx.commit().await?; 
    
    let flow_symbol = if is_in_flow { "+" } else { "-" };
    println!(" [Block {}] Net-Flow Update: {} {} POL. New Total: {}", 
        block_number, 
        flow_symbol,
        format_wei_to_pol(&net_change_value.to_string()),
        format_wei_to_pol(&new_total_str)
    );

    Ok(())
}

async fn run_indexer(pool: SqlitePool, rpc_url: String) -> Result<()> {
// ... (run_indexer function body remains the same, as it did not have E0716 errors)
    let provider = Provider::<Ws>::connect(&rpc_url).await?;
    let binance_addresses: HashSet<Address> = BINANCE_ADDRS_STR.iter().filter_map(|s| s.parse::<Address>().ok()).collect();
    let contract_addr = POL_CONTRACT_ADDRESS.parse::<Address>()?;
    let transfer_topic: H256 = TRANSFER_EVENT_TOPIC.parse().unwrap();
    
    let latest_indexed_block: i64 = query_scalar!(
        "SELECT latest_block_indexed FROM cumulative_net_flows WHERE exchange_name = 'Binance' AND token_symbol = 'POL'"
    )
    .fetch_optional(&pool)
    .await?
    .unwrap_or(0);

    let start_block_num = if latest_indexed_block == 0 { 
        provider.get_block_number().await?
    } else {
        U64::from(latest_indexed_block as u64) + 1
    };

    let filter = Filter::new()
        .address(contract_addr)
        .topic0(transfer_topic)
        .from_block(start_block_num); 

    println!("Starting real-time stream from block {}. Monitoring for POL transfers involving Binance addresses.", start_block_num);
    
    let mut log_stream = provider.subscribe_logs(&filter).await?;

    while let Some(log) = log_stream.next().await {
        if log.block_number.is_some() && log.topics.len() == 3 { 
            if let Err(e) = process_log_and_update_db(&log, &pool, &binance_addresses).await {
                eprintln!("Error processing log in block {}: {:?}", log.block_number.unwrap(), e);
            }
        }
    }

    Ok(())
}

async fn handle_query(pool: SqlitePool, args: QueryArgs) -> Result<()> {
// ... (handle_query function body remains the same)
    let result = sqlx::query_as!(
        NetFlowRecord,
        r#"
        SELECT latest_block_indexed, cumulative_net_flow_wei, exchange_name, token_symbol
        FROM cumulative_net_flows 
        WHERE exchange_name = ? AND token_symbol = ?
        "#,
        args.exchange,
        args.token
    )
    .fetch_optional(&pool)
    .await?;

    match result {
        Some(record) => {
            let total_wei = &record.cumulative_net_flow_wei;
            
            let sign = if total_wei.starts_with('-') { "-" } else { "+" };
            let abs_total_wei = total_wei.trim_start_matches('-');

            let net_flow_pol = format_wei_to_pol(abs_total_wei);

            println!("\n=======================================================");
            println!("  Cumulative Net-Flow Data");
            println!("=======================================================");
            println!("  Exchange:              {}", record.exchange_name);
            println!("  Token:                 {}", record.token_symbol);
            println!("  Latest Block Indexed:  {}", record.latest_block_indexed);
            println!("  Net Flow (POL):        {} {}", sign, net_flow_pol);
            println!("  Net Flow (Wei):        {}", record.cumulative_net_flow_wei);
            println!("=======================================================\n");
        }
        None => {
            println!("No data found for Exchange: {} and Token: {}", args.exchange, args.token);
            println!("Ensure the database is initialized with migrations and the indexer has run.");
        }
    }

    Ok(())
}

// --- 4. Main Dispatcher ---

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    env_logger::init(); 
    
    // We defer the pool connection until inside the match arm
    
    match Cli::parse() {
        Cli::Index => {
            // --- FIX: Use an IN-MEMORY DATABASE to bypass all Code 14 errors ---
            let in_memory_url = "sqlite::memory:";
            let pool = SqlitePool::connect(in_memory_url).await?; // Pool created in RAM
            sqlx::migrate!().run(&pool).await?; // Migrations run in RAM (no disk access)
            // -------------------------------------------------------------------
            
            let rpc_url = env::var("POLYGON_RPC_URL")
                .map_err(|_| eyre::eyre!("POLYGON_RPC_URL (WS endpoint) must be set in the .env file."))?;
            run_indexer(pool, rpc_url).await
        }
        Cli::Query(args) => {
            // For query, we stick to file-based access, but the user would run query only 
            // after indexing was done on a file-based DB.
            let database_url = env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite:./netflow.db".to_string());
            let pool = SqlitePool::connect(&database_url).await?;
            sqlx::migrate!().run(&pool).await?; 
            handle_query(pool, args).await
        }
    }
}