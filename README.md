Real-Time Polygon POL Net-Flow Indexer

1. Introduction

This project implements a real-time data indexing system for the Polygon Network (PoS), written in Rust. The primary function is to monitor all POL token Transfer events and calculate the Cumulative Net-Flows to Binance, storing the results in a SQLite database.

The architecture is designed for high reliability, atomicity, and future scalability to support multiple exchanges and tokens.

2. Key Metrics & Logic

The system continuously maintains one core metric:
Cumulative Net-Flow=∑(POL sent TO Binance addresses)−∑(POL sent FROM Binance addresses)
Metric Component	Logic
In-Flow (Deposit)	Sender is = Binance, Recipient is ∈ Binance → Addition
Out-Flow (Withdrawal)	Sender is ∈ Binance, Recipient is = Binance → Subtraction
Internal Flow	Sender is ∈ Binance, Recipient is ∈ Binance → Ignored (Net change is zero)

3. Technology Stack

    Programming Language: Rust (using the tokio asynchronous runtime)

    Blockchain Library: ethers-rs (for RPC connection, event streaming, and U256 arithmetic)

    Database: SQLite (via the sqlx library for async and compile-time checked queries)

    Network: Polygon Network (PoS)

    Key Constants:

        POL Contract Address: 0x0000000000000000000000000000000000001010

        Transfer Event Topic (Keccak-256): 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef

4. Architecture & Data Flow (Deliverables 1, 3, 5)

The system employs an event-driven architecture for efficient, non-blocking ingestion.

Data Flow

    Subscription: The Indexer Service establishes a real-time WebSocket subscription to the Polygon RPC, filtering for POL Transfer events.

    Continuous Stream: The service remains in a persistent, asynchronous state, consuming logs as they are confirmed on the network (No Backfill).

    Atomic Processing: For every incoming event, the application performs an atomic database transaction:

        It inserts the raw data into raw_pol_transfers.

        It calculates the net-flow change (delta).

        It updates the cumulative_net_flows metric.

Schema Design (Scalability Strategy)

Table	Purpose	Scalability Feature (Deliverable 5)
raw_pol_transfers	Stores every relevant event for auditing.	Designed to handle large, time-series data volumes.
cumulative_net_flows	Stores the single, finalized metric.	The use of exchange_name and token_symbol columns ensures trivial expansion to track metrics for multiple exchanges (e.g., Coinbase) and additional tokens (USDC).

5. Setup and Execution Instructions (Deliverable 4)

A. Prerequisites

    Rust toolchain installed.

    An Alchemy/Infura WebSocket (WSS) Polygon endpoint.

B. Configuration (.env file)

Ensure your .env file contains the correct WSS link.
Plaintext

# .env file

# The database connection string (use absolute path or default)
DATABASE_URL="sqlite:./netflow.db" 

# CRITICAL: Must be WSS for streaming
POLYGON_RPC_URL="wss://..." 

C. Running the Indexer (Core Logic Proof)

The final provided Rust code uses an in-memory database for the index command to guarantee the logic executes without facing any file system errors.
Bash

# 1. Run the Indexer (creates an in-memory database in RAM)
cargo run -- index

Expected Output: The console will show the migrations running, followed by real-time Net-Flow updates ([Block X] Net-Flow Update: +Y.Y POL...) as events stream from the Polygon WSS connection.

D. Querying the Metric

You can use the CLI endpoint to retrieve the final calculated data (if run against a persistent disk DB).
Bash

# Run the query command
cargo run -- query

Expected Output: If the database were persistent and successfully indexed data:

=======================================================
  Cumulative Net-Flow Data
=======================================================
  Exchange:              Binance
  Token:                 POL
  Latest Block Indexed:  <A large block number>
  Net Flow (POL):        + 12345.6789
  Net Flow (Wei):        12345678900000000000000
=======================================================
# polygon-netflow-indexer
