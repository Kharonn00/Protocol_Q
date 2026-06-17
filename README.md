# Kalshi Quantitative Trading Engine (KQTE)

## Executive Summary
The Kalshi Quantitative Trading Engine is a high-frequency, fully asynchronous algorithmic trading daemon designed to execute asymmetric taker strategies on Kalshi's 15-minute Cryptocurrency Options markets. Built on absolute Zero-Trust security principles and strict $O(1)$ memory bounds, the engine aggregates Binance futures liquidation streams and Coinbase spot transaction feeds to perfectly time and snipe explosive macro-level breakouts in the Kalshi options chain.

Currently deployed as an optimized, multi-stage AWS ECS Fargate service, the daemon operates two distinct quantitative sniper models: a predictive **Binance Liquidation Sniper** and a rolling **Z-Score Momentum Breakout Sniper**, completely ignoring regular algorithmic chop to capture massive, high-probability directional wicks.

---

## 🏛️ System Architecture

* **Runtime Environment:** Python 3.12-slim and Rust (ABI-aligned multi-stage compilation).
* **Rust PyO3 Extension:** Native compiled `kalshi_bot` library provides `FastIndicators` for ultra-low latency, GIL-free technical calculations (Bollinger Bands, EMA, RSI, and running Z-Scores).
* **Orchestration:** AWS Elastic Container Service (ECS) with Fargate (`readonlyRootFilesystem: true`).
* **State Management:** Fully stateless, ephemeral execution engine (Twelve-Factor App Compliant).
* **Secret Management:** AWS Secrets Manager via `boto3` (No local key storage).
* **Data Ingestion:** Multiplexed secure WebSockets (Coinbase Spot Tape `ticker`, Binance Futures `!forceOrder@arr`).

---

## 📈 Quantitative Strategies

### 1. The Binance Liquidation Sniper (Macro Breakouts)
Waits exclusively for massive directional futures liquidations to capture explosive options chain momentum.
* Monitors the Binance USD-M Futures WebSocket for forced liquidations exceeding asset-specific thresholds (e.g., $\$1.5\text{M}$ for BTC, $\$750\text{k}$ for ETH, $\$300\text{k}$ for DOGE, and $\$100\text{k}$ for HYPE and SOL).
* Validates liquidation events using strict notional bounds ($\$10$ to $\$1\text{B}$) to prevent logic-level overflow vulnerabilities.
* When a high-impact macro-liquidation occurs, the bot immediately snipes predictive momentum contracts on the Kalshi chain in the direction of the cascade.

### 2. Z-Score Momentum Breakout Sniper (Spot Volatility)
Uses high-precision rolling Z-Scores to snipe explosive breakout wicks on spot exchanges.
* Evaluates ticks compiled in real-time by the Rust extension to monitor standard deviation changes.
* Snipes momentum contracts during the **Golden Window** (8 to 3 minutes remaining in the 15-minute event) when the spot price Z-Score diverges past $\pm 2.5$.

### 3. Macroeconomic Circuit Breaker (The Steamroller Defense)
Fundamentally prevents the bot from trading during exogenous regime shifts.
* Fetches global economic schedules (e.g., CPI, FOMC, Fed Funds Rate) via a static `JSON` economic calendar feed.
* Utilizes a highly optimized $O(1)$ parsing loop to discard international events, tracking strictly high-impact `USD` shocks with safety lookahead bounds (max 7 days).
* Engages a mathematical temporal deadlock (a configurable 30-minute lockout window before/after the event) to pause trading, preventing the bot from "picking up pennies in front of a steamroller" during scheduled macro volatility.

### 4. Strict Temporal Risk Gates
The bot enforces a highly specific mathematical "kill box" on the 15-minute Kalshi event timeline:
* **Phase 1: Strike Window (Min 0-12 / 15 to 3 minutes left):** Active trade execution window. Ticks are ingested to warm up the statistical baseline (minimum 1000 ticks required).
* **Phase 2: Illiquidity Lockdown (Min 12-15 / 180 to 0 seconds left):** Absolute lock. Spreads widen toxically; the bot deadlocks itself to prevent late-stage adverse selection.

---

## 🛡️ Execution & Risk Controls

* **Multi-Asset Ingestion Support**: Actively trades five cryptocurrency pairs: **BTC-USD**, **HYPE-USD**, **SOL-USD**, **ETH-USD**, and **DOGE-USD**.
* **Strict Capital Allocation**: Hard limits all executions to exactly 100 contracts (approx ~$100 max exposure per event) to securely manage drawdown risk during hyper-volatile liquidation wicks.
* **Probability Floor**: Rejects any trade execution where `best_ask < 0.15` to avoid purchasing un-winnable low-probability options near expiration.
* **Dynamic Take-Profit (Laddered Exit)**: Automatically distributes exit orders across 3 distinct tranches:
  * **Tranche 1 (Conservative 40%):** Locked at a strictly floored **50% ROI**.
  * **Tranche 2 (Dynamic 35%):** Scales linearly between **50% and 85% ROI** depending on time remaining to event expiration.
  * **Tranche 3 (Moonshot 25%):** Locked at an aggressive **95% ROI** maximum limit.
  * **Mathematical Protection:** Employs `ROUND_UP` floating precision math and strict monotonic step constraints to ensure the exit ladder never mathematically collapses onto itself, even in illiquid conditions.
* **Trailing Order Cancellation**: Upon event expiration timeout, all resting take-profit orders are force-cancelled with a 5-step retry loop to reconcile exact filled quantities.

---

## 🔒 Security Posture & Zero-Trust Architecture

The system is engineered assuming a strictly hostile network and execution environment:
* **Exception Safety (Zero-Leak Execution)**: The `execute_and_hold_entry` setup block encloses all variables inside the primary `try:` block. Any setup exceptions trigger balance rollbacks (`except`) and concurrency slot release (`finally`), preventing slot and capital leakage.
* **Double-Checked Locking (DCL) Concurrency Shield**: Checks position limit bounds locklessly, yields to retrieve market prices, and then validates state inside a synchronous `balance_lock` to stop duplicate execution races.
* **Liskov Substitution Interface Alignment**: Method signature parameters for `get_order_details` are standardized using generic `**kwargs` across simulated, paper, and live brokers, eliminating runtime crashes.
* **Cancel-Fill Race Condition Shield**: Implements cryptographic-grade validation loops during order cancellations. If a `cancel_order` request is rejected or is unconfirmed, the engine verifies details up to 5 times with jittered backoff.
* **O(1) Orphan Fill Websocket Cache**: An advanced List-based array mapping intercepts and buffers WebSocket fills that resolve faster than the HTTP REST API edge router. By looping and enqueuing these partial fills instantaneously upon `execute_trade` resolution, the engine completely seals against TOCTOU dropped-fill phantom positions without violating memory bounds.
* **Defensive Data Ingestion (Anti-Compression Bombs)**: External API fetches enforce rigid `Content-Length` bounds ($< 512$ KB) and exact MIME-type header validation (`application/json`) prior to deserialization, preventing Zip Bombs, HTML parsing crashes, and malicious control-character log injections.
* **Heap Memory Cryptographic Hardening**: API credentials and keys are scrubbed immediately. String references are deleted (`del key_id`, `del private_key`), mutable buffers are zeroed using `ctypes.memset` in Secrets Manager parsing, `LiveKalshiBroker.close()` zeroes keys on termination, and a forced `gc.collect()` sweep wipes cleartext keys from memory page files.
* **Zero-Trust Path Traversal Mitigation**: Every dynamic API variable is URL-encoded using `urllib.parse.quote(..., safe='')`, neutralizing parameter pollution and path traversal attacks.
* **Identity Transparency**: Conforms strictly to third-party Terms of Service by passing honest User-Agent HTTP identifiers (`KalshiQuantEngine/1.0`), preventing WAF circumvention bans.

---

## 🧠 Memory & Concurrency Optimization

Engineered to run infinitely without Out-Of-Memory (OOM) degradation or Garbage Collection (GC) stutter:
* **Garbage Collection (GC) Freezing:** By deliberately decoupling from the Coinbase Level 2 WebSocket firehose and offloading indicator calculations to Rust, the engine prevents Python GC "Stop-The-World" pauses.
* **Active Cooldown Guards:** Cooldown periods are locked during yields to prevent concurrent tick duplicate executions, and are immediately reset if a trade fails or returns early to ensure indicator calculations continue without interruption.
* **Asynchronous Backpressure:** `asyncio.Queue(maxsize=...)` implements hard network boundary limits. In the event of a flash-crash, the system drops stale queue frames rather than allocating unmanageable heap pressure.
* **Resilient Outage Backoff:** WebSocket consumers implement a truncated exponential backoff with random uniform jitter, protecting the container from CPU thrashing, log bloat, and IP rate-bans during prolonged exchange outages.

---

## 🚀 Deployment Operations

This project utilizes an AWS ECR/ECS automated pipeline.

```bash
# 1. Setup virtual env and compile Rust PyO3 indicators
python -m venv venv
$env:VIRTUAL_ENV="C:\Users\A2\OneDrive\Documents\Python Bots\kalshi_bot\venv"
pip install -r requirements.txt maturin
maturin develop --release

# 2. Authenticate to AWS ECR via secure STS token piping
aws ecr get-login-password --region <REGION> | docker login --username AWS --password-stdin <ACCOUNT_ID>.dkr.ecr.<REGION>.amazonaws.com

# 3. Build the explicit cross-architecture Docker Image
docker build --platform linux/amd64 -t kalshi-quant-engine .

# 4. Tag and Push
docker tag kalshi-quant-engine:latest <ACCOUNT_ID>.dkr.ecr.<REGION>.amazonaws.com/kalshi-quant-engine:latest
docker push <ACCOUNT_ID>.dkr.ecr.<REGION>.amazonaws.com/kalshi-quant-engine:latest

# 5. Trigger ECS Zero-Downtime Rolling Deployment
aws ecs update-service --cluster QuantCluster --service kalshi-bot-service --force-new-deployment
```

---

## 🔮 Roadmap / Future Implementation

* **Phase 10: PostgreSQL Telemetry Sink:** Stream CloudWatch fill/execution logs into TimescaleDB/PostgreSQL to perform long-term Expected Value (EV) backtesting analysis on Z-Score fade threshold optimizations.
* **Phase 11: Out-of-Band Orderbook Imbalance (OBI):** If future statistical strategies dictate the need for resting L2 Depth tracking, engineer a native Cython or Rust C-Extension to manage Level 2 memory allocations entirely outside of the Python Global Interpreter Lock (GIL) and Garbage Collector.
