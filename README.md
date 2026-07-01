# Kalshi Quantitative Trading Engine (KQTE)

## Executive Summary
The Kalshi Quantitative Trading Engine is a high-frequency, fully asynchronous algorithmic trading daemon designed to execute asymmetric taker strategies on Kalshi's 15-minute Cryptocurrency Options markets. Built on absolute Zero-Trust security principles and strict $O(1)$ memory bounds, the engine aggregates Binance futures liquidation streams and Coinbase spot transaction feeds to perfectly time and snipe explosive macro-level breakouts in the Kalshi options chain.

Currently deployed as an optimized, multi-stage AWS ECS Fargate service, the daemon operates the **Binance Liquidation Sniper** (Strategy 1) as its primary execution vehicle. Strategy 2 (Z-Score Sniper) and Strategy 3 (DOGE Theta Harvester) are **currently commented out / deactivated** to focus capital allocation strictly on high-leverage directional breakouts.

---

## 🏛️ System Architecture

* **Runtime Environment:** Python 3.12-slim and Rust (ABI-aligned multi-stage compilation).
* **Rust PyO3 Extension:** Native compiled `kalshi_bot` library provides `FastIndicators` for ultra-low latency, GIL-free technical calculations (Bollinger Bands, EMA, RSI, and running Z-Scores).
* **Orchestration:** AWS Elastic Container Service (ECS) with Fargate (`readonlyRootFilesystem: true`).
* **State Management:** Fully stateless, ephemeral execution engine (Twelve-Factor App Compliant).
* **Secret Management:** AWS Secrets Manager via `boto3` (No local key storage).
* **Data Ingestion:** Multiplexed secure WebSockets (Coinbase Spot Tape `ticker`, Binance Futures `!forceOrder@arr`).
* **Health Check & DoS Rate Limiting:** Exposed on TCP Port `8080` with a per-source-IP rate limiter (bypassed for internal loopback and private VPC IP addresses to prevent orchestrator termination loops).

---

## 📈 Quantitative Strategies

### 1. The Binance Liquidation Sniper (Macro Breakouts) - [ACTIVE]
Waits exclusively for massive directional futures liquidations to capture explosive options chain momentum.
* **Asset Support**: Tracks **BTC-USD** ($1.5M threshold), **ETH-USD** ($750k threshold), **SOL-USD** ($300k threshold), **DOGE-USD** ($300k threshold), and **HYPE-USD** ($100k threshold).
* **Time-Window Gate**: Restricted execution to a strict window of **1.5 to 8 minutes remaining** (`90.0 <= seconds_left <= 480.0`) to avoid early-event chop and late-event illiquidity.
* **Spot-to-Strike Distance Gate**: Restricts entries to Out-of-the-Money (OTM) options only if the spot-to-strike distance is within $1.5 \times \text{Standard Deviation}$ ($\sigma$) derived from Bollinger Bands, dynamically scaling the gate with active market volatility.
* **Pricing Consistency Gate**: Restricts OTM entries to a maximum purchase price of `$0.55` to prevent buying stale or illiquid markup contracts.
* **Fallback Ingestion**: If Coinbase ticks are missing (e.g. for `HYPE-USD`), the engine utilizes the Binance liquidation event price as a spot proxy to feed standard technical indicators and safety gates.
* **Asset Performance Auto-Throttle**: Queries [PerformanceTracker](file:///C:/Users/A2/OneDrive/Documents/Python Bots/kalshi_bot/kalshi_main.py#L278) to throttle trades dynamically if the rolling outcome history (last 20 trades per asset/hour in a `deque`) yields a win rate $\le 35\%$ over at least 5 samples.

### 2. Z-Score Momentum Breakout / Mean Reversion Sniper - [DEACTIVATED]
* *Status*: Commented out / disabled to prioritize liquidation breakout edge.

### 3. DOGE Theta Harvester - [DEACTIVATED]
* *Status*: Commented out / disabled due to asymmetric risk-expectancy profiles.

### 4. Macroeconomic Circuit Breaker (The Steamroller Defense) - [ACTIVE]
Fundamentally prevents the bot from trading during exogenous regime shifts.
* Fetches global economic schedules (e.g., CPI, FOMC, Fed Funds Rate) via a static `JSON` economic calendar feed.
* Utilizes a highly optimized $O(1)$ parsing loop to discard international events, tracking strictly high-impact `USD` shocks with safety lookahead bounds (max 7 days) and a strict 512KB compression bomb check.
* Engages a temporal lockout (a configurable 30-minute window before/after the event) to pause trading, preventing adverse selection during scheduled macro volatility.

---

## 🛡️ Execution & Risk Controls

* **Strict Capital Allocation**: Hard limits all executions to a trade budget threshold (approx ~$100 max exposure per event) to securely manage drawdown risk.
* **Rolling High-Water Mark Drawdown Circuit Breaker**: Measured as `(peak_balance - portfolio_val) / peak_balance` (quant peak-to-trough standard) rather than a static anchor. Tracks the equity peak monotonically inside the `balance_lock` and triggers an immediate halt if drawdown reaches `25%` (`DRAWDOWN_LIMIT_PCT`).
* **Take-Profit (Laddered Exit)**: Automatically distributes exit orders across 3 distinct tranches:
  * **Tranche 1 (Conservative 40%):** Target **50% ROI**.
  * **Tranche 2 (Dynamic 35%):** Scales between **50% and 85% ROI** based on time remaining to event expiration.
  * **Tranche 3 (Moonshot 25%):** Target **95% ROI** maximum limit.
  * **Hold-to-Settle Optimization**: If **$\ge 2$ of 3** Take-Profit tranches cap at the maximum `$0.99` limit, the engine bypasses TP routing and falls back to Hold-to-Settle, minimizing exchange maker/taker fee overhead.
* **Trailing Order Cancellation & Hold-To-Settle**: Expiries and buzzer-beater positions dynamically evaluate the final bid-ask price or spot vs. strike price at expiration to resolve payout delta entries accurately.

---

## 🔒 Security Posture & Zero-Trust Architecture

The system is engineered assuming a strictly hostile network and execution environment:
* **WebSockets SSRF/DNS Rebinding Defense**: All incoming WS connections check target URLs against [is_safe_destination_async](file:///C:/Users/A2/OneDrive/Documents/Python Bots/kalshi_bot/kalshi_main.py#L387) pre-flight to prevent connections resolving to private loopback or internal metadata space.
* **Health Check Rate Limiting**: Employs a per-source-IP sliding window rate limiter on the health server to protect the shared `asyncio` event loop against external DoS without interfering with orchestrator loopback health checks.
* **Locked Capital Telemetry Filtering**: [get_locked_capital](file:///C:/Users/A2/OneDrive/Documents/Python Bots/kalshi_bot/kalshi_main.py#L828) aggregates only resting orders with `action == "buy"`, preventing resting sell/TP orders from inflating telemetry logs.
* **Paper Trading Lock Boundaries**: Employs a dedicated `paper_orders_lock` within [LiveKalshiBroker](file:///C:/Users/A2/OneDrive/Documents/Python Bots/kalshi_bot/kalshi_main.py#L744) to guarantee thread-safe mutations of paper balance and paper order logs across async yields.
* **Exception Safety (Zero-Leak Execution)**: Concurrency slots and balance tracking are managed in strict `finally` blocks, releasing capital slots instantly upon task cancellation.
* **Double-Checked Locking (DCL) Concurrency Shield**: Checks position bounds locklessly, yields to retrieve market prices, and then validates state inside a synchronous `balance_lock` to stop duplicate execution races.
* **Heap Memory Cryptographic Hardening**: Overwrites immutable string dictionary entries (`PRIVATE_KEY`, `KEY_ID`) inside Secrets Manager decoding, zeroes mutable `bytearray` buffers with `ctypes.memset`, and performs double `gc.collect()` passes on shutdown to eliminate OpenSSL key residency.

---

## 🧠 Memory & Concurrency Optimization

Engineered to run infinitely without Out-Of-Memory (OOM) degradation or Garbage Collection (GC) stutter:
* **Garbage Collection (GC) Freezing:** Decouples from heavy Level 2 feeds and offloads hot-path indicators (fast Bollinger Bands, RSI, Z-Scores) to compiled Rust `FastIndicators` structures to prevent Python GC "Stop-The-World" latency jitter.
* **Active Cooldown Guards:** Cooldowns are locked during yields to prevent tick duplication, and are immediately reset if a trade returns early.
* **Asynchronous Backpressure:** `asyncio.Queue` limits backpressure by dropping stale frames during flash-crash scenarios rather than allocating unmanageable heap buffers.

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
