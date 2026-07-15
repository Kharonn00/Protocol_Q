# Kalshi Quantitative Trading Engine (KQTE)

## Executive Summary
The Kalshi Quantitative Trading Engine is a high-frequency, fully asynchronous algorithmic trading daemon designed to execute asymmetric taker strategies on Kalshi's 15-minute Cryptocurrency Options markets. Built on absolute Zero-Trust security principles and strict $O(1)$ memory bounds, the engine aggregates Binance, Bybit, and Hyperliquid futures liquidation streams alongside Coinbase spot transaction feeds to perfectly time and snipe explosive macro-level breakouts in the Kalshi options chain.

Currently deployed as an optimized, multi-stage AWS ECS Fargate service, the daemon operates the **Liquidation Sniper** (Strategy 1) as its primary execution vehicle. Strategy 2 (Z-Score Sniper) and Strategy 3 (DOGE Theta Harvester) are **currently commented out / deactivated** to focus capital allocation strictly on high-leverage directional breakouts.

---

## 🏛️ System Architecture

* **Runtime Environment:** Python 3.12-slim and Rust (ABI-aligned multi-stage compilation).
* **Rust PyO3 Extension:** Native compiled `kalshi_bot` library provides `FastIndicators` for ultra-low latency, GIL-free technical calculations (Bollinger Bands, EMA, RSI, and running Z-Scores).
* **Orchestration:** AWS Elastic Container Service (ECS) with Fargate (`readonlyRootFilesystem: true`).
* **State Management:** Fully stateless, ephemeral execution engine (Twelve-Factor App Compliant).
* **Secret Management:** AWS Secrets Manager via `boto3` (No local key storage).
* **Data Ingestion:** Multiplexed secure WebSockets:
  * **Coinbase**: Spot transaction tape (`ticker`).
  * **Binance**: Futures forced liquidation feed (`!forceOrder@arr`).
  * **Bybit**: Linear futures public liquidation feed (`allLiquidation.{symbol}`).
  * **Hyperliquid**: On-chain trades feed filtered for liquidation events (`trades`).
* **Health Check & DoS Rate Limiting:** Exposed on TCP Port `8080` with a per-source-IP rate limiter (bypassed for internal loopback and private VPC IP addresses to prevent orchestrator termination loops).

---

## 📈 Quantitative Strategies

### 1. The Liquidation Sniper (Multi-Exchange Macro Breakouts) - [ACTIVE]
Waits exclusively for massive directional futures liquidations to capture explosive options chain momentum.
* **Asset Support**: Tracks **BTC-USD**, **ETH-USD**, **SOL-USD**, **DOGE-USD**, and **HYPE-USD**.
* **Exchange Streams**: Normalizes and ingests three distinct, public, non-authenticated real-time WebSockets:
  * **Binance**: USDS-M perpetual liquidations.
  * **Bybit**: V5 linear perpetual liquidations (mapping Bybit position side to order direction).
  * **Hyperliquid**: Decentralized perpetual fills (detecting on-chain liquidation sub-objects).
* **Dual-Regime Time-Window Gates**: Operating under two contiguous, non-overlapping temporal windows across the 15-minute event cycle:
  * **Early Window (15m to 10m remaining - Mean Reversion Mode)**: Assumes early-stage wicks will pull back. Reverts the trade direction (buys `NO` on short liquidations, `YES` on long liquidations) and applies strict trend shield and Bollinger Band boundaries.
  * **Golden Window (10m to 1.5m remaining - Momentum Breakout Mode)**: Standard directional sniping (buys `YES` on short liquidations, `NO` on long liquidations) to catch breakout runs.
* **Spot-to-Strike Distance Gate**: Restricts entries to Out-of-the-Money (OTM) options only if the spot-to-strike distance is within $1.5 \times \text{Standard Deviation}$ ($\sigma$) derived from Bollinger Bands, dynamically scaling the gate with active market volatility.
* **Pricing Consistency Gate**: Restricts OTM entries to a maximum purchase price of `$0.55` to prevent buying stale or illiquid markup contracts.
* **Fallback Ingestion**: If Coinbase ticks are missing (e.g. for `HYPE-USD`), the engine utilizes the Binance/Bybit/Hyperliquid liquidation event price as a spot proxy to feed indicators and safety gates.
* **Asset Performance Auto-Throttle**: Queries [PerformanceTracker](kalshi_main.py#L278) to throttle trades dynamically if the rolling outcome history (last 20 trades per asset/hour in a `deque`) yields a win rate $\le 35\%$ over at least 5 samples.

### 2. Z-Score Momentum Breakout / Mean Reversion Sniper - [DEACTIVATED]
* *Status*: Commented out / disabled to prioritize liquidation breakout edge.

### 3. DOGE Theta Harvester - [DEACTIVATED]
* *Status*: Commented out / disabled due to asymmetric risk-expectancy profiles.

### 4. Macroeconomic Circuit Breaker (The Steamroller Defense) - [ACTIVE]
Fundamentally prevents the bot from trading during exogenous regime shifts.
* Fetches economic releases schedule schedule via a static JSON feed.
* Temporal lockout (30-minute window before/after the event) blocks trading, preventing adverse selection during high-impact USD economic events.

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
* **WebSockets SSRF/DNS Rebinding Defense**: All incoming WS connections check target URLs against [is_safe_destination_async](kalshi_main.py#L387) pre-flight to prevent connections resolving to private loopback or internal metadata space. Applies to Coinbase, Binance, Bybit, and Hyperliquid client handshakes.
* **HTTP Redirect SSRF Prevention**: Forces `allow_redirects=False` on all external HTTP requests inside candle synchronization loops to prevent attackers from bypassing DNS resolver gates via HTTP 3xx redirects to local metadata.
* **Trusted Proxy Client IP Resolution**: Health endpoint resolves `X-Forwarded-For` from right to left (to prevent header spoofing) strictly if the immediate connecting IP belongs to a private network (e.g. AWS ALB). Direct connections fallback to the socket IP.
* **Health Check Rate Limiting**: Employs a per-source-IP sliding window rate limiter on the health server, with sliding eviction rather than full resets to prevent cache poisoning, and loopback/private connection bypasses.
* **Locked Capital Telemetry Filtering**: [get_locked_capital](kalshi_main.py#L828) aggregates only resting orders with `action == "buy"`, preventing resting sell/TP orders from inflating telemetry logs.
* **Paper Trading Lock Boundaries**: Employs a dedicated `paper_orders_lock` within [LiveKalshiBroker](kalshi_main.py#L744) to guarantee thread-safe mutations of paper balance and paper order logs across async yields.
* **Shielded Task Cancellation Cleanups (Zero-Leak Execution)**: Cleanup routines (order cancellation and final balance/position reconciliation) are wrapped in background coroutines shielded via `_safe_shield`, ensuring they execute to completion in the event loop even if the parent task is aborted or timed out.
* **Double-Checked Locking (DCL) Concurrency Shield**: Checks position bounds locklessly, yields to retrieve market prices, and then validates state inside a synchronous `balance_lock` to stop duplicate execution races.
* **Heap Memory Cryptographic Hardening**: Overwrites immutable string dictionary entries (`SecretString`, `PRIVATE_KEY`, `KEY_ID`) inside Secrets Manager decoding, zeroes mutable `bytearray` buffers with `ctypes.memset`, and performs double `gc.collect()` passes on shutdown to eliminate OpenSSL key residency.

## 🧠 Memory & Concurrency Optimization

Engineered to run infinitely without Out-Of-Memory (OOM) degradation or Garbage Collection (GC) stutter:
* **Head-of-Line (HoL) Blocking Resolution**: Worker loops process incoming queue messages by spawning candidate breakout evaluations as independent, concurrent task wrappers (`asyncio.create_task`) rather than sequentially awaiting HTTP REST yields. This allows the worker loop to continuously drain the ingestion queue in microseconds.
* **Semaphore Queue Backpressure**: The worker loop yields on `semaphore.acquire()` *prior* to spawning tasks. This enforces true backpressure on ingestion queues under extreme volatility spikes, preventing memory exhaustion.
* **Double-Checked Latency Gating**: Worker tasks re-evaluate the ingress timestamp *after* acquiring the semaphore. If the signal has been delayed by $>1.5$ seconds due to queue congestion, it is discarded and the semaphore is released, preventing trades on stale wicks.
* **Double-Serialization Elimination**: Push raw Python dictionaries directly to the in-process queue from the Bybit and Hyperliquid feeds, bypassing CPU-intensive `orjson.dumps`/`loads` rounds and validating payloads polymorphically.
* **Backpressure Overflow Fallback Safety**: Leverages custom queue-overflow overrides wrapping fallback pushes in correct timestamped tuples to prevent worker unpack crashes during extreme volatility peaks.
* **Garbage Collection (GC) Freezing:** Decouples from heavy Level 2 feeds and offloads hot-path indicators (fast Bollinger Bands, RSI, Z-Scores) to compiled Rust `FastIndicators` structures to prevent Python GC "Stop-The-World" latency jitter.
* **Active Cooldown Guards:** Cooldowns are locked during yields to prevent tick duplication, and are immediately reset if a trade returns early.
* **Asynchronous Backpressure:** `asyncio.Queue` limits backpressure by dropping stale frames during flash-crash scenarios rather than allocating unmanageable heap buffers.

---

## 🚀 Deployment Operations

This project utilizes an AWS ECR/ECS automated pipeline.

```bash
# 1. Setup virtual env and compile Rust PyO3 indicators
python -m venv venv
$env:VIRTUAL_ENV="venv"
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
