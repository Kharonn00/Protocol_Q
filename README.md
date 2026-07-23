# Kalshi Quantitative Trading Engine (KQTE)

## Executive Summary
The Kalshi Quantitative Trading Engine is a high-frequency, fully asynchronous algorithmic trading daemon designed to execute asymmetric taker strategies on Kalshi's 15-minute Cryptocurrency Options markets. Built on absolute Zero-Trust security principles and strict $O(1)$ memory bounds, the engine aggregates Binance, Bybit, and Hyperliquid futures liquidation streams alongside Coinbase spot transaction feeds to perfectly time and snipe explosive macro-level breakouts in the Kalshi options chain.

Currently deployed as an optimized, multi-stage AWS ECS Fargate service, the daemon operates **Strategy 1 (Liquidation Sniper)**, **Strategy 4 (CF Benchmarks Index Lag Arbitrage)**, and **Strategy 5 (Taker Order Flow Imbalance)** as its active execution vehicles. Strategy 2 (Z-Score Sniper) and Strategy 3 (DOGE Theta Harvester) are **currently commented out / deactivated** to focus capital allocation strictly on high-edge quantitative signals.

---

## 🏛️ System Architecture & Package Structure

* **Runtime Environment:** Python 3.12-slim and Rust (ABI-aligned multi-stage compilation).
* **Package Modularization (`engine/` Package):**
  * `engine/config.py`: Centralized `BotConfig`, default `config` instance, `GLOBAL_SSL_CONTEXT`, `TRUSTED_INTERNAL_HOSTS` whitelist, strategy parameters (`INDEX_LAG_MIN_DIVERGENCE`, `OFI_BUY_SELL_RATIO`, `OFI_MIN_VOLUME_NOTIONAL`), and precompiled regexes (`DOLLAR_STRIKE_RE`, `GENERIC_NUMBER_RE`, `_ANSI_ESCAPE_RE`).
  * `engine/models.py`: `AssetState`, `PerformanceTracker`, zero-trust Pydantic schema validation (`EconomicEvent`, `EconomicCalendarResponse`), type-safe financial parsers (`safe_decimal`, `safe_int`), fast ticker validators (`validate_tick_data` with explicit side & volume checks, `validate_binance_payload`), and fallback handlers (`PyFastIndicators`, `DummyIndexLagTracker`, `DummyTakerOrderFlowTracker`) for environments without Rust FFI binaries.
  * `engine/security.py`: `SafeResolver` (SSRF & DNS rebinding defense), `MacroCircuitBreaker`, `sanitize_log_str` (CWE-117 log injection and ANSI escape defense), `is_private_ip`, `safe_drain_queue`, `calculate_backoff_delay`, `handle_health_check`, and `log_exception_group`.
  * `engine/broker.py`: `ExecutionBroker` abstract base class, `SimExecutionBroker`, `LiveKalshiBroker`, RSA-PSS V2 protocol request signing, REST API methods, paper trading orderbook matching engine (`_paper_orders` with atomic lock protection), and `_extract_fill_price` helper.
  * `engine/strategy.py`: Core `LiveTradingEngine`, Binance Liquidation Sniper, Index Lag Arbitrage, Taker Order Flow Imbalance, generic signal router, laddered Take-Profit lifecycle monitor, settlement reconciliation loops, multi-venue WebSocket ingestion consumers, and `get_kalshi_credentials` (AWS Secrets Manager zero-residency PEM loader).
  * `kalshi_main.py`: Lightweight 100-line entry point bootstrapper script that instantiates `LiveTradingEngine` and manages the asyncio event loop.
* **Rust PyO3 Extension:** Native compiled `kalshi_bot` library provides `FastIndicators` (Bollinger Bands, EMA, RSI, Z-Scores), `IndexLagTracker` ($O(1)$ 60s index moving average ring buffer), and `TakerOrderFlowTracker` ($O(1)$ 30s trade tape imbalance tracker) with strict capacity bounds (5,000 max capacity) and timestamp sequence validation.
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
* **Dual-Regime Time-Window Gates**: Operating under strict temporal windows across the 15-minute event cycle:
  * **Ignored Window (Minutes 0.0 to 3.0 / `seconds_left > 720.0s`)**: No trades allowed (blocks early-event wick traps before strike price equilibrium forms).
  * **Early Window (Minutes 3.0 to 7.0 / `480.0s < seconds_left <= 720.0s` - Mean Reversion Mode)**: Assumes early-stage wicks will pull back. Reverts trade direction (buys `NO` on short liquidations, `YES` on long liquidations) and applies strict 4H Macro Trend Shield and Bollinger Band boundaries.
  * **Golden Window (Minutes 7.0 to 13.5 / `90.0s <= seconds_left <= 480.0s` - Momentum Breakout Mode)**: Standard directional sniping (buys `YES` on short liquidations, `NO` on long liquidations) to catch momentum breakout runs.
  * **Ignored Window (Minutes 13.5 to 15.0 / `seconds_left < 90.0s`)**: No trades allowed (prevents last-second expiration slippage).
* **Decoupled Directional Price Caps**:
  * **`MAX_ENTRY_PRICE_YES` (`$0.55`)**: Prevents buying overpriced `YES` momentum tops (historically 0% win rate when buying `YES` above $0.60).
  * **`MAX_ENTRY_PRICE_NO` (`$0.75`)**: Preserves high-probability 70-75% `NO` mean-reversion entries while blocking negative-EV $0.80+ outliers.
* **Spot-to-Strike Distance Gate**: Restricts entries to Out-of-the-Money (OTM) options only if the spot-to-strike distance is within $1.5 \times \text{Standard Deviation}$ ($\sigma$) derived from Bollinger Bands, dynamically scaling the gate with active market volatility.
* **Fallback Ingestion**: If Coinbase ticks are missing (e.g. for `HYPE-USD`), the engine utilizes the Binance/Bybit/Hyperliquid liquidation event price as a spot proxy to feed indicators and safety gates.
* **Asset Performance Auto-Throttle**: Queries `PerformanceTracker` in `engine/models.py` to throttle trades dynamically if the rolling outcome history (last 20 trades per asset/hour in a `deque`) yields a win rate $\le 35\%$ over at least 5 samples.

### 2. Z-Score Momentum Breakout / Mean Reversion Sniper - [DEACTIVATED]
* *Status*: Commented out / disabled to prioritize liquidation breakout edge.

### 3. DOGE Theta Harvester - [DEACTIVATED]
* *Status*: Commented out / disabled due to asymmetric risk-expectancy profiles.

### 4. CF Benchmarks Index Lag Arbitrage - [ACTIVE]
Exploits the 60-second rolling average calculation lag of the settlement index during sudden price surges.
* **Mechanism**: Rust `IndexLagTracker` maintains an $O(1)$ ring buffer tracking spot prices over 60 seconds. When spot price diverges from the 60s moving average by more than `INDEX_LAG_MIN_DIVERGENCE` (0.12%), a directional signal fires.
* **Risk Controls**: Operates strictly within the Golden Window (1.5 to 8.0 minutes remaining) and enforces standard price caps (`MAX_ENTRY_PRICE_YES` $\le \$0.55$, `MAX_ENTRY_PRICE_NO` $\le \$0.75$).

### 5. Taker Order Flow Imbalance (OFI) - [ACTIVE]
Tracks completed cash-register taker trades (`match` events) to capture institutional order flow momentum.
* **Zero-L2 Policy**: Strictly ignores resting limit orders, making the strategy 100% immune to L2 orderbook spoofing.
* **Mechanism**: Rust `TakerOrderFlowTracker` maintains an $O(1)$ 30-second trade tape volume buffer. Fires when Taker Buy/Sell volume ratio exceeds `OFI_BUY_SELL_RATIO` (3.5x) on minimum notional volume `OFI_MIN_VOLUME_NOTIONAL` ($50,000).

### 6. Macroeconomic Circuit Breaker (The Steamroller Defense) - [ACTIVE]
Fundamentally prevents the bot from trading during exogenous regime shifts.
* Fetches economic releases schedule via a static feed.
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
* **WebSockets SSRF/DNS Rebinding Defense**: All incoming WS connections check target URLs against `is_safe_destination_async` in `engine/security.py` pre-flight to prevent connections resolving to private loopback or internal metadata space. Applies to Coinbase, Binance, Bybit, and Hyperliquid client handshakes.
* **HTTP Redirect SSRF Prevention**: Forces `allow_redirects=False` on all external HTTP requests inside candle synchronization loops to prevent attackers from bypassing DNS resolver gates via HTTP 3xx redirects to local metadata.
* **Trusted Proxy Client IP Resolution**: Health endpoint resolves `X-Forwarded-For` from right to left (to prevent header spoofing) strictly if the immediate connecting IP belongs to a private network (e.g. AWS ALB). Direct connections fallback to the socket IP.
* **Health Check Rate Limiting**: Employs a per-source-IP sliding window rate limiter on the health server, with sliding eviction rather than full resets to prevent cache poisoning, and loopback/private connection bypasses.
* **Locked Capital Telemetry Filtering**: `get_locked_capital` in `engine/broker.py` aggregates only resting orders with `action == "buy"`, preventing resting sell/TP orders from inflating telemetry logs.
* **Paper Trading Lock Boundaries**: Employs a dedicated `paper_orders_lock` within `LiveKalshiBroker` in `engine/broker.py` to guarantee thread-safe mutations of paper balance and paper order logs across async yields.
* **Shielded Task Cancellation Cleanups (Zero-Leak Execution)**: Cleanup routines (order cancellation and final balance/position reconciliation) are wrapped in background coroutines shielded via `_safe_shield`, ensuring they execute to completion in the event loop even if the parent task is aborted or timed out.
* **Double-Checked Locking (DCL) Concurrency Shield**: Checks position bounds locklessly, yields to retrieve market prices, and then validates state inside a synchronous `balance_lock` to stop duplicate execution races.
* **Heap Memory Cryptographic Hardening**: Overwrites immutable string dictionary entries (`SecretString`, `PRIVATE_KEY`, `KEY_ID`) inside Secrets Manager decoding, zeroes mutable `bytearray` buffers with `ctypes.memset`, and performs double `gc.collect()` passes on shutdown to eliminate OpenSSL key residency.
* **Rust FFI Queue Capacity Bounding (SEV-1 Mitigation)**: Implements hard 5,000-element capacity caps on Rust `IndexLagTracker` and `TakerOrderFlowTracker` ring buffers to prevent out-of-memory (OOM) heap exhaustion under tick floods.
* **Zero-Lockout Execution Cooldowns**: Mutates `state.cooldown_until` strictly inside atomic `balance_lock` closures upon confirmed capital deduction. Orderbook rejections (ask > limit, spread bounds, distance gates) trigger zero lockout (`0s`), allowing immediate subsequent strategy evaluation.
* **Strict Payload & Sequence Validation**: Enforces explicit `"side"` validation (`"buy"`/`"sell"`) and positive volume checks (`volume > 0.0`) in `validate_tick_data`, and rejects out-of-order timestamps in Rust queues.

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
