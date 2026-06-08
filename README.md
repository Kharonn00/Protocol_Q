# Kalshi Quantitative Trading Engine (KQTE)

## Executive Summary
The Kalshi Quantitative Trading Engine is a high-frequency, fully asynchronous algorithmic trading daemon designed to execute asymmetric taker strategies on Kalshi's 15-minute Cryptocurrency Options markets (BTC-USD, ETH-USD). Built on absolute Zero-Trust security principles and strict $O(1)$ memory bounds, the engine aggregates real-time spot transaction tape (ticker feeds) and futures liquidation streams to snipe statistical mispricings in the Kalshi options chain.

Currently deployed as an optimized, multi-stage AWS ECS Fargate service, the daemon operates two distinct quantitative engines in parallel: an $O(1)$ **Welford Mean-Reversion Engine** to fade algorithmic chop, and a predictive **Binance Liquidation Sniper** to capture macro-level breakouts.

---

## 🏛️ System Architecture

* **Runtime Environment:** Python 3.12-slim (ABI-aligned multi-stage compilation)
* **Orchestration:** AWS Elastic Container Service (ECS) with Fargate (`readonlyRootFilesystem: true`)
* **State Management:** Fully stateless, ephemeral execution engine (Twelve-Factor App Compliant)
* **Secret Management:** AWS Secrets Manager via `boto3` (No local key storage)
* **Data Ingestion:** Multiplexed secure WebSockets (Coinbase Spot Tape `ticker`, Binance Futures `!forceOrder@arr`)

---

## 📈 Quantitative Strategies

### 1. Welford Mean-Reversion (Fading the Rubber-Band)
The primary execution engine hunts for "fake breakouts" (algorithmic stops or retail panic buying) during ranging markets.
* Continuously calculates a highly responsive baseline using a recursive Exponential Moving Average (EMA).
* Utilizes **Welford’s Online Algorithm** to track strict real-time variance and standard deviation without storing arrays in memory.
* **Structural Spoofing Immunity:** Operates exclusively on executed spot trades (ticker tape) rather than the unexecuted L2 orderbook, rendering the algorithm mathematically immune to market manipulation tactics such as Orderbook Spoofing or Quote Stuffing.
* If a sudden transaction spike yields a Z-Score $> \pm 2.5$, the engine assumes a standard deviation anomaly and aggressively buys the opposite side of the Kalshi orderbook for pennies to capture the imminent mean-reversion.

### 2. The Binance Liquidation Sniper (Macro Breakouts)
Operates as a parallel contingency model to bypass mean-reversion logic during legitimate directional market crashes.
* Monitors the Binance USD-M Futures WebSocket for forced liquidations $> \$1.5M$.
* When a macro-liquidation occurs, the bot bypasses the standard mathematical Anchor constraints and immediately snipes predictive momentum contracts before Market Makers can pull their resting quotes.

### 3. Macroeconomic Circuit Breaker (The Steamroller Defense)
Fundamentally prevents the bot from trading during exogenous regime shifts.
* Fetches global economic schedules (e.g., CPI, FOMC, NFP) via a static `JSON` calendar feed.
* Utilizes a highly optimized $O(1)$ parsing loop to discard international events, tracking strictly high-impact `USD` shocks.
* Engages a mathematical temporal deadlock (a configurable lockout window before/after the event) to pause the Welford engine, preventing the bot from "picking up pennies in front of a steamroller" during scheduled macro volatility.

### 4. Strict Temporal Risk Gates
The bot enforces a highly specific mathematical "kill box" on the 15-minute Kalshi event timeline:
* **Phase 1: Burn-In (Min 0-7):** Read-only observation mode. Welford volatility and EMA baselines are continuously updated to establish a pristine baseline while Theta (time-decay) crushes amateur premium.
* **Phase 2: Strike Window (Min 7-12):** Active hunting. Welford Z-Scores achieve peak predictive efficiency.
* **Phase 3: Illiquidity Lockdown (Min 12-15):** Absolute lock. Spreads widen toxically; the bot mathematically deadlocks itself to prevent late-stage adverse selection.

---

## 🔒 Security Posture & Zero-Trust Architecture

The system is engineered assuming a strictly hostile network and execution environment:
* **Cancel-Fill Race Condition Shield:** Implements cryptographic-grade validation loops during order cancellations. If a `cancel_order` request is rejected or misses its execution window, the engine queries the active portfolio state as a defensive fallback rather than blindly refunding local capital, effectively eliminating local state drift and phantom positions.
* **Defensive Data Ingestion (Anti-Compression Bombs):** External API fetches enforce rigid `Content-Length` bounds ($< 512$ KB) and exact MIME-type header validation (`application/json`) prior to deserialization, preventing Zip Bombs, HTML parsing crashes, and malicious control-character log injections.
* **Heap Memory Cryptographic Hardening:** The Kalshi API private key is dynamically injected via AWS Secrets Manager. String fragments are explicitly un-referenced (`del`), the immutable `bytearray` is scrubbed via `ctypes.memset`, and a forced `gc.collect()` sweep guarantees cleartext key materials are instantly wiped from physical memory pages prior to entering the trading loop.
* **Zero-Trust Path Traversal Mitigation:** Every dynamic API variable—including `contract_id`, `order_id`, and `client_order_id`—is aggressively URL-encoded using `urllib.parse.quote(..., safe='')`, neutralizing HTTP directory traversal and parameter pollution.
* **Identity Transparency:** Conforms strictly to third-party Terms of Service by passing honest User-Agent HTTP identifiers (`KalshiQuantEngine/1.0`), preventing the service from engaging in active deception or Web Application Firewall (WAF) circumvention.

---

## 🧠 Memory Optimization

Engineered to run infinitely without Out-Of-Memory (OOM) degradation or Garbage Collection stutter:
* **Garbage Collection (GC) Freezing:** By deliberately decoupling from the Coinbase Level 2 WebSocket firehose, the engine bypasses Python's GC "Stop-The-World" execution pauses. Welford's algorithm processes infinite streaming ticks utilizing four pre-allocated 64-bit float scalars, strictly preserving an absolute $O(1)$ operational heap and eliminating the latency jitter inherent in dictionary churn.
* **Asynchronous Backpressure:** `asyncio.Queue(maxsize=...)` implements hard network boundary limits. In the event of a flash-crash emitting 100,000+ ticks a second, the system intelligently drops stale queue frames rather than allocating unmanageable heap pressure.
* **Resilient Outage Backoff:** WebSocket consumers implement a truncated exponential backoff with random uniform jitter, protecting the container from CPU thrashing, log bloat, and IP rate-bans during prolonged exchange outages.

---

## 🚀 Deployment Operations

This project utilizes an AWS ECR/ECS automated pipeline.

```
# 1. Authenticate to AWS ECR via secure STS token piping
aws ecr get-login-password --region <REGION> | docker login --username AWS --password-stdin <ACCOUNT_ID>.dkr.ecr.<REGION>.amazonaws.com

# 2. Build the explicit cross-architecture Docker Image
docker build --platform linux/amd64 -t kalshi-quant-engine .

# 3. Tag and Push
docker tag kalshi-quant-engine:latest <ACCOUNT_ID>.dkr.ecr.<REGION>.amazonaws.com/kalshi-quant-engine:latest
docker push <ACCOUNT_ID>.dkr.ecr.<REGION>.amazonaws.com/kalshi-quant-engine:latest

# 4. Trigger ECS Zero-Downtime Rolling Deployment
aws ecs update-service --cluster QuantCluster --service kalshi-bot-service --force-new-deployment

```

## 🔮 Roadmap / Future Implementation

* **Phase 10: PostgreSQL Telemetry Sink:** Stream CloudWatch fill/execution logs into TimescaleDB/PostgreSQL to perform long-term Expected Value (EV) backtesting analysis on Z-Score fade threshold optimizations.
* **Phase 11: Out-of-Band Orderbook Imbalance (OBI):** If future statistical strategies dictate the need for resting L2 Depth tracking, engineer a native Cython or Rust C-Extension to manage Level 2 memory allocations entirely outside of the Python Global Interpreter Lock (GIL) and Garbage Collector.

---

### Memory Impact
**Space Complexity: O(1) Contextual Updates.**
The documentation now accurately describes the application's $O(1)$ space constraints. Updating the README to correctly align with our deprecation of the L2 dictionaries ensures that future developers collaborating on this project understand that allocating arrays, tracking resting books, or introducing non-scalar metrics into the `AssetState` is fundamentally prohibited by the core memory architecture rules. 

### Security Posture
**Administrative Accuracy & Defense Mapping.**
The README now explicitly outlines the advanced defense mechanisms implemented in recent iterations—including the Cancel-Fill Race Shield, Anti-Compression Bomb safeguards, and ToS compliance mechanisms (User-Agent declarations). Properly documenting the specific threat models mitigated (e.g., L2 Spoofing immunity, Zip Bombs) acts as a living threat matrix, maintaining high organizational maturity and ensuring system components are not inadvertently "relaxed" by future maintainers.
