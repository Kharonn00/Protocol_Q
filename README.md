# Kalshi Quantitative Trading Engine (KQTE)

## Executive Summary
The Kalshi Quantitative Trading Engine is a high-frequency, fully asynchronous algorithmic trading daemon designed to execute asymmetric taker strategies on Kalshi's 15-minute Cryptocurrency Options markets (BTC-USD, HYPE-USD, SOL-USD). Built on absolute Zero-Trust security principles and strict $O(1)$ memory bounds, the engine aggregates real-time spot transaction tape (ticker feeds) and futures liquidation streams to snipe statistical mispricings in the Kalshi options chain.

Currently deployed as an optimized, multi-stage AWS ECS Fargate service, the daemon operates two distinct quantitative engines in parallel: a hybrid Python-Rust **Bollinger Band & Z-Score Mean-Reversion Engine** to fade algorithmic chop, and a predictive **Binance Liquidation Sniper** to capture macro-level breakouts.

---

## 🏛️ System Architecture

* **Runtime Environment:** Python 3.12-slim and Rust (ABI-aligned multi-stage compilation)
* **Rust PyO3 Extension:** Native compiled `kalshi_bot` library provides `FastIndicators` for ultra-low latency, GIL-free technical calculations (Bollinger Bands, EMA, Z-Score).
* **Orchestration:** AWS Elastic Container Service (ECS) with Fargate (`readonlyRootFilesystem: true`)
* **State Management:** Fully stateless, ephemeral execution engine (Twelve-Factor App Compliant)
* **Secret Management:** AWS Secrets Manager via `boto3` (No local key storage)
* **Data Ingestion:** Multiplexed secure WebSockets (Coinbase Spot Tape `ticker`, Binance Futures `!forceOrder@arr`)

---

## 📈 Quantitative Strategies

### 1. Z-Score Mean-Reversion (Fading the Rubber-Band)
The primary execution engine hunts for "fake breakouts" (algorithmic stops or retail panic buying) during ranging markets.
* Continuously updates technical baselines using a native compiled Rust technical indicator framework (`FastIndicators`).
* Tracks real-time volatility and standard deviation without storing arrays in memory, maintaining a strict $O(1)$ space complexity.
* **Trend & Breakout Filter (Z-Score Ceiling):** If the Z-Score exceeds $\pm 4.0$, the engine disables mean-reversion logic under the assumption that the mathematical assumption of "reversion" has been broken by a strong directional trend.
* If a price spike yields a Z-Score $> \pm \text{threshold}$ (but remains under $\pm 4.0$), the engine fades the anomaly by buying the opposite side of the option chain.
* **Structural Spoofing Immunity:** Operates exclusively on executed spot trades (ticker tape) rather than the unexecuted L2 orderbook, rendering the algorithm mathematically immune to market manipulation tactics such as Orderbook Spoofing or Quote Stuffing.

### 2. The Binance Liquidation Sniper (Macro Breakouts)
Operates as a parallel contingency model to bypass mean-reversion logic during legitimate directional market crashes.
* Monitors the Binance USD-M Futures WebSocket for forced liquidations exceeding asset-specific thresholds (e.g., $\$1.5\text{M}$ for BTC, $\$100\text{k}$ for HYPE and SOL).
* Validates liquidation events using strict notional bounds ($\$10$ to $\$1\text{B}$) to prevent logic-level overflow vulnerabilities.
* When a macro-liquidation occurs, the bot bypasses the standard mathematical Anchor constraints and immediately snipes predictive momentum contracts.

### 3. Macroeconomic Circuit Breaker (The Steamroller Defense)
Fundamentally prevents the bot from trading during exogenous regime shifts.
* Fetches global economic schedules (e.g., CPI, FOMC, NFP) via a static `JSON` calendar feed.
* Utilizes a highly optimized $O(1)$ parsing loop to discard international events, tracking strictly high-impact `USD` shocks with safety lookahead bounds (max 7 days).
* Engages a mathematical temporal deadlock (a configurable lockout window before/after the event) to pause trading, preventing the bot from "picking up pennies in front of a steamroller" during scheduled macro volatility.

### 4. Strict Temporal Risk Gates
The bot enforces a highly specific mathematical "kill box" on the 15-minute Kalshi event timeline:
* **Phase 1: Burn-In (Min 0-5):** Observation and baseline establishment mode.
* **Phase 2: Strike Window (Min 5-12 / 600 to 180 seconds left):** Active trade execution window. 
* **Phase 3: Illiquidity Lockdown (Min 12-15 / 180 to 0 seconds left):** Absolute lock. Spreads widen toxically; the bot deadlocks itself to prevent late-stage adverse selection.

---

### 🛡️ Execution & Risk Controls

* **Probability Floor:** Rejects any trade execution where `best_ask < 0.15` to avoid purchasing un-winnable low-probability options near expiration.
* **Dynamic Take-Profit (Dynamic TP):** Automatically scales target ROI based on the time remaining to event expiration:
  * At **10 minutes remaining (600s)**: Targets up to **80% ROI** (`1.80x` multiplier).
  * At **3 minutes remaining (180s)**: Targets **15% ROI** (`1.15x` multiplier).
  * Smoothly interpolates linearly between these bounds to adjust risk appetite relative to theta decay.
* **Trailing Order Cancellation:** Upon event expiration timeout, all resting take-profit orders are force-cancelled with a 5-step jittered verification loop to reconcile exact filled quantities.

---

## 🔒 Security Posture & Zero-Trust Architecture

The system is engineered assuming a strictly hostile network and execution environment:
* **Cancel-Fill Race Condition Shield:** Implements cryptographic-grade validation loops during order cancellations. If a `cancel_order` request is rejected or is unconfirmed (TOCTOU prevention), the engine verifies details up to 5 times with jittered backoff, ensuring capital is not prematurely refunded locally.
* **Defensive Data Ingestion (Anti-Compression Bombs):** External API fetches enforce rigid `Content-Length` bounds ($< 512$ KB) and exact MIME-type header validation (`application/json`) prior to deserialization, preventing Zip Bombs, HTML parsing crashes, and malicious control-character log injections.
* **Heap Memory Cryptographic Hardening:** API credentials and keys are scrubbed immediately. String references are deleted (`del`), mutable buffers are zeroed using `ctypes.memset`, stack frame references in the bootstrapper are systematically cleared, and a forced `gc.collect()` sweep wipes cleartext keys from memory page files.
* **Zero-Trust Path Traversal Mitigation:** Every dynamic API variable—including `contract_id` and `order_id`—is URL-encoded using `urllib.parse.quote(..., safe='')`, neutralizing parameter pollution and path traversal attacks.
* **Identity Transparency:** Conforms strictly to third-party Terms of Service by passing honest User-Agent HTTP identifiers (`KalshiQuantEngine/1.0`), preventing WAF circumvention bans.

---

## 🧠 Memory & Concurrency Optimization

Engineered to run infinitely without Out-Of-Memory (OOM) degradation or Garbage Collection stutter:
* **Garbage Collection (GC) Freezing:** By deliberately decoupling from the Coinbase Level 2 WebSocket firehose and offloading indicator calculations to Rust, the engine prevents Python GC "Stop-The-World" pauses.
* **Active Cooldown Guards:** Cooldown periods are locked during yields to prevent concurrent tick duplicate executions, and are immediately reset if a trade fails or returns early to ensure indicator calculations continue without interruption.
* **Asynchronous Backpressure:** `asyncio.Queue(maxsize=...)` implements hard network boundary limits. In the event of a flash-crash emitting 100,000+ ticks a second, the system intelligently drops stale queue frames rather than allocating unmanageable heap pressure.
* **Resilient Outage Backoff:** WebSocket consumers implement a truncated exponential backoff with random uniform jitter, protecting the container from CPU thrashing, log bloat, and IP rate-bans during prolonged exchange outages.

---

## 🚀 Deployment Operations

This project utilizes an AWS ECR/ECS automated pipeline.

```bash
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

---

## 🔮 Roadmap / Future Implementation

* **Phase 10: PostgreSQL Telemetry Sink:** Stream CloudWatch fill/execution logs into TimescaleDB/PostgreSQL to perform long-term Expected Value (EV) backtesting analysis on Z-Score fade threshold optimizations.
* **Phase 11: Out-of-Band Orderbook Imbalance (OBI):** If future statistical strategies dictate the need for resting L2 Depth tracking, engineer a native Cython or Rust C-Extension to manage Level 2 memory allocations entirely outside of the Python Global Interpreter Lock (GIL) and Garbage Collector.
