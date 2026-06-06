# Kalshi Quantitative Trading Engine (KQTE)

## Executive Summary
The Kalshi Quantitative Trading Engine is a high-frequency, fully asynchronous algorithmic trading daemon designed to execute asymmetric taker strategies on Kalshi's 15-minute Cryptocurrency Options markets (BTC-USD, ETH-USD). Built on absolute Zero-Trust security principles and strict $O(1)$ memory bounds, the engine aggregates global L2 spot orderbooks and futures liquidation feeds to snipe statistical mispricings in the Kalshi options chain.

Currently deployed as a highly available AWS ECS Fargate service with a strict Read-Only Root Filesystem, the daemon operates two distinct quantitative engines in parallel: an $O(1)$ **Welford Mean-Reversion Engine** to fade algorithmic chop, and a predictive **Binance Liquidation Sniper** to capture macro-level breakouts.

---

## 🏛️ System Architecture

* **Framework:** Pure asynchronous Python (`asyncio`, `aiohttp`, `websockets`)
* **Orchestration:** AWS Elastic Container Service (ECS) with Fargate (`readonlyRootFilesystem: true`)
* **State Management:** Fully stateless, ephemeral execution engine (Twelve-Factor App Compliant)
* **Secret Management:** AWS Secrets Manager via `boto3` (No local key storage)
* **Data Ingestion:** Multiplexed secure WebSockets (Coinbase L2 Spot, Binance Futures `!forceOrder@arr`)

---

## 📈 Quantitative Strategies

### 1. Welford Mean-Reversion (Fading the Rubber-Band)
The primary execution engine hunts for "fake breakouts" (algorithmic spoofing or retail panic buying) during ranging markets.
* Continuously calculates a highly responsive baseline using a recursive Exponential Moving Average (EMA).
* Utilizes **Welford’s Online Algorithm** to track strict real-time variance and standard deviation without storing arrays in memory.
* If a sudden price spike yields a Z-Score $> \pm 2.5$, the engine assumes a standard deviation anomaly and aggressively buys the opposite side of the Kalshi orderbook for pennies to capture the imminent mean-reversion.

### 2. The Binance Liquidation Sniper (Macro Breakouts)
Operates as a parallel contingency model to bypass mean-reversion logic during legitimate market crashes.
* Monitors the Binance USD-M Futures WebSocket for forced liquidations $> \$1.5M$.
* When a macro-liquidation occurs, the bot bypasses the standard mathematical Anchor constraints and immediately snipes predictive momentum contracts before Market Makers can pull their resting quotes.

### 3. Strict Temporal Risk Gates
The bot enforces a highly specific mathematical "kill box" on the 15-minute event timeline:
* **Phase 1: Burn-In (Min 0-7):** Read-only mode. Welford volatility establishes a pristine baseline while Theta (time-decay) crushes amateur premium.
* **Phase 2: Strike Window (Min 7-12):** Active hunting. Welford Z-Scores achieve peak predictive efficiency.
* **Phase 3: Illiquidity Lockdown (Min 12-15):** Absolute lock. Spreads widen toxically; the bot mathematically deadlocks itself to prevent late-stage adverse selection.

---

## 🔒 Security Posture & Zero-Trust Architecture

The system is engineered assuming a strictly hostile network and execution environment:
* **DoD-Grade Container Immutability:** The ECS deployment mandates a completely locked, read-only root filesystem. The OS-level heartbeat executes exclusively inside an un-guessable, permissions-restricted 128MB memory partition (`tmpfs`).
* **Payload Validation & Log Truncation:** All inbound WebSocket ticks undergo strict Default-Deny `Pydantic` schema validation. Malicious JSON injections automatically drop. Furthermore, uncompressed external API error dumps are aggressively truncated to `[:250]` characters to prevent cloud-logging buffer exhaustion.
* **CWE-362 (TOCTOU) Mitigation:** Atomic scope `asyncio.Lock` wrappers guarantee that exposure checks and capital limits evaluate sequentially, physically preventing parallel WebSockets from over-leveraging account constraints.
* **Memory Level Key-Wiping:** The cryptographic `load_pem_private_key` buffer is zeroed out of RAM using OS-level `ctypes.memset` immediately after the PKCS#1 v1.5 API signature is generated, neutralizing heap-dump extraction vectors.

---

## 🧠 Memory Optimization

Engineered to run infinitely without Out-Of-Memory (OOM) degradation or Garbage Collection stutter:
* **Absolute $O(1)$ Space Complexity:** 
  * The Welford pricing engine evaluates millions of continuous high-frequency ticks using strictly four pre-allocated 64-bit float variables. All array buffers (e.g., Pandas dataframes, standard Python lists) are explicitly banned from the pricing logic.
  * The L2 orderbook engine aggressively bounds and prunes the bid/ask spread dictionaries via `_prune_orderbook` to a strict maximum threshold.
* **Asynchronous Backpressure:** `asyncio.Queue(maxsize=...)` implements hard network boundary limits. In the event of a flash-crash emitting 100,000+ ticks a second, the system intelligently drops stale queue frames rather than allocating unmanageable heap pressure.

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

🔮 Roadmap / Future Implementation

  - Phase 9: Macroeconomic Circuit Breaker: Implement a daily cron task to fetch
    high-impact US economic schedules (CPI, FOMC, NFP). Introduce a temporal
    deadlock parameter to pause the Welford Mean-Reversion engine 15 minutes
    prior to scheduled exogenous shocks, mathematically preventing the bot from
    "picking up pennies in front of a steamroller" during fundamental macro
    regime shifts.
  - Phase 10: PostgreSQL Telemetry Sink: Stream CloudWatch fill/execution logs
    into TimescaleDB/PostgreSQL to perform long-term Expected Value (EV)
    backtesting analysis on Z-Score fade threshold optimizations.


***

### Memory Impact
**Space Complexity: O(1) Contextual State Update.**
Rewriting documentation is an offline, administrative process that utilizes absolutely zero runtime memory inside the AWS container. However, effectively documenting the removal of `collections.deque` and the addition of `readonlyRootFilesystem` guarantees that any new engineers onboarded to the project immediately understand the absolute $O(1)$ constraints of this repository, physically preventing the accidental reintroduction of lagging data arrays.

### Security Posture
**Administrative Information Hygiene (Least Privilege):**
I removed my previous AI commentary from the markdown body, as it was leaking internal development prompts into the public documentation. I maintained the sanitization of your AWS Account ID (`<ACCOUNT_ID>`) and Region (`<REGION>`) placeholders. This clean `README.md` now acts as a professional, institutional representation of our system's architecture without exposing live credentials, specific CloudWatch query structures, or infrastructure routing topology.
