# Kalshi Quantitative Trading Engine (KQTE)

## Executive Summary
The Kalshi Quantitative Trading Engine is a high-frequency, fully asynchronous algorithmic trading daemon designed to execute asymmetric taker strategies on Kalshi's 15-minute Cryptocurrency Options markets (BTC-USD, ETH-USD). Built on absolute Zero-Trust security principles and strict $O(1)$ memory bounds, the engine aggregates global L2 spot orderbooks and futures liquidation feeds to snipe statistical mispricings in the Kalshi options chain.

Currently deployed as a high-availability AWS ECS service, the engine bypasses traditional Maker "adverse selection" traps by employing a highly predictive Binance Liquidation Sniper model.

---

## 🏛️ System Architecture

* **Framework:** Pure asynchronous Python (`asyncio`, `aiohttp`, `websockets`)
* **Orchestration:** AWS Elastic Container Service (ECS) with Fargate
* **State Management:** Fully stateless, ephemeral execution engine (Twelve-Factor App Compliant)
* **Secret Management:** AWS Secrets Manager via `boto3` (No local key storage)
* **Data Ingestion:** Multiplexed secure WebSockets (Coinbase L2 Spot, Binance Futures `!forceOrder@arr`)

---

## 📈 Quantitative Strategies

### 1. The Binance Liquidation Sniper (Asymmetric Breakout)
Instead of providing resting liquidity (and suffering adverse selection from high-frequency market makers), the engine operates as an asymmetric taker.
* Monitors the Binance USD-M Futures WebSocket for forced liquidations $> \$1.5M$.
* When a macro-liquidiation occurs (e.g., a massive short squeeze), the bot bypasses Kalshi's slow price-discovery and immediately snipes 'YES' or 'NO' contracts before Market Makers can pull their resting quotes.

### 2. Strict Temporal Risk Gates
The bot enforces a highly specific mathematical "kill box" on the 15-minute event timeline:
* **Phase 1: Burn-In (Min 0-7):** Read-only mode. EWMA volatility establishes a baseline while Theta (time-decay) crushes amateur premium.
* **Phase 2: Strike Window (Min 7-12):** Active hunting. Z-score and Erf probabilities achieve peak predictive efficiency.
* **Phase 3: Illiquidity Lockdown (Min 12-15):** Absolute lock. Spreads widen toxically; the bot refuses to trade to prevent late-stage adverse selection.

---

## 🔒 Security Posture & Zero-Trust Architecture

The system is engineered assuming a strictly hostile network environment:
* **Payload Validation:** All inbound WebSocket ticks are routed through strict Default-Deny `Pydantic` validation models. Impossible string encodings or malicious JSON injection attempts gracefully fail and drop.
* **CWE-362 (TOCTOU) Mitigation:** Atomic scope `asyncio.Lock` wrappers guarantee that exposure checks and capital limits evaluate sequentially, physically preventing parallel websockets from over-leveraging the account limits.
* **Memory Level Key-Wiping:** The cryptographic `load_pem_private_key` array is zeroed out of RAM using `ctypes.memset` immediately after the PKCS#1 v1.5 Kalshi API signature is generated, preventing key leakage in the event of an OS heap-dump.
* **Immutable Limits:** Drawdown and spread maximums are isolated in AWS ECS Environment Variables. Configuration tampering requires elevated AWS IAM privileges.

---

## 🧠 Memory Optimization

Engineered to run infinitely without container degradation:
* **Strict $O(1)$ Space Complexity:** 
  * The L2 orderbook engine explicitly bounds and prunes the spread dictionary via `_prune_orderbook` to a strict 1,000-key limit.
  * EWMA calculations rely on statically sized circular buffers (`collections.deque(maxlen=10)`).
* **Asynchronous Backpressure:** `asyncio.Queue(maxsize=...)` implements hard network boundary drops. If the network experiences a flash-crash and emits 100,000 ticks a second, the system intelligently drops stale queue frames rather than ballooning RAM.

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

  - Phase 7: Database Telemetry: Route internal _write_audit_log from ephemeral
    JSONL local disk straight to AWS DynamoDB / CloudWatch Streams, achieving a
    completely Read-Only root filesystem.
  - Phase 8: Welford's Mean-Reversion Backup: If the macro regime shifts into a
    sideways consolidation (low volatility), pivot to an O(1) Welford Online
    Algorithm to calculate Hurst Exponent and Ornstein-Uhlenbeck (OU)
    mean-reversion, fading "fake breakouts."


***

### Memory Impact
**Space Complexity: O(1) Documentation Generation.**
This text document itself does not impact our operational codebase's execution footprint. However, meticulously documenting our use of bounded arrays (`deque(maxlen=10)`), explicit L2 dictionary pruning limits, and asynchronous network queue bounds acts as an administrative enforcement of our O(1) architectural baseline. Any future developer who reads this repository will immediately understand that dynamic array accumulation or loading massive pandas dataframes into the trading loop is strictly prohibited.

### Security Posture
**Information Disclosure Controls:** 
Notice that in the Deployment Operations section of the README, I explicitly sanitized your specific AWS Account ID (`319751623616`) and Region (`us-east-1`), replacing them with `<ACCOUNT_ID>` and `<REGION>` placeholders. Even in a private repository, adhering to the Principle of Least Privilege dictates that we never hardcode raw infrastructure identifiers into text files, protecting against internal credential leakage or accidental repository exposure. The README focuses strictly on architectural mechanics and defense-in-depth methodologies without disclosing live attack surfaces.
