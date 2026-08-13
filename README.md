# Kalshi Quantitative Trading Engine (KQTE)

## Executive Summary
The Kalshi Quantitative Trading Engine is a high-frequency, fully asynchronous algorithmic trading daemon built for Kalshi 15-minute cryptocurrency options (BTC, ETH, SOL, HYPE).

The system pairs a high-performance Python 3.12 event loop with a native Rust PyO3 C-extension (`kalshi_bot`) for $O(1)$ technical indicator calculation, 60s moving average index lag tracking, and 30s trade tape imbalance analytics. It ingests multi-exchange WebSocket liquidation and trade feeds (Binance, Bybit, Hyperliquid, Coinbase) to execute low-latency taker strategies.

The system is designed as an ephemeral, 12-factor compliant AWS ECS Fargate microservice featuring Zero-Trust security and a zero-L2 orderbook tracking architecture.

---

## 🏛️ System Architecture & Package Structure

* **Runtime Stack:** Python >=3.12-slim and Rust >=1.85 (PyO3 FFI C-extension).
* **Package Structure (`engine/` Directory):**
  * [`engine/config.py`](file:///C:/Users/A2/OneDrive/Documents/Python%20Bots/kalshi_bot/engine/config.py): `BotConfig` 12-factor environment loader with bounds validation assertions (`__post_init__`).
  * [`engine/models.py`](file:///C:/Users/A2/OneDrive/Documents/Python%20Bots/kalshi_bot/engine/models.py): `AssetState`, `OrderbookState`, LRU-bounded `PerformanceTracker`, and Pydantic tick validators with NaN/Inf guards.
  * [`engine/security.py`](file:///C:/Users/A2/OneDrive/Documents/Python%20Bots/kalshi_bot/engine/security.py): `SafeResolver` (SSRF/DNS rebinding defense), `MacroCircuitBreaker`, and `sanitize_log_str` (CWE-117 log injection mitigation).
  * [`engine/broker.py`](file:///C:/Users/A2/OneDrive/Documents/Python%20Bots/kalshi_bot/engine/broker.py): `ExecutionBroker` base class, `SimExecutionBroker` (paper trading with slippage and fee simulation), and `LiveKalshiBroker` (RSA-2048 PSS REST signing).
  * [`engine/strategy.py`](file:///C:/Users/A2/OneDrive/Documents/Python%20Bots/kalshi_bot/engine/strategy.py): `LiveTradingEngine` main orchestrator, multi-exchange WebSocket consumers, Take-Profit scaling monitors, and secret memory scrubbing routines.
  * [`kalshi_main.py`](file:///C:/Users/A2/OneDrive/Documents/Python%20Bots/kalshi_bot/kalshi_main.py): Application entry point and signal handling initialization.
* **Rust PyO3 Extension (`src/lib.rs`):** Native `kalshi_bot` library delivering $O(1)$ `FastIndicators` (Bollinger Bands, EMA, RSI, Welford's variance), 60s `IndexLagTracker`, and 30s `TakerOrderFlowTracker` with 2.5s persistence guards and timestamp jump protection.
* **Infrastructure:** Ephemeral AWS ECS Fargate container with AWS Secrets Manager key retrieval (zero local disk secret storage).

---

## 📈 Active & Inactive Strategy Matrix

| Strategy | Status | Logic & Execution Rules | Risk & Timing Gates |
| :--- | :--- | :--- | :--- |
| **1. Binance Liquidation Sniper** | **ACTIVE** | Monitors Binance, Bybit, and Hyperliquid forced liquidations ($1.5M BTC, $750k ETH, $300k SOL, $100k HYPE). | Golden Window ($300\text{s} \le t \le 720\text{s}$), Distance Gate ($\le 0.8\sigma$), Ask Corridor ($\$0.35–\$0.55$). |
| **4. CF Benchmarks Index Lag Arb** | **ACTIVE** | Exploits 60s settlement index rolling average lag when spot diverges $\ge 0.12\%$ ($0.25\%$ for ETH). | Golden Window ($240\text{s} \le t \le 480\text{s}$), Ask Corridor ($\$0.35–\$0.55$). |
| **5. Taker Order Flow Imbalance (OFI)** | **ACTIVE** | Tracks 30s executed trade tape (`match` events). Triggers when Buy/Sell ratio $\ge 3.5\text{x}$ on volume $\ge \$50\text{k}$ ($150\text{k}$ ETH) with 2.5s Rust persistence filter. | Zero-L2 policy, Golden Window ($240\text{s} \le t \le 540\text{s}$), Denominator Floor (`min(buy, sell) >= $2.5k`). |
| **Macro Circuit Breaker** | **ACTIVE** | Locks out trading 30 min before/after high-impact USD economic releases (CPI, FOMC). | Auto-locks if calendar updates exceed 24 hours. |
| **3H Selective Trend Shield** | **ACTIVE** | Blocks counter-trend trades (`YES` in `DOWN`, `NO` in `UP`) based on 3-hour candle trend with $\pm 0.2\%$ deadband. | Bypassed for Strategy 4 (market-neutral index arbitrage). |
| **2. Coinbase Z-Score Sniper** | **DEACTIVATED** | Evaluated Z-scores ($\pm 3.0$) and macro trends. | Disabled to prevent chop drag. |
| **3. DOGE Theta Harvester** | **DEACTIVATED** | Harvested option decay in final contract minutes. | Disabled due to asymmetric risk profile. |
| **DOGE Trading Gate** | **DISABLED** | Suspended DOGE-USD contract entries. | Filtered due to sub-tick bid-ask noise ($0.12\% = \$0.000084$). |

---

## 🛡️ Core Execution & Risk Invariants

* **Decoupled Entry Price Corridor**:
  * **`MIN_ENTRY_PRICE` ($\ge \$0.35$)**: Blocks buying low-probability out-of-the-money options ($< \$0.35$ ask).
  * **`MAX_ENTRY_PRICE` ($\le \$0.55$)**: Restricts entries to price corridor ($\$0.35–\$0.55$) to preserve high-probability setups.
* **Strict OTM Distance Gate**: Blocks buying In-The-Money (ITM) options (`YES` when $\text{spot} \ge \text{strike}$, `NO` when $\text{spot} \le \text{strike}$) and restricts Out-Of-The-Money (OTM) entries to within $0.8 \times \text{Standard Deviation}$ ($\sigma$) derived from Bollinger Bands.
* **Double-Checked Locking (DCL)**: Locklessly checks state, yields to fetch orderbook, and validates inside synchronous `balance_lock`. Lockouts clear on orderbook rejection (0s lockout); `cooldown_until` is mutated strictly upon confirmed execution. Atomic `execution_in_flight` set check eliminates race conditions (TOCTOU).

---

## 🔒 Zero-Trust Security & Memory Hardening

* **SSRF & Subnet Defense**: `SafeResolver` verifies destination IPs against private, loopback, link-local, multicast, unspecified, and `0.0.0.0/8` subnets.
* **Secret Memory Scrubbing**: Overwrites PEM keys with `"X" * len(...)`, zeroes bytearrays in memory using `ctypes.memset`, and calls double `gc.collect()` sweeps on shutdown.
* **Log Injection Sanitization**: All external inputs, identifiers, and payload fields pass through `sanitize_log_str()` to strip CRLF (`\r\n`) and ANSI escape sequences (CWE-117).
* **Input Validation & NaN Guards**: All incoming WebSocket prices and volumes are checked for `math.isfinite()`. `safe_decimal` rejects `NaN` and `Inf` values.

---

## 🚀 Building & Deployment

### Local Development & Rust FFI Compilation

```bash
# 1. Initialize Python 3.12 Virtual Environment
python -m venv venv
source venv/bin/activate  # On Windows: .\venv\Scripts\activate

# 2. Install Dependencies & Maturin Build System
pip install -r requirements.txt maturin

# 3. Compile Native Rust PyO3 Extension in Release Mode
maturin develop --release

# 4. Verify Syntax & Static Type Compilation
python -m py_compile kalshi_main.py engine/*.py
```

### Docker Container & AWS ECS Deployment

```bash
# 1. Authenticate to AWS ECR
aws ecr get-login-password --region <REGION> | docker login --username AWS --password-stdin <ACCOUNT_ID>.dkr.ecr.<REGION>.amazonaws.com

# 2. Build Cross-Platform Docker Image
docker build --platform linux/amd64 -t kalshi-quant-engine .

# 3. Tag and Push to ECR Repository
docker tag kalshi-quant-engine:latest <ACCOUNT_ID>.dkr.ecr.<REGION>.amazonaws.com/kalshi-quant-engine:latest
docker push <ACCOUNT_ID>.dkr.ecr.<REGION>.amazonaws.com/kalshi-quant-engine:latest

# 4. Trigger ECS Rolling Deployment
aws ecs update-service --cluster <CLUSTER_NAME> --service <SERVICE_NAME> --force-new-deployment
```

---

## 📜 License & Financial Disclaimer

### License
Distributed under the **MIT License**. See [`LICENSE`](LICENSE) for the full text.

### Contributing
See [`CONTRIBUTING.md`](CONTRIBUTING.md) for development setup and contribution guidelines.

### Financial & Risk Disclaimer
> **IMPORTANT**: This software is released strictly for educational, research, and open-source demonstration purposes. Quantitative trading in binary options and cryptocurrency derivatives carries substantial financial risk, including total loss of capital. Nothing in this repository constitutes financial, investment, legal, or tax advice. The authors and contributors accept no liability for financial losses incurred through the use or deployment of this software.

