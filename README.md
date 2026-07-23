# Kalshi Quantitative Trading Engine (KQTE)

## Executive Summary
The Kalshi Quantitative Trading Engine is a fast, asynchronous trading system. It runs taker strategies on Kalshi 15-minute cryptocurrency options.
The system uses Zero-Trust security rules and has a strict O(1) memory limit. It collects liquidation data from Binance, Bybit, and Hyperliquid futures. It also collects Coinbase spot trades. The system uses this data to trade price wicks in Kalshi options.

The system runs as an AWS ECS Fargate service. It has three active strategies:
* **Strategy 1 (Liquidation Sniper)**
* **Strategy 4 (CF Benchmarks Index Lag Arbitrage)**
* **Strategy 5 (Taker Order Flow Imbalance)**

Strategy 2 (Z-Score Sniper) and Strategy 3 (DOGE Theta Harvester) are disabled to save capital.

---

## 🏛️ System Architecture & Package Structure

* **Runtime Environment:** Python 3.12-slim and Rust (compiled library).
* **Package Structure (`engine/` Directory):**
  * `engine/config.py`: Contains `BotConfig` and default parameters. It defines strategy limits and regex patterns.
  * `engine/models.py`: Contains `AssetState` and `PerformanceTracker`. It validates schemas and parses data. It has fallback classes if the Rust library is missing.
  * `engine/security.py`: Contains `SafeResolver`, `MacroCircuitBreaker`, and log sanitization.
  * `engine/broker.py`: Contains `ExecutionBroker` base class, `SimExecutionBroker`, and `LiveKalshiBroker`. It handles requests, rate limits, and paper trading.
  * `engine/strategy.py`: Contains `LiveTradingEngine`. It runs strategy loops, Take-Profit targets, settlement checks, and WebSocket clients.
  * `kalshi_main.py`: The entry point script. It starts the engine and the asyncio event loop.
* **Rust PyO3 Extension:** The native `kalshi_bot` library. It calculates Bollinger Bands, EMA, RSI, Z-Scores (`FastIndicators`). It tracks the CME index lag (`IndexLagTracker`) and taker order flow imbalance (`TakerOrderFlowTracker`) with O(1) space limits.
* **Infrastructure:** AWS ECS Fargate with a read-only root file system.
* **State Management:** Stateless and ephemeral service.
* **Secret Management:** Secrets are loaded from AWS Secrets Manager. No keys are saved on local disk.
* **Data Ingest:** Secure WebSocket feeds:
  * **Coinbase**: Spot trades (`ticker`).
  * **Binance**: Futures liquidations (`!forceOrder@arr`).
  * **Bybit**: Futures liquidations (`allLiquidation.{symbol}`).
  * **Hyperliquid**: Perpetual liquidations (`trades`).
* **Health Check:** Port `8080` handles health checks. It has a rate limiter to prevent Denial of Service (DoS).

---

## 📈 Quantitative Strategies

### 1. The Liquidation Sniper - [ACTIVE]
This strategy waits for large futures liquidations on external exchanges. It then buys options to capture price moves.
* **Supported Assets**: tracks BTC, ETH, SOL, DOGE, and HYPE.
* **Data Feeds**: collects liquidations from Binance, Bybit, and Hyperliquid.
* **Time Windows**:
  * **0.0 to 3.0 min remaining**: No trades. Strikes are not stable yet.
  * **3.0 to 7.0 min remaining (Mean Reversion)**: Buys opposite to the liquidation side (buys `NO` on short liquidations, `YES` on long liquidations).
  * **7.0 to 13.5 min remaining (Golden Window)**: Buys in the direction of the liquidation (buys `YES` on short liquidations, `NO` on long liquidations).
  * **13.5 to 15.0 min remaining**: No trades. Prevents slippage near expiration.
* **Price Caps**:
  * **`MAX_ENTRY_PRICE_YES` ($0.55)**: Stops buying expensive `YES` options.
  * **`MAX_ENTRY_PRICE_NO` ($0.65)**: Stops buying expensive `NO` options.
* **Distance Gate**: Spot price must be within $1.5 \times \text{Standard Deviation}$ ($\sigma$) of the strike price.
* **Fallback Price**: If Coinbase spot is missing, the system uses external liquidation prices as a proxy.
* **Auto-Throttle**: Stops trading if the win rate drops below 35% over the last 20 trades.

### 2. Z-Score Sniper - [DEACTIVATED]
* Disabled.

### 3. DOGE Theta Harvester - [DEACTIVATED]
* Disabled.

### 4. CF Benchmarks Index Lag Arbitrage - [ACTIVE]
This strategy exploits the 60-second delay of the CME settlement index during fast spot price moves.
* **How it works**: Rust `IndexLagTracker` monitors spot prices. If the spot price differs from the index by $>0.12\%$, a signal fires.
* **Risk Controls**: Executes only during the Golden Window. It enforces price caps (`YES` <= $0.55, `NO` <= $0.65).

### 5. Taker Order Flow Imbalance (OFI) - [ACTIVE]
Tracks executed taker trades to measure order flow direction.
* **No L2 Order Book**: Ignores resting limit orders to prevent spoofing.
* **How it works**: Rust `TakerOrderFlowTracker` monitors trades over 30 seconds. It fires a signal if the Buy/Sell ratio is $>3.5\text{x}$ on volume $\ge \$50,000$.

### 6. Macroeconomic Circuit Breaker - [ACTIVE]
Stops trading during high-impact economic releases (CPI, FOMC).
* Blocks all trading for 30 minutes before and 30 minutes after the release.

---

## 🛡️ Execution & Risk Controls

* **Capital Allocation**: Limits max exposure per event to keep drawdowns low.
* **Drawdown Circuit Breaker**: Measures peak-to-trough drawdown inside `balance_lock`. It stops the bot if drawdown reaches 25%.
* **Take-Profit (TP)**: Splits exit orders into three parts:
  * **Tranche 1 (40%):** Target 50% ROI.
  * **Tranche 2 (35%):** Target 50% to 85% ROI based on remaining time.
  * **Tranche 3 (25%):** Target 95% ROI.
  * **Concurrent Exits:** Cancels resting orders in parallel 20 seconds before expiration.
  * **Hold-to-Settle:** If 2 of 3 tranches reach $0.99, the bot holds the position to settlement to save transaction fees.

---

## 🔒 Security Posture & Zero-Trust Architecture

* **WebSocket SSRF Filter**: Validates target URLs before connecting. Prevents connections to local IP addresses.
* **HTTP Redirect Filter**: Sets `allow_redirects=False` to prevent redirect attacks.
* **Proxy IP Check**: Resolves client IP address from `X-Forwarded-For` headers safely.
* **Health Endpoint Rate Limiting**: Prevents DoS attacks on the health server.
* **Locked Capital Filter**: Telemetry logs only count buy orders. Prevents inflated capital logs.
* **Paper Trading Isolation**: Uses `paper_orders_lock` to protect paper balance changes.
* **Shielded Cleanup**: Cleanup tasks run in shielded async wrappers. They finish even if the parent task stops.
* **Double-Checked Locking (DCL)**: Checks state locklessly, gets prices, and then validates details under `balance_lock`. Uses `execution_in_flight` set check. If a contract is active in-flight, new tasks stop immediately to prevent race conditions.
* **Memory Hardening**: Overwrites PEM keys in memory, zeroes byte arrays using `ctypes.memset`, and runs GC on shutdown.
* **Rust Queue Limits**: Restricts queues to 5,000 items. Prevents out-of-memory errors.
* **API Rate Limit Cooldown**: Pauses the bot for 60 seconds if it receives an HTTP 429 rate limit error.
* **Zero-Lockout Cooldown**: Rejection on price or spread triggers a 0s cooldown instead of a full lockout.
* **Slippage & Fee Simulation**: Paper trading applies $0.01 slippage and $0.005 fee per contract to simulate real market conditions.

---

## 🧠 Memory & Concurrency Optimization

* **Queue Backpressure**: Spawns candidate checks as background tasks so the queue drains quickly.
* **Semaphore Limits**: Limits concurrent tasks to prevent memory issues.
* **Latency Check**: Drops tasks if queue delay is $>1.5$ seconds.
* **Single Serialization**: Ingests raw JSON data directly to avoid double serialization overhead.
* **GC Freezing**: Offloads calculations to Rust to prevent Python GC pauses.

---

## 🚀 Deployment Operations

Set up the virtual environment, compile Rust, and deploy the Docker container:

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
