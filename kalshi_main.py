import os
import sys
import time
import math
import json
import base64
import asyncio
import logging
import urllib.parse
import aiohttp
import orjson
import websockets
import uuid
import certifi
import ssl
import ctypes
import atexit
import datetime
import tempfile
import random
from dataclasses import dataclass
from typing import Dict, Optional, Tuple
from abc import ABC, abstractmethod
from pydantic import BaseModel, Field, ValidationError

# AWS SDK for Secure Secret Management
import boto3
from botocore.exceptions import ClientError

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.serialization import load_pem_private_key

# ==========================================
# CONFIGURATION & LOGGING
# ==========================================
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s - %(message)s')
logger = logging.getLogger("KalshiQuantEngine")

@dataclass
class BotConfig:
    MAX_CONCURRENT_TRADES: int = 2
    TRADE_BUDGET_PCT: float = 0.10
    MAX_CONTRACTS_PER_TRADE: int = 500
    MAX_EXPOSURE_PER_EVENT: int = 1500
    
    # Structurally safe defaults via environment variables (Twelve-Factor App Compliant)
    DRAWDOWN_LIMIT_PCT: float = float(os.environ.get("DRAWDOWN_LIMIT_PCT", "0.20"))
    STALE_BALANCE_TIMEOUT_SEC: float = 120.0
    L2_MAX_DEPTH_PCT: float = 0.05
    
    BINANCE_LIQUIDATION_THRESHOLD: float = float(os.environ.get("BINANCE_LIQ_THRESHOLD", "1500000.0"))
    MAX_ALLOWED_SPREAD: float = float(os.environ.get("MAX_ALLOWED_SPREAD", "0.25"))

    # --- MEAN REVERSION CONFIGURATION ---
    Z_SCORE_THRESHOLD: float = float(os.environ.get("Z_SCORE_THRESHOLD", "2.5"))
    MIN_WELFORD_TICKS: int = int(os.environ.get("MIN_WELFORD_TICKS", "100"))
    MAX_FADE_PRICE: float = float(os.environ.get("MAX_FADE_PRICE", "0.45"))

config = BotConfig()

# ==========================================
# SECURITY: Zero-Trust Payload Validation
# ==========================================
class TickData(BaseModel):
    type: str
    product_id: str = Field(..., pattern="^(BTC-USD|ETH-USD)$")
    price: float = Field(..., gt=0, lt=200000)

class BinanceOrderDetails(BaseModel):
    s: str      # Symbol (e.g., BTCUSDT)
    S: str      # Side: "BUY" (short liq) or "SELL" (long liq)
    q: float = Field(..., gt=0)  # SAST FIX: Strict Lower Bound
    p: float = Field(..., gt=0)  # SAST FIX: Strict Lower Bound

class BinancePayload(BaseModel):
    e: str
    o: BinanceOrderDetails

# ==========================================
# STATE MANAGEMENT (Strict O(1) Memory)
# ==========================================
class AssetState:
    def __init__(self):
        self.active_contract_id: str = ""
        self.strike_price: float = 0.0
        self.expiration_time: float = 0.0  
        self.last_price: Optional[float] = None
        self.cooldown_until: float = 0.0
        self.last_seen_contract_id: str = ""

        # 1. Welford Online Algorithm State (O(1) Variance)
        self.welford_count: int = 0
        self.welford_mean: float = 0.0
        self.welford_m2: float = 0.0
        
        # 2. Recursive EMA Baseline State (O(1) Anchor)
        self.ewma_price: float = 0.0

        # 3. L2 Orderbook State
        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}
        
        # 4. Risk Management State (Now Contract-Isolated to Prevent Rollover Race Conditions)
        self.positions: Dict[str, int] = {}       # contract_id -> current_position_size
        self.position_sides: Dict[str, str] = {}  # contract_id -> position_side ("YES" / "NO")

# ==========================================
# GENERAL UTILITIES (O(1) Helpers)
# ==========================================
def calculate_backoff_delay(attempt: int, base: float = 1.0, max_delay: float = 60.0) -> float:
    # Standard Truncated Exponential Backoff: base * 2^(attempt - 1)
    delay = min(max_delay, base * (2.0 ** (attempt - 1)))
    # SAST FIX: Introduce random jitter to prevent thundering herd retries on API gateway failures
    jitter = random.uniform(0.0, 1.0)
    return delay + jitter

def log_exception_group(eg: BaseException):
    # SAST FIX: Recursive unpack of ExceptionGroups for clean flat log formatting [3]
    if hasattr(eg, 'exceptions'):
        for exc in eg.exceptions:
            log_exception_group(exc)
    else:
        logger.critical(f"TaskGroup sub-exception: {type(eg).__name__} - {str(eg)}")

# ==========================================
# INTERFACE: Strict Execution Contract
# ==========================================
class ExecutionBroker(ABC):
    @abstractmethod
    async def start(self): pass

    @abstractmethod
    async def close(self): pass

    @abstractmethod
    async def get_balance(self) -> Optional[Tuple[float, float]]: pass

    @abstractmethod
    async def get_locked_capital(self) -> float: pass
    
    @abstractmethod
    async def get_active_market(self, asset_symbol: str, current_price: float) -> Tuple[str, float, float]: pass

    @abstractmethod
    async def get_best_bid_ask(self, contract_id: str, side: str) -> Optional[Tuple[float, float]]: pass

    @abstractmethod
    async def get_order_details(self, order_id: str) -> dict: pass
    
    @abstractmethod
    async def get_order_by_client_id(self, client_order_id: str) -> dict: pass

    @abstractmethod
    async def cancel_order(self, order_id: str) -> bool: pass

    @abstractmethod
    async def execute_trade(self, action: str, contract_id: str, side: str, limit_price: float, quantity: int, client_order_id: str = None) -> Optional[str]: pass

# ==========================================
# BROKERS: Simulation & Live
# ==========================================
class SimExecutionBroker(ExecutionBroker):
    def __init__(self):
        self.simulated_balance = 1000.00
        self.positions: Dict[Tuple[str, str], int] = {} 
        self.VALID_ACTIONS = frozenset({"buy", "sell"})
        self.VALID_SIDES = frozenset({"yes", "no"})

    async def start(self):
        logger.info("[SIMULATION] Offline Broker resources initialized.")

    async def close(self):
        logger.info("[SIMULATION] Broker resources cleaned up.")

    async def get_balance(self) -> Optional[Tuple[float, float]]:
        return self.simulated_balance, self.simulated_balance

    async def get_locked_capital(self) -> float:
        return 0.0

    async def get_active_market(self, asset_symbol: str, current_price: float) -> Tuple[str, float, float]:
        base_asset = asset_symbol.split('-')[0]
        strike = round(current_price / 50) * 50
        contract_id = f"KX{base_asset}-15M-{strike}"
        exp_time = time.time() - (time.time() % 900) + 900
        return contract_id, float(strike), exp_time

    async def get_best_bid_ask(self, contract_id: str, side: str) -> Optional[Tuple[float, float]]:
        return 0.40, 0.45

    async def get_order_details(self, order_id: str) -> dict:
        return {"status": "executed", "unfilled_count": 0}

    async def get_order_by_client_id(self, client_order_id: str) -> dict:
        return {"status": "executed", "unfilled_count": 0}

    async def cancel_order(self, order_id: str) -> bool:
        return True

    async def execute_trade(self, action: str, contract_id: str, side: str, limit_price: float, quantity: int, client_order_id: str = None) -> Optional[str]:
        if action.lower() not in self.VALID_ACTIONS or side.lower() not in self.VALID_SIDES:
            return None
        total_value = limit_price * quantity
        order_id = f"sim-{uuid.uuid4().hex}"
        key = (contract_id, side.lower())
        
        if action.lower() == "buy":
            if self.simulated_balance < total_value:
                return None
            self.simulated_balance -= total_value
            self.positions[key] = self.positions.get(key, 0) + quantity
            return order_id
        return None

class LiveKalshiBroker(ExecutionBroker):
    def __init__(self, key_id: str, private_key_pem: bytearray, paper_trade: bool = False):
        self.base_url = "https://external-api.kalshi.com/trade-api/v2"
        self.key_id = key_id
        self.session = None
        self.paper_trade = paper_trade
        self._paper_orders: Dict[str, int] = {}
        # Paper balance follows twelve-factor app dynamic override
        self._paper_balance: float = float(os.environ.get("PAPER_BALANCE", "1000.00"))
        self.VALID_ACTIONS = frozenset({"buy", "sell"})
        self.VALID_SIDES = frozenset({"yes", "no"})
        
        self.timeout_short = aiohttp.ClientTimeout(total=2.0)
        self.timeout_long = aiohttp.ClientTimeout(total=3.0)
        
        try:
            # SAST FIX: Direct load from bytearray avoids leaking cleartext private key copies in heap pages [3]
            self.private_key = load_pem_private_key(private_key_pem, password=None)
        except Exception:
            logger.critical("Cryptographic key load failed. Halting system.")
            raise ValueError("Invalid Private Key Format")
        finally:
            ctypes.memset((ctypes.c_char * len(private_key_pem)).from_buffer(private_key_pem), 0, len(private_key_pem))

    async def start(self):
        # SAST FIX: Enforce isolated root CA certificates using Certifi for complete TLS chain validation [5]
        ssl_ctx = ssl.create_default_context(cafile=certifi.where())
        connector = aiohttp.TCPConnector(ssl=ssl_ctx)
        self.session = aiohttp.ClientSession(connector=connector)

    async def close(self):
        if self.session:
            await self.session.close()

    def _generate_signature(self, timestamp: str, method: str, path: str) -> str:
        # SAST FIX: Strip query parameters strictly prior to signature building [3]
        signed_path = f"/trade-api/v2{path}"
        path_without_query = signed_path.split('?')[0]
        message = f"{timestamp}{method}{path_without_query}".encode('utf-8')
        
        # SAST FIX: RSA-PSS Signing padding with SHA256 matches exact Kalshi V2 protocol [1]
        signature = self.private_key.sign(
            message,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH
            ),
            hashes.SHA256()
        )
        return base64.b64encode(signature).decode('utf-8')

    async def get_balance(self) -> Optional[Tuple[float, float]]:
        if self.paper_trade:
            return self._paper_balance, self._paper_balance
            
        path = "/portfolio/balance"
        method = "GET"
        current_time_ms = str(int(time.time() * 1000))
        signature = self._generate_signature(current_time_ms, method, path)
        headers = {
            "KALSHI-ACCESS-KEY": self.key_id,
            "KALSHI-ACCESS-SIGNATURE": signature,
            "KALSHI-ACCESS-TIMESTAMP": current_time_ms
        }
        try:
            async with self.session.get(f"{self.base_url}{path}", headers=headers, timeout=self.timeout_short) as response:
                if response.status == 200:
                    data = await response.json()
                    # SAST FIX: Base risk limits on Portfolio Value (NAV) instead of just Cash to prevent false-drawdowns [2]
                    available_balance = float(data.get("balance", 0)) / 100.0
                    portfolio_value = float(data.get("portfolio_value", 0)) / 100.0
                    return available_balance, portfolio_value
                return None
        except Exception as e:
            logger.error(f"[API] Error fetching balance: {type(e).__name__}", exc_info=True)
            return None

    async def get_locked_capital(self) -> float:
        if self.paper_trade: return 0.0
        
        path = "/portfolio/orders?status=resting"
        method = "GET"
        current_time_ms = str(int(time.time() * 1000))
        signature = self._generate_signature(current_time_ms, method, path)
        headers = {
            "KALSHI-ACCESS-KEY": self.key_id,
            "KALSHI-ACCESS-SIGNATURE": signature,
            "KALSHI-ACCESS-TIMESTAMP": current_time_ms
        }
        locked_capital = 0.0
        try:
            async with self.session.get(f"{self.base_url}{path}", headers=headers, timeout=self.timeout_long) as response:
                if response.status == 200:
                    data = await response.json()
                    orders = data.get("orders", [])
                    for order in orders:
                        unfilled = order.get("unfilled_count", 0)
                        price = order.get("yes_price") or order.get("no_price", 0)
                        locked_capital += (unfilled * (price / 100.0))
        except Exception as e:
            logger.error(f"[API] Error fetching resting orders: {type(e).__name__}", exc_info=True)
        return locked_capital

    async def get_active_market(self, asset_symbol: str, current_price: float) -> Tuple[str, float, float]:
        base_asset = asset_symbol.split('-')[0]
        series_ticker = f"KX{base_asset}15M"
        
        # SAST FIX: Strict URL parameter escaping to prevent parameter pollution [4]
        safe_series_ticker = urllib.parse.quote(series_ticker, safe='')
        path = f"/markets?series_ticker={safe_series_ticker}&status=open"
        method = "GET"
        current_time_ms = str(int(time.time() * 1000))
        signature = self._generate_signature(current_time_ms, method, path)
        headers = {
            "KALSHI-ACCESS-KEY": self.key_id,
            "KALSHI-ACCESS-SIGNATURE": signature,
            "KALSHI-ACCESS-TIMESTAMP": current_time_ms
        }
        try:
            async with self.session.get(f"{self.base_url}{path}", headers=headers, timeout=self.timeout_long) as response:
                if response.status != 200: return "", 0.0, 0.0
                data = await response.json()
                markets = data.get("markets", [])
                if not markets: return "", 0.0, 0.0
                
                current_ts = time.time()
                valid_markets = []
                
                for market in markets:
                    close_time_str = market.get("close_time", "")
                    if close_time_str:
                        try:
                            exp_ts = datetime.datetime.fromisoformat(close_time_str.replace("Z", "+00:00")).timestamp()
                            if exp_ts > current_ts + 60.0:  
                                valid_markets.append((exp_ts, market))
                        except ValueError:
                            pass
                
                if not valid_markets: return "", 0.0, 0.0
                
                valid_markets.sort(key=lambda x: x[0])
                target_exp_ts = valid_markets[0][0]
                target_markets = [m for t, m in valid_markets if t == target_exp_ts]
                
                closest_market = None
                smallest_distance = float('inf')
                closest_strike = 0.0
                
                for market in target_markets:
                    try:
                        strike_val = market.get("floor_strike")
                        
                        if strike_val is not None:
                            strike_val = float(strike_val)
                        else:
                            subtitle = market.get("subtitle", "")
                            try:
                                strike_val = float(subtitle.split()[-1].replace('$', '').replace(',', ''))
                            except Exception:
                                strike_val = 0.0
                                
                        if strike_val <= 500.0:
                            continue
                            
                        distance = abs(current_price - strike_val)
                        if distance < smallest_distance:
                            smallest_distance = distance
                            closest_market = market
                            closest_strike = strike_val
                    except Exception as e:
                        safe_ticker = repr(market.get('ticker', 'UNKNOWN'))[:50]
                        logger.warning(f"Error parsing market {safe_ticker}: {repr(e)}")
                        continue
                        
                if closest_market:
                    return closest_market['ticker'], closest_strike, target_exp_ts
                return "", 0.0, 0.0
        except Exception as e: 
            logger.error("Error fetching active market", exc_info=True)
            return "", 0.0, 0.0

    async def get_best_bid_ask(self, contract_id: str, side: str) -> Optional[Tuple[float, float]]:
        # SAST FIX: Resolved critical syntax signature parameter mismatch & applied strict quote sanitization [1, 4]
        safe_contract_id = urllib.parse.quote(contract_id, safe='')
        path = f"/markets/{safe_contract_id}/orderbook?depth=1"
        current_time_ms = str(int(time.time() * 1000))
        signature = self._generate_signature(current_time_ms, "GET", path)
        headers = {
            "KALSHI-ACCESS-KEY": self.key_id,
            "KALSHI-ACCESS-SIGNATURE": signature,
            "KALSHI-ACCESS-TIMESTAMP": current_time_ms
        }
        try:
            async with self.session.get(f"{self.base_url}{path}", headers=headers, timeout=self.timeout_short) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    ob_fp = data.get("orderbook_fp")
                    ob_standard = data.get("orderbook")
                    
                    if ob_fp:
                        yes_bids = ob_fp.get("yes_dollars", [])
                        no_bids = ob_fp.get("no_dollars", [])
                        if not yes_bids or not no_bids: return None
                        best_yes_bid = float(yes_bids[0][0])
                        best_no_bid = float(no_bids[0][0])
                    elif ob_standard:
                        yes_bids = ob_standard.get("yes", [])
                        no_bids = ob_standard.get("no", [])
                        if not yes_bids or not no_bids: return None
                        best_yes_bid = yes_bids[0][0] / 100.0
                        best_no_bid = no_bids[0][0] / 100.0
                    else: return None
                    
                    if side.lower() == "yes": 
                        return best_yes_bid, round(1.0 - best_no_bid, 4)
                    else: 
                        return best_no_bid, round(1.0 - best_yes_bid, 4)
        except Exception as e: 
            logger.error("Orderbook fetch error", exc_info=True)
        return None

    async def get_order_details(self, order_id: str) -> dict:
        if order_id.startswith("paper-"):
            qty = self._paper_orders.pop(order_id, 0)
            return {"status": "executed", "executed_count": qty, "unfilled_count": 0}

        # SAST FIX: Escape order_id path parameter with safe='' to prevent HTTP directory traversal [4, 1]
        safe_order_id = urllib.parse.quote(order_id, safe='')
        path = f"/portfolio/orders/{safe_order_id}"
        current_time_ms = str(int(time.time() * 1000))
        signature = self._generate_signature(current_time_ms, "GET", path)
        headers = {
            "KALSHI-ACCESS-KEY": self.key_id,
            "KALSHI-ACCESS-SIGNATURE": signature,
            "KALSHI-ACCESS-TIMESTAMP": current_time_ms
        }
        try:
            async with self.session.get(f"{self.base_url}{path}", headers=headers, timeout=self.timeout_short) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("order", {})
        except Exception as e: 
            logger.error(f"Error fetching order details for {order_id}", exc_info=True)
        return {}

    async def get_order_by_client_id(self, client_order_id: str) -> dict:
        # SAST FIX: Escape client_order_id query parameters with safe='' to protect against HTTP injection [4, 1]
        safe_client_order_id = urllib.parse.quote(client_order_id, safe='')
        path = f"/portfolio/orders?client_order_id={safe_client_order_id}"
        current_time_ms = str(int(time.time() * 1000))
        signature = self._generate_signature(current_time_ms, "GET", path)
        headers = {
            "KALSHI-ACCESS-KEY": self.key_id,
            "KALSHI-ACCESS-SIGNATURE": signature,
            "KALSHI-ACCESS-TIMESTAMP": current_time_ms
        }
        try:
            async with self.session.get(f"{self.base_url}{path}", headers=headers, timeout=self.timeout_long) as resp:
                if resp.status == 200:
                    # SAST FIX: Corrected variable name reference from response.json() to resp.json() to prevent runtime NameError [1]
                    data = await resp.json()
                    orders = data.get("orders", [])
                    if orders: return orders[0] 
        except Exception as e: 
            logger.error(f"Error fetching order by client ID {client_order_id[:20]}...", exc_info=True)  # SAST FIX: Cardinality noise reduction
        return {}

    async def cancel_order(self, order_id: str) -> bool:
        if order_id.startswith("paper-"): return True
        
        # SAST FIX: Escape order_id path parameters with safe='' to prevent directory traversal [4, 1]
        safe_order_id = urllib.parse.quote(order_id, safe='')
        path = f"/portfolio/orders/{safe_order_id}"
        method = "DELETE"
        current_time_ms = str(int(time.time() * 1000))
        signature = self._generate_signature(current_time_ms, method, path)
        headers = {
            "KALSHI-ACCESS-KEY": self.key_id,
            "KALSHI-ACCESS-SIGNATURE": signature,
            "KALSHI-ACCESS-TIMESTAMP": current_time_ms
        }
        try:
            async with self.session.delete(f"{self.base_url}{path}", headers=headers, timeout=self.timeout_short) as response:
                return response.status in [200, 201]
        except Exception as e: 
            logger.error(f"Error cancelling order {order_id}", exc_info=True)
            return False

    async def execute_trade(self, action: str, contract_id: str, side: str, limit_price: float, quantity: int, client_order_id: str = None) -> Optional[str]:
        if action.lower() not in self.VALID_ACTIONS or side.lower() not in self.VALID_SIDES: return None
            
        if self.paper_trade:
            order_id = f"paper-{uuid.uuid4().hex}"
            self._paper_orders[order_id] = quantity
            total_trade_value = limit_price * quantity
            if action.lower() == "buy":
                self._paper_balance -= total_trade_value
            elif action.lower() == "sell":
                self._paper_balance += total_trade_value
                
            logger.warning(f"[PAPER TRADE EXECUTED] {action.upper()} {quantity}x {contract_id} '{side.upper()}' @ ${limit_price:.2f}")
            return order_id
            
        path = "/portfolio/orders"
        method = "POST"
        current_time_ms = str(int(time.time() * 1000))
        price_cents = int(round(limit_price * 100))
        
        client_oid = client_order_id or f"bot-{current_time_ms}-{action.lower()}-{uuid.uuid4().hex}"
        
        payload = {
            "action": action.lower(),
            "client_order_id": client_oid,
            "count": quantity,
            "side": side.lower(),
            "ticker": contract_id,
            "type": "limit"
        }
        
        if side.lower() == "yes": payload["yes_price"] = price_cents
        else: payload["no_price"] = price_cents

        signature = self._generate_signature(current_time_ms, method, path)
        headers = {
            "KALSHI-ACCESS-KEY": self.key_id,
            "KALSHI-ACCESS-SIGNATURE": signature,
            "KALSHI-ACCESS-TIMESTAMP": current_time_ms,
            "Content-Type": "application/json"
        }

        try:
            async with self.session.post(f"{self.base_url}{path}", json=payload, headers=headers, timeout=self.timeout_short) as response:
                if response.status in [200, 201]:
                    data = await response.json()
                    order_id = data.get("order", {}).get("order_id")
                    if not order_id: return None
                    return order_id
                else: 
                    try:
                        err_json = await response.json()
                        err_msg = err_json.get("error", {}).get("message", "Unknown API error")
                    except Exception:
                        err_msg = "Could not parse JSON error response."
                    # SAST FIX: Truncate external API error payloads to prevent log bloat / buffer exhaustion
                    logger.error(f"[API ERROR] Trade rejected (HTTP {response.status}): {str(err_msg)[:250]}")
                    return None
        except Exception as e: 
            logger.error("Error executing trade", exc_info=True)
            return None

# ==========================================
# QUANTITATIVE PRICING ENGINE
# ==========================================
class LiveTradingEngine:
    def __init__(self, broker: ExecutionBroker):
        self.broker = broker
        self.starting_balance: float = 0.0
        self.available_balance: float = 0.0
        self.capital_in_flight: float = 0.0 
        self.consecutive_api_failures: int = 0
        self.last_sync_time: float = 0.0
        
        self.active_trade_count: int = 0
        self._binance_events_received: int = 0 
        self.shutting_down: bool = False
        
        # SAST FIX: Prevent drawdown "amnesia" on restarts [2]
        env_starting_bal = os.environ.get("STARTING_BALANCE")
        if env_starting_bal is not None:
            try:
                self.starting_balance = float(env_starting_bal)
                logger.info(f"[RISK MANAGER] Bound to absolute, restart-persistent starting balance: ${self.starting_balance:.2f}")
            except ValueError:
                logger.warning(f"Malformed STARTING_BALANCE env var: {env_starting_bal}. Falling back to dynamic initialization.")
        
        self.balance_lock = asyncio.Lock() 
        self.trade_cap_lock = asyncio.Lock()
        self.api_failure_lock = asyncio.Lock()
        
        self.assets: Dict[str, AssetState] = {
            "BTC-USD": AssetState(),
            "ETH-USD": AssetState(),
        }
        self._pending_tasks = set()

        if sys.platform != 'win32':
            # SAST FIX: Cryptographically secure, un-guessable temp file creation
            fd, self.heartbeat_file = tempfile.mkstemp(prefix="kalshi_heartbeat_", suffix=".tick")
            os.close(fd)
                
            def cleanup_heartbeat(path):
                try:
                    if os.path.exists(path):
                        os.remove(path)
                except Exception as e:
                    logger.warning(f"Failed to remove heartbeat file: {e}")
            atexit.register(cleanup_heartbeat, self.heartbeat_file)

    def _get_filled_qty_from_details(self, details: dict, requested_qty: int) -> int:
        maker_fill = details.get("maker_fill_count", 0)
        taker_fill = details.get("taker_fill_count", 0)
        exec_fill = details.get("executed_count", 0)
        
        total_fill = maker_fill + taker_fill + exec_fill
        if total_fill > 0: return total_fill
        if details.get("status") == "executed": return requested_qty
        return 0

    async def _decrement_trade_cap(self):
        async with self.trade_cap_lock:
            self.active_trade_count = max(0, self.active_trade_count - 1)

    async def shutdown(self):
        self.shutting_down = True
        if self._pending_tasks:
            logger.info(f"Draining {len(self._pending_tasks)} in-flight tasks...")
            await asyncio.wait(self._pending_tasks, timeout=30.0)
            logger.info("Task drain complete.")

    def purge_memory(self, queue: asyncio.Queue = None):
        current_time = time.time()
        for symbol, state in self.assets.items():
            state.cooldown_until = current_time + 15.0
            state.bids.clear()
            state.asks.clear()
        if queue:
            while not queue.empty():
                try: queue.get_nowait(); queue.task_done()
                except asyncio.QueueEmpty: break

    def _prune_orderbook(self, ob: dict, current_price: float, is_bid: bool, max_depth: float = config.L2_MAX_DEPTH_PCT):
        threshold_price = current_price * (1.0 - max_depth) if is_bid else current_price * (1.0 + max_depth)
        stale_keys = [p for p in ob.keys() if (p < threshold_price if is_bid else p > threshold_price)]
        for k in stale_keys:
            ob.pop(k, None)
            
        if len(ob) > 1000:
            sorted_keys = sorted(ob.keys(), reverse=is_bid)
            keys_to_drop = sorted_keys[1000:]
            for k in keys_to_drop:
                ob.pop(k, None)

    async def sync_balance_loop(self):
        try:
            self.capital_in_flight = await self.broker.get_locked_capital()
        except Exception as e: 
            logger.warning("Startup reconciliation failed to fetch locked capital", exc_info=True)

        backoff = 60
        while not self.shutting_down:
            balance_data = await self.broker.get_balance()
            if balance_data is not None:
                available_bal, portfolio_val = balance_data
                async with self.balance_lock:
                    self.available_balance = available_bal
                    self.last_sync_time = time.time()
                    
                    if self.starting_balance == 0.0:
                        # Fallback to dynamic NAV if no static environment variable is provided
                        self.starting_balance = portfolio_val
                    
                    if self.starting_balance > 0:
                        # SAST FIX: Drawdown limit checks the total NAV, preventing false-drawdowns on trades [2]
                        drawdown = (self.starting_balance - portfolio_val) / self.starting_balance
                        if drawdown >= config.DRAWDOWN_LIMIT_PCT:
                            logger.critical(f"DRAWDOWN LIMIT REACHED ({drawdown*100:.1f}%). Halting operations.")
                            self.shutting_down = True
                            
                logger.info(f"[RISK MANAGER] Wallet Synchronized | Available Cash: ${self.available_balance:.2f} | Net Asset Value: ${portfolio_val:.2f} | In-Flight: ${self.capital_in_flight:.2f}")
                backoff = 60 
                
                try:
                    if sys.platform != "win32":
                        with open(self.heartbeat_file, 'w') as f:
                            f.write(str(time.time()))
                except Exception as e: 
                    logger.warning("Failed to write heartbeat file", exc_info=True)
            else:
                backoff = min(backoff * 2, 600) 
            await asyncio.sleep(backoff)

    async def sync_markets_loop(self):
        while not self.shutting_down:
            for symbol, state in self.assets.items():
                last_price = getattr(state, 'last_price', None)
                if not last_price: continue
                    
                contract_id, strike, exp_time = await self.broker.get_active_market(symbol, last_price)
                if contract_id and state.active_contract_id != contract_id:
                    logger.info(f"[MARKET ROUTER] {symbol} Locked onto valid upcoming contract: {contract_id}")
                    state.active_contract_id = contract_id
                    state.strike_price = strike
                    state.expiration_time = exp_time 
            await asyncio.sleep(30)

    async def execute_and_hold_entry(self, state: AssetState, contract_id: str, side: str, limit_price: float, quantity: int, total_cost: float, seconds_left: float):
        # SAST FIX: Escape contract_id to prevent HTTP Request Path Manipulation [4]
        safe_contract_id = urllib.parse.quote(contract_id, safe='')
        client_entry_oid = f"entry-{safe_contract_id}-{uuid.uuid4().hex}"
        locked_capital = total_cost

        try:
            order_id = await self.broker.execute_trade(
                action="buy",
                contract_id=contract_id,
                side=side,
                limit_price=limit_price,
                quantity=quantity,
                client_order_id=client_entry_oid
            )

            if order_id:
                async with self.api_failure_lock:
                    self.consecutive_api_failures = 0
                    
                logger.info(f"[{contract_id}] BUY Order active. Verifying immediate fill...")

                filled_qty = 0
                poll_interval = 2.0
                elapsed = 0.0
                timeout = 10.0 

                while elapsed < timeout:
                    details = await self.broker.get_order_details(order_id)
                    status = details.get("status", "unknown")
                    filled_qty = self._get_filled_qty_from_details(details, quantity)

                    if filled_qty > 0 or status in ["executed", "canceled"]:
                        break

                    await asyncio.sleep(poll_interval)
                    elapsed += poll_interval

                if filled_qty > 0:
                    if filled_qty < quantity:
                        await self.broker.cancel_order(order_id)
                        unfilled_qty = quantity - filled_qty
                        refund = unfilled_qty * limit_price
                        async with self.balance_lock:
                            self.capital_in_flight = max(0.0, self.capital_in_flight - refund)
                            self.available_balance += refund
                            
                            # SAST FIX: Isolated contract positions mapping prevents state rollover contamination [2]
                            state.positions[contract_id] = max(0, state.positions.get(contract_id, 0) - unfilled_qty)
                            if state.positions[contract_id] <= 0:
                                state.position_sides.pop(contract_id, None)
                                state.positions.pop(contract_id, None)
                        locked_capital -= refund
                        logger.info(f"[{contract_id}] Partial Fill ({filled_qty}/{quantity}). Unfilled canceled & limits refunded.")
                    else:
                        logger.info(f"[{contract_id}] Order perfectly filled ({filled_qty}/{quantity}).")

                    async with self.balance_lock:
                        self.capital_in_flight = max(0.0, self.capital_in_flight - locked_capital)
                    
                    logger.warning(f"[{contract_id}] Position Secured. Execution task safely terminating.")
                    return
                else:
                    logger.warning(f"[{contract_id}] Limit buy missed fill window. Canceling.")
                    await self.broker.cancel_order(order_id)
                    async with self.balance_lock:
                        self.capital_in_flight = max(0.0, self.capital_in_flight - locked_capital)
                        self.available_balance += locked_capital
                        
                        # SAST FIX: Isolated contract positions mapping prevents state rollover contamination [2]
                        state.positions[contract_id] = max(0, state.positions.get(contract_id, 0) - quantity)
                        if state.positions[contract_id] <= 0:
                            state.position_sides.pop(contract_id, None)
                            state.positions.pop(contract_id, None)
            else:
                async with self.api_failure_lock:
                    self.consecutive_api_failures += 1
                    if self.consecutive_api_failures >= 5:
                        current_time = time.time()
                        for s in self.assets.values(): s.cooldown_until = current_time + 300.0
                        self.consecutive_api_failures = 0

                logger.critical(f"[{contract_id}] API Execution dropped. Releasing slot natively.")
                async with self.balance_lock:
                    self.capital_in_flight = max(0.0, self.capital_in_flight - locked_capital)
                    self.available_balance += locked_capital
                    
                    # SAST FIX: Isolated contract positions mapping prevents state rollover contamination [2]
                    state.positions[contract_id] = max(0, state.positions.get(contract_id, 0) - quantity)
                    if state.positions[contract_id] <= 0:
                        state.position_sides.pop(contract_id, None)
                        state.positions.pop(contract_id, None)

        except Exception as e:
            logger.critical(f"[{contract_id}] Unhandled exception in entry manager. Forcing release.", exc_info=True)
            if locked_capital > 0:
                async with self.balance_lock:
                    self.capital_in_flight = max(0.0, self.capital_in_flight - locked_capital)
                    self.available_balance += locked_capital
                    
                    # SAST FIX: Isolated contract positions mapping prevents state rollover contamination [2]
                    state.positions[contract_id] = max(0, state.positions.get(contract_id, 0) - quantity)
                    if state.positions[contract_id] <= 0:
                        state.position_sides.pop(contract_id, None)
                        state.positions.pop(contract_id, None)
            raise
        finally:
            await asyncio.shield(self._decrement_trade_cap())

    # ==========================================
    # CORE QUANTITATIVE ENGINE: Welford Mean-Reversion
    # ==========================================
    async def process_live_tick(self, raw_bytes: bytes):
        if self.shutting_down: return
        
        if self.last_sync_time == 0.0: return 
        if time.time() - self.last_sync_time > config.STALE_BALANCE_TIMEOUT_SEC: return

        try:
            parsed_dict = orjson.loads(raw_bytes)
            msg_type = parsed_dict.get("type")
        except orjson.JSONDecodeError: return

        product_id = parsed_dict.get("product_id")
        if product_id not in self.assets: return
        state = self.assets[product_id]

        if msg_type == "snapshot":
            try:
                state.bids = {float(p): float(s) for p, s in parsed_dict.get("bids", [])}
                state.asks = {float(p): float(s) for p, s in parsed_dict.get("asks", [])}
            except (ValueError, TypeError):
                state.bids.clear()
                state.asks.clear()
            return
            
        if msg_type == "l2update":
            for side_str, price_str, size_str in parsed_dict.get("changes", []):
                try:
                    price, size = float(price_str), float(size_str)
                    target_dict = state.bids if side_str == "buy" else state.asks
                    if size == 0.0: target_dict.pop(price, None)
                    else: target_dict[price] = size
                except (ValueError, TypeError): continue
            
            if state.last_price:
                if len(state.bids) > 1000: self._prune_orderbook(state.bids, state.last_price, is_bid=True)
                if len(state.asks) > 1000: self._prune_orderbook(state.asks, state.last_price, is_bid=False)
            return

        if msg_type != "ticker": return
        try: tick = TickData(**parsed_dict)
        except ValidationError: return

        current_time = time.time()
        
        # 1. Update Welford's Online Variance (O(1))
        state.welford_count += 1
        delta = tick.price - state.welford_mean
        state.welford_mean += delta / state.welford_count
        delta2 = tick.price - state.welford_mean
        state.welford_m2 += delta * delta2
        
        # 2. Update Recursive EMA Anchor (O(1))
        if state.ewma_price == 0.0:
            state.ewma_price = tick.price
        else:
            alpha = 0.01  # Approximately 200 tick half-life
            state.ewma_price = (tick.price * alpha) + (state.ewma_price * (1.0 - alpha))

        if current_time < state.cooldown_until:
            state.last_price = tick.price
            return

        last_price = state.last_price
        state.last_price = tick.price
        if not last_price: return

        # Reset Event State when Contract rolls over
        if state.active_contract_id and state.active_contract_id != state.last_seen_contract_id:
            state.last_seen_contract_id = state.active_contract_id
            async with self.balance_lock:
                # SAST FIX: Prune expired positions inside lock natively to maintain memory limits [2]
                active_ids = {state.active_contract_id}
                state.positions = {cid: val for cid, val in state.positions.items() if cid in active_ids}
                state.position_sides = {cid: val for cid, val in state.position_sides.items() if cid in active_ids}

        if state.ewma_price == 0.0 or not state.active_contract_id: return
        if state.expiration_time == 0.0: return
        seconds_left = state.expiration_time - current_time

        # Temporal Burn-in and Lock-out rules
        if seconds_left > 480.0: return 
        if seconds_left < 180.0: return 
        if state.welford_count < config.MIN_WELFORD_TICKS: return

        # 3. Calculate Z-Score for Mean-Reversion Evaluation
        variance = state.welford_m2 / state.welford_count
        std_dev = math.sqrt(variance) if variance > 0 else 0.0
        if std_dev == 0.0: return
        
        z_score = (tick.price - state.ewma_price) / std_dev
        
        # 4. Fade Logic: If price spikes heavily, buy the cheap reversion side.
        trade_side = None
        if z_score > config.Z_SCORE_THRESHOLD:
            # Price spiked artificially HIGH above anchor. Fade it.
            trade_side = "NO"
        elif z_score < -config.Z_SCORE_THRESHOLD:
            # Price flash-crashed artificially LOW below anchor. Fade it.
            trade_side = "YES"
            
        if not trade_side: return 

        executing_contract_id = state.active_contract_id
        current_pos_side = state.position_sides.get(executing_contract_id)

        if current_pos_side and current_pos_side != trade_side: return 

        async with self.trade_cap_lock:
            if self.active_trade_count >= config.MAX_CONCURRENT_TRADES: return
            self.active_trade_count += 1

        best_vals = await self.broker.get_best_bid_ask(executing_contract_id, trade_side)
        if not best_vals:
            await self._decrement_trade_cap()
            return
        
        best_bid, best_ask = best_vals
        spread = round(best_ask - best_bid, 4)
        
        # 5. The Pricing Guard: We are fading a breakout, so the side we buy must be cheap.
        if spread > config.MAX_ALLOWED_SPREAD or best_ask > config.MAX_FADE_PRICE or best_bid < 0.01:
            await self._decrement_trade_cap()
            return
        
        limit_price = max(0.01, min(0.99, best_ask))
        
        # --- SAST FIX: Avoid Lock Ordering Inversion (balance_lock -> trade_cap_lock) ---
        should_decrement = False
        async with self.balance_lock:
            # SAST FIX: Read actual position size securely inside lock to prevent race condition [2]
            actual_pos_size = state.positions.get(executing_contract_id, 0)
            remaining_exposure = config.MAX_EXPOSURE_PER_EVENT - actual_pos_size
            if remaining_exposure <= 0:
                should_decrement = True
            else:
                trade_budget = self.available_balance * config.TRADE_BUDGET_PCT
                raw_quantity = int(trade_budget / limit_price)
                quantity = min(raw_quantity, config.MAX_CONTRACTS_PER_TRADE, remaining_exposure)
                
                if quantity < 1:
                    should_decrement = True
                else:
                    total_cost = quantity * limit_price
                    self.available_balance -= total_cost
                    self.capital_in_flight += total_cost
                    
                    state.position_sides[executing_contract_id] = trade_side
                    state.positions[executing_contract_id] = actual_pos_size + quantity
                    state.cooldown_until = current_time + 15.0
        
        if should_decrement:
            await self._decrement_trade_cap()
            return
        
        logger.warning(f"[{tick.product_id}] EDGE FOUND (MEAN-REVERSION)! Z-Score: {z_score:.2f} | Ask: ${best_ask:.2f} | Fading fake breakout, sniping {quantity} '{trade_side}' contracts.")
        
        exec_task = asyncio.create_task(
            self.execute_and_hold_entry(
                state, executing_contract_id, trade_side, limit_price, quantity, total_cost, seconds_left
            )
        )
        self._pending_tasks.add(exec_task)
        
        def safe_cb(t):
            self._pending_tasks.discard(t)
            try: t.result()
            except asyncio.CancelledError: pass
            except Exception as e: logger.critical(f"Task crashed: {e}", exc_info=True)
            
        exec_task.add_done_callback(safe_cb)
        return 

    # ==========================================
    # BINANCE LIQUIDATION SNIPER
    # ==========================================
    async def process_binance_liquidation(self, raw_bytes: bytes):
        if self.shutting_down: return
        
        self._binance_events_received += 1
        if self._binance_events_received % 500 == 0:
            logger.info(f"[BINANCE FEED] {self._binance_events_received} total events received. Feed is alive & actively dropping non-qualifying orders.")
            
        try:
            parsed_dict = orjson.loads(raw_bytes)
            try:
                payload = BinancePayload(**parsed_dict)
            except ValidationError:
                return 
                
            if payload.e != "forceOrder": return
            
            symbol = payload.o.s
            if "BTC" in symbol: asset_symbol = "BTC-USD"
            elif "ETH" in symbol: asset_symbol = "ETH-USD"
            else: return
            
            state = self.assets.get(asset_symbol)
            if not state or not state.active_contract_id: return
            
            notional = payload.o.p * payload.o.q
            if notional < config.BINANCE_LIQUIDATION_THRESHOLD: return 
            
            if payload.o.S == "SELL": trade_side = "NO"
            elif payload.o.S == "BUY": trade_side = "YES"
            else: return

            # --- SAST FIX: Check Stale Balance AFTER qualifying event to avoid log spam ---
            if self.last_sync_time == 0.0 or time.time() - self.last_sync_time > config.STALE_BALANCE_TIMEOUT_SEC: 
                logger.warning(f"[{asset_symbol}] Dropping qualifying liquidation event (${notional:,.2f}) — balance data is stale.")
                return
            
            current_time = time.time()
            if current_time < state.cooldown_until: return
            
            executing_contract_id = state.active_contract_id
            current_pos_side = state.position_sides.get(executing_contract_id)

            if current_pos_side and current_pos_side != trade_side: return
            
            async with self.trade_cap_lock:
                if self.active_trade_count >= config.MAX_CONCURRENT_TRADES: return
                self.active_trade_count += 1
                
            best_vals = await self.broker.get_best_bid_ask(executing_contract_id, trade_side)
            if not best_vals:
                await self._decrement_trade_cap()
                return
                
            best_bid, best_ask = best_vals
            
            if best_ask >= 0.85 or best_bid < 0.01 or (best_ask - best_bid) > config.MAX_ALLOWED_SPREAD:
                await self._decrement_trade_cap()
                return
                
            limit_price = max(0.01, min(0.99, best_ask))
            
            # --- SAST FIX: Avoid Lock Ordering Inversion (balance_lock -> trade_cap_lock) ---
            should_decrement = False
            async with self.balance_lock:
                # SAST FIX: Read actual position size securely inside lock to prevent race condition [2]
                actual_pos_size = state.positions.get(executing_contract_id, 0)
                remaining_exposure = config.MAX_EXPOSURE_PER_EVENT - actual_pos_size
                if remaining_exposure <= 0:
                    should_decrement = True
                else:
                    trade_budget = self.available_balance * config.TRADE_BUDGET_PCT
                    raw_quantity = int(trade_budget / limit_price)
                    quantity = min(raw_quantity, config.MAX_CONTRACTS_PER_TRADE, remaining_exposure)
                    
                    if quantity < 1:
                        should_decrement = True
                    else:
                        total_cost = quantity * limit_price
                        self.available_balance -= total_cost
                        self.capital_in_flight += total_cost
                        
                        state.position_sides[executing_contract_id] = trade_side
                        state.positions[executing_contract_id] = actual_pos_size + quantity
                        state.cooldown_until = current_time + 15.0
            
            if should_decrement:
                await self._decrement_trade_cap()
                return
            
            logger.warning(f"[{asset_symbol}] BINANCE LIQUIDATION SIGNAL (${notional:,.2f})! Ask: ${best_ask:.2f} | Sniping {quantity} '{trade_side}' contracts.")
            
            seconds_left = state.expiration_time - current_time if state.expiration_time else 900.0
            
            exec_task = asyncio.create_task(
                self.execute_and_hold_entry(
                    state, executing_contract_id, trade_side, limit_price, quantity, total_cost, seconds_left
                )
            )
            self._pending_tasks.add(exec_task)
            
            def safe_cb(t):
                self._pending_tasks.discard(t)
                try: t.result()
                except asyncio.CancelledError: pass
                except Exception as e: logger.critical(f"Task crashed: {e}", exc_info=True)
                
            exec_task.add_done_callback(safe_cb)
            
        except Exception as e:
            logger.error("Liquidation processing fault", exc_info=True) 

# ==========================================
# ASYNC QUEUES (Producer-Consumer DoS Fix)
# ==========================================
async def coinbase_websocket_consumer(engine: LiveTradingEngine, queue: asyncio.Queue):
    uri = "wss://ws-feed.exchange.coinbase.com" 
    subscribe_message = {
        "type": "subscribe",
        "product_ids": ["BTC-USD", "ETH-USD"],
        "channels": ["ticker", "level2"]
    }

    attempt = 0
    while not engine.shutting_down:
        try:
            ssl_ctx = ssl.create_default_context(cafile=certifi.where())
            async with websockets.connect(uri, ssl=ssl_ctx, max_size=1048576, max_queue=256) as ws:
                logger.info("Connected to Coinbase Live Spot Feed (Ticker + Level2). Warming buffers...")
                attempt = 0  # Reset retry counter on successful connection
                await ws.send(orjson.dumps(subscribe_message).decode('utf-8'))
                
                async for message in ws:
                    if engine.shutting_down: break
                    try: queue.put_nowait(message)
                    except asyncio.QueueFull: engine.purge_memory(queue) 
                    
        except websockets.exceptions.ConnectionClosed:
            engine.purge_memory(queue) 
            attempt += 1
            delay = calculate_backoff_delay(attempt)
            logger.warning(f"Coinbase WebSocket closed. Retry attempt {attempt} in {delay:.2f}s...")
            await asyncio.sleep(delay)
        except Exception as e:
            logger.error(f"Coinbase WebSocket fault: {type(e).__name__}", exc_info=True)
            engine.purge_memory(queue) 
            attempt += 1
            delay = calculate_backoff_delay(attempt)
            logger.warning(f"Coinbase WebSocket fault retrying in {delay:.2f}s...")
            await asyncio.sleep(delay)

async def market_worker_loop(engine: LiveTradingEngine, queue: asyncio.Queue):
    while not engine.shutting_down:
        try:
            message = await asyncio.wait_for(queue.get(), timeout=1.0)
            try: await engine.process_live_tick(message)
            except Exception as e: logger.error("Tick fault", exc_info=True)
            finally: queue.task_done()
        except asyncio.TimeoutError:
            continue

async def binance_websocket_consumer(engine: LiveTradingEngine, queue: asyncio.Queue):
    uri = "wss://fstream.binance.com/ws/!forceOrder@arr"
    
    attempt = 0
    while not engine.shutting_down:
        try:
            ssl_ctx = ssl.create_default_context(cafile=certifi.where())
            async with websockets.connect(uri, ssl=ssl_ctx, max_size=1048576, max_queue=256) as ws:
                logger.info("Connected to Binance Futures Feed. Liquidation snipers online...")
                attempt = 0  # Reset retry counter on successful connection
                async for message in ws:
                    if engine.shutting_down: break
                    try: 
                        queue.put_nowait(message)
                    except asyncio.QueueFull: 
                        logger.warning("Binance queue overflow - dropping liquidation event.")
        except websockets.exceptions.ConnectionClosed:
            attempt += 1
            delay = calculate_backoff_delay(attempt)
            logger.warning(f"Binance WebSocket closed. Retry attempt {attempt} in {delay:.2f}s...")
            await asyncio.sleep(delay)
        except Exception as e:
            logger.error(f"Binance WebSocket fault: {type(e).__name__}", exc_info=True)
            attempt += 1
            delay = calculate_backoff_delay(attempt)
            logger.warning(f"Binance WebSocket fault retrying in {delay:.2f}s...")
            await asyncio.sleep(delay)

async def binance_worker_loop(engine: LiveTradingEngine, queue: asyncio.Queue):
    while not engine.shutting_down:
        try:
            message = await asyncio.wait_for(queue.get(), timeout=1.0)
            try: 
                await engine.process_binance_liquidation(message)
            except Exception as e: 
                logger.error("Binance liquidation fault", exc_info=True)
            finally: 
                queue.task_done()
        except asyncio.TimeoutError:
            continue

# ==========================================
# SECURE CREDENTIAL LOADER
# ==========================================
def get_kalshi_credentials(secret_name: str, region_name: str = "us-east-1") -> dict:
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    try:
        response = client.get_secret_value(SecretId=secret_name)
        resp_dict = orjson.loads(response['SecretString'])
        return {
            "KEY_ID": resp_dict["KEY_ID"],
            "PRIVATE_KEY": bytearray(resp_dict["PRIVATE_KEY"], 'utf-8')
        }
    except Exception as e:
        # SAST FIX: Prevent verbose AWS stack traces leaking environment metadata [4]
        logger.critical(f"Failed to retrieve secrets from AWS: {type(e).__name__} - {str(e)}")
        sys.exit(1)

# ==========================================
# BOOTSTRAPPER
# ==========================================
async def main():
    env_mode = os.environ.get("BOT_ENV", "simulation").lower()
    
    if env_mode in ["live", "paper"]:
        creds = get_kalshi_credentials("prod/kalshi/api-keys", region_name="us-east-1")
        if env_mode == "live":
            confirm = os.environ.get("LIVE_TRADING_CONFIRM", "")
            if confirm != "I_ACCEPT_FINANCIAL_RISK":
                logger.critical("Live mode blocked. Halting.")
                sys.exit(1)
            logger.warning("!!! INITIALIZING LIVE TRADING BROKER !!!")
            broker = LiveKalshiBroker(key_id=creds["KEY_ID"], private_key_pem=creds["PRIVATE_KEY"], paper_trade=False)
        else:
            logger.info("Initializing PAPER TRADING Broker. Connected to live markets, but zero financial risk.")
            broker = LiveKalshiBroker(key_id=creds["KEY_ID"], private_key_pem=creds["PRIVATE_KEY"], paper_trade=True)
    else:
        logger.info("Initializing SIMULATION Broker. Completely offline.")
        broker = SimExecutionBroker()

    await broker.start()
    engine = LiveTradingEngine(broker)
    
    if sys.platform != "win32":
        import signal
        loop = asyncio.get_running_loop()
        def _on_sigterm():
            logger.warning("SIGTERM received from OS. Initiating graceful shutdown...")
            engine.shutting_down = True
            for task in asyncio.all_tasks(loop):
                name = task.get_name()
                if name and ("consumer" in name or "worker" in name or "sync" in name):
                    task.cancel()
        loop.add_signal_handler(signal.SIGTERM, _on_sigterm)
    
    try:
        tick_queue = asyncio.Queue(maxsize=10000) 
        binance_queue = asyncio.Queue(maxsize=1000)
        
        # SAST FIX: Structured concurrency via TaskGroup replaces non-cancelling gather array [1]
        async with asyncio.TaskGroup() as tg:
            tg.create_task(engine.sync_balance_loop(), name="sync_balance")
            tg.create_task(engine.sync_markets_loop(), name="sync_markets")
            tg.create_task(coinbase_websocket_consumer(engine, tick_queue), name="consumer")
            tg.create_task(market_worker_loop(engine, tick_queue), name="worker")
            tg.create_task(binance_websocket_consumer(engine, binance_queue), name="binance_consumer")
            tg.create_task(binance_worker_loop(engine, binance_queue), name="binance_worker")
            
    except Exception as e:
        # SAST FIX: Format and flatten ExceptionGroup logs for clean CloudWatch ingest [3]
        log_exception_group(e)
    finally:
        logger.info("Executing final engine shutdown protocols...")
        await engine.shutdown()
        await broker.close()

if __name__ == "__main__":
    try: 
        asyncio.run(main())
    except KeyboardInterrupt: 
        logger.info("Bot halted manually via KeyboardInterrupt.")