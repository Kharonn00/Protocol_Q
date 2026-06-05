import os
import sys
import time
import math
import json
import base64
import asyncio
import logging
import aiohttp
import orjson
import websockets
import uuid
import certifi
import ssl
import ctypes
import atexit
import datetime
import collections
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
    MIN_EDGE_REQUIREMENT: float = 0.04
    MIN_PROBABILITY_THRESHOLD: float = 0.55
    STALE_BALANCE_TIMEOUT_SEC: float = 120.0
    L2_MAX_DEPTH_PCT: float = 0.05
    
    # --- SAST & ARCHITECTURE FIXES ---
    BINANCE_LIQUIDATION_THRESHOLD: float = float(os.environ.get("BINANCE_LIQ_THRESHOLD", "1500000.0"))
    MAX_ALLOWED_SPREAD: float = float(os.environ.get("MAX_ALLOWED_SPREAD", "0.25"))

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
    q: float    # Original quantity
    p: float    # Order price

class BinancePayload(BaseModel):
    e: str
    o: BinanceOrderDetails

# ==========================================
# STATE MANAGEMENT
# ==========================================
class AssetState:
    def __init__(self):
        # 1. Terminal Delta EWMA State
        self.history = collections.deque([15.0] * 10, maxlen=10)
        self.last_seen_contract_id: str = ""
        self.period_open_price: float = 0.0
        
        self.active_contract_id: str = ""
        self.strike_price: float = 0.0
        self.expiration_time: float = 0.0  
        self.last_price: Optional[float] = None
        self.cooldown_until: float = 0.0

        # 2. Implied Volatility (IV) State
        self.implied_volatility: float = 0.0
        self.last_iv_update_time: float = 0.0

        # 3. L2 Orderbook State (Strictly O(1) Bounded)
        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}
        
        # 4. Risk Management State
        self.position_side: Optional[str] = None
        self.position_size: int = 0

# ==========================================
# INTERFACE: Strict Execution Contract
# ==========================================
class ExecutionBroker(ABC):
    @abstractmethod
    async def start(self): pass

    @abstractmethod
    async def close(self): pass

    @abstractmethod
    async def get_balance(self) -> Optional[float]: pass

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

    async def get_balance(self) -> Optional[float]:
        return self.simulated_balance

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
            self.private_key = load_pem_private_key(bytes(private_key_pem), password=None)
        except Exception:
            logger.critical("Cryptographic key load failed. Halting system.")
            raise ValueError("Invalid Private Key Format")
        finally:
            ctypes.memset((ctypes.c_char * len(private_key_pem)).from_buffer(private_key_pem), 0, len(private_key_pem))

    async def start(self):
        self.session = aiohttp.ClientSession()

    async def close(self):
        if self.session:
            await self.session.close()

    def _generate_signature(self, timestamp: str, method: str, path: str) -> str:
        signed_path = f"/trade-api/v2{path}"
        message = f"{timestamp}{method}{signed_path}".encode('utf-8')
        
        # nosec B412 -- Kalshi V2 API mandates PKCS#1 v1.5
        signature = self.private_key.sign(
            message,
            padding.PKCS1v15(),
            hashes.SHA256()
        )
        return base64.b64encode(signature).decode('utf-8')

    async def get_balance(self) -> Optional[float]:
        if self.paper_trade:
            return self._paper_balance
            
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
                    return float(data.get("balance", 0)) / 100.0
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
        
        path = f"/markets?series_ticker={series_ticker}&status=open"
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
        path = f"/markets/{contract_id}/orderbook?depth=1"
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

        path = f"/portfolio/orders/{order_id}"
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
        path = f"/portfolio/orders?client_order_id={client_order_id}"
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
                    data = await resp.json()
                    orders = data.get("orders", [])
                    if orders: return orders[0] 
        except Exception as e: 
            logger.error(f"Error fetching order by client ID {client_order_id}", exc_info=True)
        return {}

    async def cancel_order(self, order_id: str) -> bool:
        if order_id.startswith("paper-"): return True
        
        path = f"/portfolio/orders/{order_id}"
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
                    logger.error(f"[API ERROR] Trade rejected (HTTP {response.status}): {err_msg}")
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
        self._binance_events_received: int = 0  # SAST FIX: Observability Counter
        self.shutting_down: bool = False
        
        self.balance_lock = asyncio.Lock() 
        self.trade_cap_lock = asyncio.Lock()
        self.api_failure_lock = asyncio.Lock()
        
        self.assets: Dict[str, AssetState] = {
            "BTC-USD": AssetState(),
            "ETH-USD": AssetState(),
        }
        self._pending_tasks = set()

        if sys.platform != 'win32':
            self.heartbeat_file = f"/tmp/kalshi_heartbeat_{os.getpid()}.tick"
            try: 
                open(self.heartbeat_file, 'a').close()
            except Exception: pass
            atexit.register(lambda: os.remove(self.heartbeat_file) if os.path.exists(self.heartbeat_file) else None)

    def _write_audit_log(self, contract_id: str, side: str, limit_price: float, quantity: int, order_id: str):
        log_entry = {
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "contract_id": contract_id,
            "side": side,
            "price": limit_price,
            "quantity": quantity,
            "order_id": order_id
        }
        try:
            with open("trades.jsonl", "a") as f:
                f.write(json.dumps(log_entry) + "\n")
        except Exception:
            logger.error("Failed to write audit log", exc_info=True)

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
            new_balance = await self.broker.get_balance()
            if new_balance is not None:
                async with self.balance_lock:
                    self.available_balance = new_balance
                    self.last_sync_time = time.time()
                    
                    if self.starting_balance == 0.0:
                        self.starting_balance = self.available_balance
                    
                    if self.starting_balance > 0:
                        drawdown = (self.starting_balance - self.available_balance) / self.starting_balance
                        if drawdown >= config.DRAWDOWN_LIMIT_PCT:
                            logger.critical(f"DRAWDOWN LIMIT REACHED ({drawdown*100:.1f}%). Halting operations.")
                            self.shutting_down = True
                            
                logger.info(f"[RISK MANAGER] Wallet Balance Synchronized: ${self.available_balance:.2f} (In-Flight: ${self.capital_in_flight:.2f})")
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

    async def sync_implied_volatility_loop(self):
        timeout = aiohttp.ClientTimeout(total=5.0)
        urls = {
            "BTC-USD": "https://www.deribit.com/api/v2/public/get_index_price?index_name=btc_dvol",
            "ETH-USD": "https://www.deribit.com/api/v2/public/get_index_price?index_name=eth_dvol"
        }
        async with aiohttp.ClientSession(timeout=timeout) as session:
            while not self.shutting_down:
                for symbol, url in urls.items():
                    try:
                        async with session.get(url) as resp:
                            if resp.status == 200:
                                data = await resp.json()
                                iv_val = float(data.get("result", {}).get("index_price", 0.0))
                                if 5.0 < iv_val < 300.0:
                                    self.assets[symbol].implied_volatility = iv_val
                                    self.assets[symbol].last_iv_update_time = time.time()
                    except Exception as e:
                        logger.warning(f"IV fetch failed for {symbol}; falling back to EWMA", exc_info=True)
                await asyncio.sleep(10.0) 

    async def execute_and_hold_entry(self, state: AssetState, contract_id: str, side: str, limit_price: float, quantity: int, total_cost: float, seconds_left: float):
        client_entry_oid = f"entry-{contract_id}-{uuid.uuid4().hex}"
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
                            # FIX: Completely restore capital to the active wallet on partial miss
                            self.available_balance += refund
                            
                            state.position_size -= unfilled_qty
                            if state.position_size <= 0:
                                state.position_side = None
                                state.position_size = 0
                        locked_capital -= refund
                        logger.info(f"[{contract_id}] Partial Fill ({filled_qty}/{quantity}). Unfilled canceled & limits refunded.")
                    else:
                        logger.info(f"[{contract_id}] Order perfectly filled ({filled_qty}/{quantity}).")

                    self._write_audit_log(contract_id, side, limit_price, filled_qty, order_id)

                    async with self.balance_lock:
                        self.capital_in_flight = max(0.0, self.capital_in_flight - locked_capital)
                    
                    logger.warning(f"[{contract_id}] Position Secured. Execution task safely terminating.")
                    return
                else:
                    logger.warning(f"[{contract_id}] Limit buy missed fill window. Canceling.")
                    await self.broker.cancel_order(order_id)
                    async with self.balance_lock:
                        self.capital_in_flight = max(0.0, self.capital_in_flight - locked_capital)
                        # FIX: Completely restore capital to the active wallet on total miss
                        self.available_balance += locked_capital
                        
                        state.position_size -= quantity
                        if state.position_size <= 0:
                            state.position_side = None
                            state.position_size = 0
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
                    # FIX: Completely restore capital to the active wallet on API drop
                    self.available_balance += locked_capital
                    
                    state.position_size -= quantity
                    if state.position_size <= 0:
                        state.position_side = None
                        state.position_size = 0

        except Exception as e:
            logger.critical(f"[{contract_id}] Unhandled exception in entry manager. Forcing release.", exc_info=True)
            if locked_capital > 0:
                async with self.balance_lock:
                    self.capital_in_flight = max(0.0, self.capital_in_flight - locked_capital)
                    # FIX: Completely restore capital to the active wallet on Exception Crash
                    self.available_balance += locked_capital
                    
                    state.position_size -= quantity
                    if state.position_size <= 0:
                        state.position_side = None
                        state.position_size = 0
            raise
        finally:
            await asyncio.shield(self._decrement_trade_cap())

    def calculate_ewma_volatility(self, history: collections.deque) -> float:
        history_list = list(history)
        ewma = history_list[0]
        alpha = 2.0 / (len(history_list) + 1)
        for val in history_list[1:]:
            ewma = (val * alpha) + (ewma * (1.0 - alpha))
        return max(15.0, ewma)

    async def process_live_tick(self, raw_bytes: bytes):
        if self.shutting_down: return
        
        if self.last_sync_time == 0.0:
            return 
            
        if time.time() - self.last_sync_time > config.STALE_BALANCE_TIMEOUT_SEC:
            logger.debug("Stale wallet balance timeout. Suppressing ticks until sync recovers.")
            return

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
                logger.warning("Malformed L2 snapshot received; orderbook cleared.")
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
        
        if current_time < state.cooldown_until:
            state.last_price = tick.price
            return

        last_price = state.last_price
        state.last_price = tick.price
        if not last_price: return

        if state.active_contract_id and state.active_contract_id != state.last_seen_contract_id:
            if state.period_open_price > 0.0:
                volatility = abs(tick.price - state.period_open_price)
                state.history.append(volatility)
            state.period_open_price = tick.price
            state.last_seen_contract_id = state.active_contract_id
            
            async with self.balance_lock:
                state.position_side = None
                state.position_size = 0

        if state.period_open_price == 0.0 or not state.active_contract_id: return
        if state.expiration_time == 0.0: return
        seconds_left = state.expiration_time - current_time

        if seconds_left > 480.0: return 
        if seconds_left < 180.0: return 

        time_since_iv = current_time - state.last_iv_update_time
        if state.implied_volatility > 0.0 and time_since_iv < 120.0:
            iv_pct = state.implied_volatility / 100.0
            time_fraction = seconds_left / 31536000.0
            remaining_vol_price = tick.price * iv_pct * math.sqrt(time_fraction)
        else:
            ewma_15m_vol = self.calculate_ewma_volatility(state.history)
            time_ratio = max(0.01, seconds_left / 900.0)
            remaining_vol_price = ewma_15m_vol * math.sqrt(time_ratio)
            
        distance = tick.price - state.strike_price
        z_score = distance / max(1e-4, remaining_vol_price) 
        base_prob_yes = 0.5 * (1.0 + math.erf(z_score / math.sqrt(2.0)))
        
        depth_threshold = tick.price * 0.001
        bid_vol = sum(size for p, size in state.bids.items() if p >= tick.price - depth_threshold)
        ask_vol = sum(size for p, size in state.asks.items() if p <= tick.price + depth_threshold)
        
        oib = 0.0
        if (bid_vol + ask_vol) > 0:
            oib = (bid_vol - ask_vol) / (bid_vol + ask_vol)
            
        prob_yes = max(0.01, min(0.99, base_prob_yes + (oib * 0.05)))
        prob_no = 1.0 - prob_yes
        
        if prob_yes > config.MIN_PROBABILITY_THRESHOLD:
            trade_side = "YES"
            fair_value = prob_yes
        elif prob_no > config.MIN_PROBABILITY_THRESHOLD:
            trade_side = "NO"
            fair_value = prob_no
        else:
            return 

        if state.position_side and state.position_side != trade_side: return 

        async with self.trade_cap_lock:
            if self.active_trade_count >= config.MAX_CONCURRENT_TRADES: return
            self.active_trade_count += 1

        executing_contract_id = state.active_contract_id 
        
        best_vals = await self.broker.get_best_bid_ask(executing_contract_id, trade_side)
        if not best_vals:
            await self._decrement_trade_cap()
            return
        
        best_bid, best_ask = best_vals
        spread = round(best_ask - best_bid, 4)
        
        if best_ask >= (fair_value - config.MIN_EDGE_REQUIREMENT):
            await self._decrement_trade_cap()
            return

        # --- SAST FIX 4: Dynamic Max Spread Guard ---
        if spread > config.MAX_ALLOWED_SPREAD or best_ask >= 0.85 or best_bid < 0.01:
            await self._decrement_trade_cap()
            return
        
        limit_price = max(0.01, min(0.99, best_ask))
        
        async with self.balance_lock:
            remaining_exposure = config.MAX_EXPOSURE_PER_EVENT - state.position_size
            if remaining_exposure <= 0:
                await self._decrement_trade_cap()
                return
                
            trade_budget = self.available_balance * config.TRADE_BUDGET_PCT
            raw_quantity = int(trade_budget / limit_price)
            quantity = min(raw_quantity, config.MAX_CONTRACTS_PER_TRADE, remaining_exposure)
            
            if quantity < 1:
                await self._decrement_trade_cap()
                return

            total_cost = quantity * limit_price
            self.available_balance -= total_cost
            self.capital_in_flight += total_cost
            
            state.position_side = trade_side
            state.position_size += quantity
            state.cooldown_until = current_time + 15.0
        
        calc_src = "IV" if (state.implied_volatility > 0.0 and time_since_iv < 120.0) else "EWMA"
        logger.warning(f"[{tick.product_id}] EDGE FOUND ({calc_src}+OIB)! FV: ${fair_value:.2f} | Ask: ${best_ask:.2f} | Sniping {quantity} '{trade_side}' contracts.")
        
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
        
        # --- SAST FIX 1: Stale Balance Guard ---
        if self.last_sync_time == 0.0:
            return 
            
        if time.time() - self.last_sync_time > config.STALE_BALANCE_TIMEOUT_SEC:
            return

        # --- SAST FIX 2: Observability Heartbeat ---
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
            if notional < config.BINANCE_LIQUIDATION_THRESHOLD:
                return 
                
            if payload.o.S == "SELL": trade_side = "NO"
            elif payload.o.S == "BUY": trade_side = "YES"
            else: return
            
            current_time = time.time()
            if current_time < state.cooldown_until: return
            
            if state.position_side and state.position_side != trade_side: return
            
            executing_contract_id = state.active_contract_id
            
            async with self.trade_cap_lock:
                if self.active_trade_count >= config.MAX_CONCURRENT_TRADES: return
                self.active_trade_count += 1
                
            best_vals = await self.broker.get_best_bid_ask(executing_contract_id, trade_side)
            if not best_vals:
                await self._decrement_trade_cap()
                return
                
            best_bid, best_ask = best_vals
            
            # --- SAST FIX 3: Dynamic Max Spread Guard ---
            if best_ask >= 0.85 or best_bid < 0.01 or (best_ask - best_bid) > config.MAX_ALLOWED_SPREAD:
                await self._decrement_trade_cap()
                return
                
            limit_price = max(0.01, min(0.99, best_ask))
            
            async with self.balance_lock:
                remaining_exposure = config.MAX_EXPOSURE_PER_EVENT - state.position_size
                if remaining_exposure <= 0:
                    await self._decrement_trade_cap()
                    return
                
                trade_budget = self.available_balance * config.TRADE_BUDGET_PCT
                raw_quantity = int(trade_budget / limit_price)
                quantity = min(raw_quantity, config.MAX_CONTRACTS_PER_TRADE, remaining_exposure)
                
                if quantity < 1:
                    await self._decrement_trade_cap()
                    return
                
                total_cost = quantity * limit_price
                self.available_balance -= total_cost
                self.capital_in_flight += total_cost
                
                state.position_side = trade_side
                state.position_size += quantity
                state.cooldown_until = current_time + 15.0
            
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

    while not engine.shutting_down:
        try:
            ssl_ctx = ssl.create_default_context(cafile=certifi.where())
            async with websockets.connect(uri, ssl=ssl_ctx, max_size=1048576, max_queue=256) as ws:
                logger.info("Connected to Coinbase Live Spot Feed (Ticker + Level2). Warming buffers...")
                await ws.send(orjson.dumps(subscribe_message).decode('utf-8'))
                
                async for message in ws:
                    if engine.shutting_down: break
                    try: queue.put_nowait(message)
                    except asyncio.QueueFull: engine.purge_memory(queue) 
                    
        except websockets.exceptions.ConnectionClosed:
            engine.purge_memory(queue) 
            await asyncio.sleep(1)
        except Exception as e:
            logger.error("Websocket fault", exc_info=True)
            engine.purge_memory(queue) 
            await asyncio.sleep(1)

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
    while not engine.shutting_down:
        try:
            ssl_ctx = ssl.create_default_context(cafile=certifi.where())
            async with websockets.connect(uri, ssl=ssl_ctx, max_size=1048576, max_queue=256) as ws:
                logger.info("Connected to Binance Futures Feed. Liquidation snipers online...")
                async for message in ws:
                    if engine.shutting_down: break
                    try: 
                        queue.put_nowait(message)
                    except asyncio.QueueFull: 
                        logger.warning("Binance queue overflow - dropping liquidation event.")
        except Exception as e:
            logger.error("Binance Websocket fault", exc_info=True)
            await asyncio.sleep(1)

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
    except ClientError as e:
        logger.critical("Failed to retrieve secrets from AWS.", exc_info=True)
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
        
        tasks = [
            asyncio.create_task(engine.sync_balance_loop(), name="sync_balance"),
            asyncio.create_task(engine.sync_markets_loop(), name="sync_markets"),
            asyncio.create_task(coinbase_websocket_consumer(engine, tick_queue), name="consumer"),
            asyncio.create_task(market_worker_loop(engine, tick_queue), name="worker"),
            asyncio.create_task(binance_websocket_consumer(engine, binance_queue), name="binance_consumer"),
            asyncio.create_task(binance_worker_loop(engine, binance_queue), name="binance_worker")
        ]
        
        if env_mode in ["live", "paper"]:
            tasks.append(asyncio.create_task(engine.sync_implied_volatility_loop(), name="sync_implied_vol"))
            
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass
    finally:
        logger.info("Executing final engine shutdown protocols...")
        await engine.shutdown()
        await broker.close()

if __name__ == "__main__":
    try: 
        asyncio.run(main())
    except KeyboardInterrupt: 
        logger.info("Bot halted manually via KeyboardInterrupt.")