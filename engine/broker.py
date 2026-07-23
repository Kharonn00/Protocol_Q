import os
import time
import base64
import asyncio
import logging
import urllib.parse
import aiohttp
import orjson
import uuid
import gc
import datetime
from decimal import Decimal, InvalidOperation
from typing import Dict, Optional, Tuple, List, Set, Any
from abc import ABC, abstractmethod
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding

from engine.config import config, GLOBAL_SSL_CONTEXT, DOLLAR_STRIKE_RE, GENERIC_NUMBER_RE
from engine.models import safe_decimal, safe_int
from engine.security import SafeResolver, sanitize_log_str

logger = logging.getLogger("KalshiQuantEngine")

# ==========================================
# INTERFACE: Execution Contract
# ==========================================
class ExecutionBroker(ABC):
    @abstractmethod
    async def start(self): pass

    @abstractmethod
    async def close(self): pass

    @abstractmethod
    async def get_balance(self) -> Optional[Tuple[Decimal, Decimal]]: pass

    @abstractmethod
    async def get_locked_capital(self) -> Decimal: pass
    
    @abstractmethod
    async def get_active_market(self, asset_symbol: str, current_price: float) -> Tuple[str, float, float]: pass

    @abstractmethod
    async def get_best_bid_ask(self, contract_id: str, side: str) -> Optional[Tuple[Decimal, Decimal, int, int]]: pass

    @abstractmethod
    async def get_order_details(self, order_id: str, **kwargs) -> dict: pass
    
    @abstractmethod
    async def get_order_by_client_id(self, client_order_id: str) -> dict: pass

    @abstractmethod
    async def cancel_order(self, order_id: str) -> bool: pass

    @abstractmethod
    async def execute_trade(self, action: str, contract_id: str, side: str, limit_price: Decimal, quantity: int, client_order_id: str = None) -> Optional[str]: pass

    @abstractmethod
    async def get_positions(self) -> Optional[Dict[str, Tuple[int, str]]]: pass

# ==========================================
# BROKERS: Simulation & Live
# ==========================================
class SimExecutionBroker(ExecutionBroker):
    def __init__(self):
        self.simulated_balance = Decimal("1000.00")
        self.positions: Dict[Tuple[str, str], int] = {} 
        self.VALID_ACTIONS = frozenset({"buy", "sell"})
        self.VALID_SIDES = frozenset({"yes", "no"})

    async def start(self):
        logger.debug("[SIMULATION] Offline Broker resources initialized.")

    async def close(self):
        logger.debug("[SIMULATION] Broker resources cleaned up.")

    async def get_balance(self) -> Optional[Tuple[Decimal, Decimal]]:
        return self.simulated_balance, self.simulated_balance

    async def get_locked_capital(self) -> Decimal:
        return Decimal("0.00")

    async def get_active_market(self, asset_symbol: str, current_price: float) -> Tuple[str, float, float]:
        base_asset = asset_symbol.split('-')[0]
        if base_asset == "BTC":
            strike = round(current_price / 50) * 50
        elif base_asset == "ETH":
            strike = round(current_price / 5) * 5
        elif base_asset == "SOL" or base_asset == "HYPE":
            strike = round(current_price)
        elif base_asset == "DOGE":
            strike = round(current_price, 4)
        else:
            strike = current_price
        contract_id = f"KX{base_asset}-15M-{strike}"
        exp_time = time.time() - (time.time() % 900) + 900
        return contract_id, float(strike), exp_time

    async def get_best_bid_ask(self, contract_id: str, side: str) -> Optional[Tuple[Decimal, Decimal, int, int]]:
        # Hardened signature implementation aligned with live tuple unpack
        return Decimal("0.40"), Decimal("0.45"), 100, 100

    async def get_order_details(self, order_id: str, **kwargs) -> dict:
        return {"status": "executed", "unfilled_count": "0"}

    async def get_order_by_client_id(self, client_order_id: str) -> dict:
        return {"status": "executed", "unfilled_count": "0"}

    async def cancel_order(self, order_id: str) -> bool:
        return True

    async def execute_trade(self, action: str, contract_id: str, side: str, limit_price: Decimal, quantity: int, client_order_id: str = None) -> Optional[str]:
        if action.lower() not in self.VALID_ACTIONS or side.lower() not in self.VALID_SIDES:
            return None
        total_value = limit_price * Decimal(quantity)
        order_id = f"sim-{uuid.uuid4().hex}"
        key = (contract_id, side.lower())
        
        if action.lower() == "buy":
            if self.simulated_balance < total_value:
                return None
            self.simulated_balance -= total_value
            self.positions[key] = self.positions.get(key, 0) + quantity
            return order_id
        elif action.lower() == "sell":
            self.simulated_balance += total_value
            new_qty = max(0, self.positions.get(key, 0) - quantity)
            if new_qty <= 0:
                self.positions.pop(key, None)
            else:
                self.positions[key] = new_qty
            return order_id
        return None

    async def get_positions(self) -> Optional[Dict[str, Tuple[int, str]]]:
        res = {}
        for (contract_id, side), qty in self.positions.items():
            if qty > 0:
                res[contract_id] = (qty, side.upper())
        return res

class LiveKalshiBroker(ExecutionBroker):
    def __init__(self, key_id: str, private_key: Any, paper_trade: bool = False):
        self.base_url = "https://external-api.kalshi.com/trade-api/v2"
        self.key_id = key_id
        self.session = None
        self.paper_trade = paper_trade
        self._paper_orders: Dict[str, dict] = {} # Upgraded schema to support high-fidelity pricing emulations
        self.paper_orders_lock = asyncio.Lock()
        self._paper_balance: Decimal = safe_decimal(os.environ.get("PAPER_BALANCE"), "1000.00")
        self.VALID_ACTIONS = frozenset({"buy", "sell"})
        self.VALID_SIDES = frozenset({"yes", "no"})
        
        # Short/Long REST timeouts to prevent worker task starvation during exchange lag
        self.timeout_short = aiohttp.ClientTimeout(total=float(os.environ.get("REST_TIMEOUT_SHORT", "0.75")))
        self.timeout_long = aiohttp.ClientTimeout(total=float(os.environ.get("REST_TIMEOUT_LONG", "1.5")))
        self.private_key = private_key
        self.rate_limited_until: float = 0.0

    async def start(self):
        resolver = SafeResolver()
        connector = aiohttp.TCPConnector(ssl=GLOBAL_SSL_CONTEXT, resolver=resolver, ttl_dns_cache=300, limit=100)
        self.session = aiohttp.ClientSession(connector=connector)

    async def close(self):
        if self.session:
            await self.session.close()
        # SEC-09: Best-effort secret scrubbing — nullify references and force
        # double GC collect to handle reference cycles in OpenSSL EVP_PKEY.
        if hasattr(self, "private_key"):
            self.private_key = None
        if hasattr(self, "key_id"):
            self.key_id = None
        gc.collect()
        gc.collect()  # Double-collect to handle cryptography lib reference cycles

    async def _read_json(self, response: aiohttp.ClientResponse, limit: int = 524288) -> Any:
        body_bytes = await response.content.read(limit)
        return orjson.loads(body_bytes)

    def _generate_signature(self, timestamp: str, method: str, path: str) -> str:
        if path.startswith("/trade-api/"):
            path_without_query = path.split('?')[0]
        else:
            signed_path = f"/trade-api/v2{path}"
            path_without_query = signed_path.split('?')[0]
        message = f"{timestamp}{method}{path_without_query}".encode('utf-8')
        
        signature = self.private_key.sign(
            message,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH
            ),
            hashes.SHA256()
        )
        return base64.b64encode(signature).decode('utf-8')

    async def get_balance(self) -> Optional[Tuple[Decimal, Decimal]]:
        if self.rate_limited_until > time.time():
            return None
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
                    data = await self._read_json(response)
                    available_balance = safe_decimal(data.get("balance", 0)) / Decimal("100.00")
                    portfolio_value = safe_decimal(data.get("portfolio_value", 0)) / Decimal("100.00")
                    return available_balance, portfolio_value
                else:
                    err_bytes = await response.content.read(1024)
                    err_text = err_bytes.decode('utf-8', errors='ignore')
                    logger.error(f"[API ERROR] Failed to fetch balance (HTTP {response.status}): {sanitize_log_str(err_text)[:250]}")
                    if response.status == 429:
                        self.rate_limited_until = time.time() + 60.0
                    return None
        except Exception as e:
            logger.error(f"[API] Error fetching balance: {type(e).__name__}", exc_info=True)
            return None

    async def get_locked_capital(self) -> Decimal:
        if self.paper_trade: return Decimal("0.00")
        
        path = "/portfolio/orders?status=resting"
        method = "GET"
        current_time_ms = str(int(time.time() * 1000))
        signature = self._generate_signature(current_time_ms, method, path)
        headers = {
            "KALSHI-ACCESS-KEY": self.key_id,
            "KALSHI-ACCESS-SIGNATURE": signature,
            "KALSHI-ACCESS-TIMESTAMP": current_time_ms
        }
        locked_capital = Decimal("0.00")
        try:
            async with self.session.get(f"{self.base_url}{path}", headers=headers, timeout=self.timeout_long) as response:
                if response.status == 200:
                    data = await self._read_json(response)
                    orders = data.get("orders", [])
                    for order in orders:
                        action = order.get("action", "").lower()
                        if action != "buy":
                            continue
                        unfilled = safe_decimal(order.get("unfilled_count", 0))
                        price_val = order.get("yes_price") or order.get("no_price")
                        price = safe_decimal(price_val, "0.00")
                        locked_capital += (unfilled * (price / Decimal("100.00")))
        except Exception as e:
            logger.error(f"[API] Error fetching resting orders: {type(e).__name__}", exc_info=True)
        return locked_capital

    async def get_positions(self) -> Optional[Dict[str, Tuple[int, str]]]:
        if self.rate_limited_until > time.time():
            return None
        if self.paper_trade:
            return None
            
        path = "/portfolio/positions"
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
                if response.status == 200:
                    data = await self._read_json(response)
                    positions_list = data.get("market_positions") or data.get("positions") or []
                    res = {}
                    for pos in positions_list:
                        ticker = pos.get("ticker") or pos.get("market_ticker")
                        if not ticker:
                            continue
                        pos_fp_str = pos.get("position_fp") or pos.get("position") or "0"
                        try:
                            pos_val = int(float(pos_fp_str))
                        except ValueError:
                            pos_val = 0
                        
                        if pos_val > 0:
                            res[ticker] = (pos_val, "YES")
                        elif pos_val < 0:
                            res[ticker] = (abs(pos_val), "NO")
                    return res
                else:
                    err_bytes = await response.content.read(1024)
                    err_text = err_bytes.decode('utf-8', errors='ignore')
                    logger.error(f"[API ERROR] Failed to fetch positions (HTTP {response.status}): {sanitize_log_str(err_text)[:250]}")
                    if response.status == 429:
                        self.rate_limited_until = time.time() + 60.0
                    return None
        except Exception as e:
            logger.error(f"[API] Error fetching positions: {type(e).__name__}", exc_info=True)
            return None

    async def get_active_market(self, asset_symbol: str, current_price: float) -> Tuple[str, float, float]:
        if self.rate_limited_until > time.time():
            return "", 0.0, 0.0
        base_asset = asset_symbol.split('-')[0]
        series_ticker = f"KX{base_asset}15M"
        
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
                if response.status != 200:
                    err_bytes = await response.content.read(1024)
                    err_text = err_bytes.decode('utf-8', errors='ignore')
                    logger.error(f"[API ERROR] Failed to fetch active market (HTTP {response.status}): {sanitize_log_str(err_text)[:250]}")
                    if response.status == 429:
                        self.rate_limited_until = time.time() + 60.0
                    return "", 0.0, 0.0
                data = await self._read_json(response, limit=1024 * 1024)
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
                                # Match numbers preceded by a dollar sign first
                                dollar_numbers = DOLLAR_STRIKE_RE.findall(subtitle)
                                if dollar_numbers:
                                    strike_val = float(dollar_numbers[-1].replace(',', ''))
                                else:
                                    # Fallback to general numbers if no dollar signs are present (take the first number as the strike)
                                    numbers = GENERIC_NUMBER_RE.findall(subtitle)
                                    if numbers:
                                        strike_val = float(numbers[0].replace(',', ''))
                                    else:
                                        strike_val = 0.0
                            except Exception:
                                strike_val = 0.0
                                
                        if base_asset == "DOGE" and strike_val > 1.0:
                            strike_val = strike_val / 100.0
                                
                        # Removed hardcoded BTC-specific strike filter to support assets like HYPE ($58)
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

    async def get_best_bid_ask(self, contract_id: str, side: str) -> Optional[Tuple[Decimal, Decimal, int, int]]:
        if self.rate_limited_until > time.time():
            return None
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
                    data = await self._read_json(resp)
                    ob_fp = data.get("orderbook_fp")
                    ob_standard = data.get("orderbook")
                    
                    if ob_fp:
                        yes_bids = ob_fp.get("yes_dollars", [])
                        no_bids = ob_fp.get("no_dollars", [])
                        valid_yes_bids = [b for b in yes_bids if isinstance(b, (list, tuple)) and len(b) >= 2]
                        valid_no_bids = [b for b in no_bids if isinstance(b, (list, tuple)) and len(b) >= 2]
                        if not valid_yes_bids or not valid_no_bids: return None
                        yes_bids_sorted = sorted(valid_yes_bids, key=lambda x: safe_decimal(x[0]))
                        no_bids_sorted = sorted(valid_no_bids, key=lambda x: safe_decimal(x[0]))
                        best_yes_bid = safe_decimal(yes_bids_sorted[-1][0])
                        best_yes_qty = safe_int(yes_bids_sorted[-1][1])
                        best_no_bid = safe_decimal(no_bids_sorted[-1][0])
                        best_no_qty = safe_int(no_bids_sorted[-1][1])
                    elif ob_standard:
                        yes_bids = ob_standard.get("yes", [])
                        no_bids = ob_standard.get("no", [])
                        valid_yes_bids = [b for b in yes_bids if isinstance(b, (list, tuple)) and len(b) >= 2]
                        valid_no_bids = [b for b in no_bids if isinstance(b, (list, tuple)) and len(b) >= 2]
                        if not valid_yes_bids or not valid_no_bids: return None
                        yes_bids_sorted = sorted(valid_yes_bids, key=lambda x: safe_decimal(x[0]))
                        no_bids_sorted = sorted(valid_no_bids, key=lambda x: safe_decimal(x[0]))
                        best_yes_bid = safe_decimal(yes_bids_sorted[-1][0]) / Decimal("100.00")
                        best_yes_qty = safe_int(yes_bids_sorted[-1][1])
                        best_no_bid = safe_decimal(no_bids_sorted[-1][0]) / Decimal("100.00")
                        best_no_qty = safe_int(no_bids_sorted[-1][1])
                    else: return None
                    
                    if side.lower() == "yes": 
                        return best_yes_bid, (Decimal("1.00") - best_no_bid), best_yes_qty, best_no_qty
                    else: 
                        return best_no_bid, (Decimal("1.00") - best_yes_bid), best_no_qty, best_yes_qty
                else:
                    if resp.status == 429:
                        self.rate_limited_until = time.time() + 60.0
                    return None
        except Exception as e: 
            if hasattr(e, 'status') and getattr(e, 'status') == 429:
                self.rate_limited_until = time.time() + 60.0
            logger.error("Orderbook fetch error", exc_info=True)
        return None

    async def get_order_details(self, order_id: str, simulate: bool = True, cached_best_vals=None, **kwargs) -> dict:
        if not order_id.startswith("paper-") and self.rate_limited_until > time.time():
            return {}
        if order_id.startswith("paper-"):
            async with self.paper_orders_lock:
                order_data = self._paper_orders.get(order_id)
                if not order_data:
                    return {}
                
                avg_price = Decimal("0.00")
                if order_data["filled_quantity"] > 0:
                    avg_price = order_data["total_cost"] / Decimal(order_data["filled_quantity"])
                else:
                    avg_price = order_data["limit_price"]
                
                if order_data["status"] == "executed":
                    return {
                        "status": "executed",
                        "executed_count": str(order_data["quantity"]),
                        "unfilled_count": "0",
                        "average_fill_price": str(avg_price)
                    }
                if order_data["status"] == "canceled":
                    return {
                        "status": "canceled",
                        "executed_count": str(order_data["filled_quantity"]),
                        "unfilled_count": str(order_data["quantity"] - order_data["filled_quantity"]),
                        "average_fill_price": str(avg_price)
                    }
                    
                if not simulate:
                    return {
                        "status": order_data["status"],
                        "executed_count": str(order_data["filled_quantity"]),
                        "unfilled_count": str(order_data["quantity"] - order_data["filled_quantity"]),
                        "average_fill_price": str(avg_price)
                    }
                
                contract_id = order_data["contract_id"]
                side = order_data["side"]
                limit_price = order_data["limit_price"]
                action = order_data["action"]
                quantity = order_data["quantity"]
            
            best_vals = cached_best_vals if cached_best_vals else await self.get_best_bid_ask(contract_id, side)
            
            async with self.paper_orders_lock:
                order_data = self._paper_orders.get(order_id)
                if not order_data:
                    return {}
                
                if order_data.get("status") == "canceled":
                    if order_data["filled_quantity"] > 0:
                        avg_price = order_data["total_cost"] / Decimal(order_data["filled_quantity"])
                    else:
                        avg_price = order_data["limit_price"]
                    return {
                        "status": "canceled",
                        "executed_count": str(order_data["filled_quantity"]),
                        "unfilled_count": str(order_data["quantity"] - order_data["filled_quantity"]),
                        "average_fill_price": str(avg_price)
                    }
                if order_data.get("status") == "executed":
                    avg_price = order_data["total_cost"] / Decimal(order_data["quantity"])
                    return {
                        "status": "executed",
                        "executed_count": str(order_data["quantity"]),
                        "unfilled_count": "0",
                        "average_fill_price": str(avg_price)
                    }
                
                filled_so_far = order_data["filled_quantity"]
                remaining = quantity - filled_so_far
                
                if best_vals:
                    best_bid, best_ask, bid_depth, ask_depth = best_vals
                    
                    new_fills = 0
                    if action == "buy" and best_ask <= limit_price:
                        new_fills = min(remaining, ask_depth)
                        if new_fills > 0:
                            slippage = Decimal(str(getattr(config, "PAPER_SLIPPAGE", "0.01")))
                            fee_rate = Decimal(str(getattr(config, "PAPER_TAKER_FEE", "0.005")))
                            slippage_price = min(Decimal("0.99"), best_ask + slippage)
                            actual_cost = Decimal(new_fills) * slippage_price + Decimal(new_fills) * fee_rate
                            
                            order_data["filled_quantity"] += new_fills
                            order_data["total_cost"] += actual_cost
                            self._paper_balance += (limit_price * Decimal(new_fills)) - actual_cost
                            if cached_best_vals is not None:
                                cached_best_vals[3] = max(0, cached_best_vals[3] - new_fills)
                            logger.warning(f"[PAPER BROKER PARTIAL] BUY fill: {new_fills}x {contract_id} '{side.upper()}' @ ${slippage_price:.2f} (Total: {order_data['filled_quantity']}/{quantity}, Fee: ${Decimal(new_fills)*fee_rate:.4f})")
                    elif action == "sell" and best_bid >= limit_price:
                        new_fills = min(remaining, bid_depth)
                        if new_fills > 0:
                            slippage = Decimal(str(getattr(config, "PAPER_SLIPPAGE", "0.01")))
                            fee_rate = Decimal(str(getattr(config, "PAPER_TAKER_FEE", "0.005")))
                            slippage_price = max(Decimal("0.01"), best_bid - slippage)
                            actual_proceeds = Decimal(new_fills) * slippage_price - Decimal(new_fills) * fee_rate
                            
                            order_data["filled_quantity"] += new_fills
                            order_data["total_cost"] += actual_proceeds
                            if cached_best_vals is not None:
                                cached_best_vals[2] = max(0, cached_best_vals[2] - new_fills)
                            self._paper_balance += actual_proceeds
                            logger.warning(f"[PAPER BROKER PARTIAL] SELL fill: {new_fills}x {contract_id} '{side.upper()}' @ ${slippage_price:.2f} (Total: {order_data['filled_quantity']}/{quantity}, Fee: ${Decimal(new_fills)*fee_rate:.4f})")
                    
                    if quantity <= 0:
                        order_data["status"] = "executed"
                        return {
                            "status": "executed",
                            "executed_count": "0",
                            "unfilled_count": "0",
                            "average_fill_price": str(limit_price)
                        }
                    if order_data["filled_quantity"] >= quantity:
                        order_data["status"] = "executed"
                        avg_price = order_data["total_cost"] / Decimal(quantity)
                        logger.warning(f"[PAPER BROKER COMPLETE] {action.upper()} order finalized for {quantity}x {contract_id}")
                        return {
                            "status": "executed",
                            "executed_count": str(quantity),
                            "unfilled_count": "0",
                            "average_fill_price": str(avg_price)
                        }
                
                if order_data["filled_quantity"] > 0:
                    avg_price = order_data["total_cost"] / Decimal(order_data["filled_quantity"])
                else:
                    avg_price = limit_price
    
                return {
                    "status": order_data["status"], 
                    "executed_count": str(order_data["filled_quantity"]), 
                    "unfilled_count": str(quantity - order_data["filled_quantity"]),
                    "average_fill_price": str(avg_price)
                }

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
                    data = await self._read_json(resp)
                    return data.get("order", {})
                else:
                    if resp.status == 429:
                        self.rate_limited_until = time.time() + 60.0
        except Exception as e: 
            if hasattr(e, 'status') and getattr(e, 'status') == 429:
                self.rate_limited_until = time.time() + 60.0
            logger.error(f"Error fetching order details for {order_id}", exc_info=True)
        return {}

    async def get_order_by_client_id(self, client_order_id: str) -> dict:
        if self.rate_limited_until > time.time():
            return {}
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
                    data = await self._read_json(resp)
                    orders = data.get("orders", [])
                    if orders: return orders[0] 
                else:
                    if resp.status == 429:
                        self.rate_limited_until = time.time() + 60.0
        except Exception as e: 
            if hasattr(e, 'status') and getattr(e, 'status') == 429:
                self.rate_limited_until = time.time() + 60.0
            logger.error(f"Error fetching order by client ID {client_order_id[:20]}...", exc_info=True)  
        return {}

    async def cancel_order(self, order_id: str) -> bool:
        if order_id.startswith("paper-"):
            async with self.paper_orders_lock:
                order_data = self._paper_orders.get(order_id)
                if order_data and order_data["status"] == "resting":
                    order_data["status"] = "canceled"
                    if order_data["action"] == "buy":
                        unfilled_qty = order_data["quantity"] - order_data["filled_quantity"]
                        if unfilled_qty > 0:
                            refund_val = order_data["limit_price"] * Decimal(unfilled_qty)
                            self._paper_balance += refund_val
                            logger.info(f"[PAPER BROKER] Cancelled {order_id}. Refunded {unfilled_qty}x @ ${order_data['limit_price']:.2f}")
                    return True
                return False
        
        if self.rate_limited_until > time.time():
            return False
            
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
                if response.status == 429:
                    self.rate_limited_until = time.time() + 60.0
                return response.status in [200, 201]
        except Exception as e: 
            if hasattr(e, 'status') and getattr(e, 'status') == 429:
                self.rate_limited_until = time.time() + 60.0
            logger.error(f"Error cancelling order {order_id}", exc_info=True)
            return False

    async def execute_trade(self, action: str, contract_id: str, side: str, limit_price: Decimal, quantity: int, client_order_id: str = None) -> Optional[str]:
        if quantity <= 0:
            logger.error(f"Invalid quantity: {quantity}")
            return None
        if action.lower() not in self.VALID_ACTIONS or side.lower() not in self.VALID_SIDES: return None
            
        if self.paper_trade:
            async with self.paper_orders_lock:
                order_id = f"paper-{uuid.uuid4().hex}"
                total_trade_value = limit_price * Decimal(quantity)
                if action.lower() == "buy":
                    if self._paper_balance < total_trade_value:
                        logger.error(f"[PAPER BROKER] Insufficient paper balance for BUY order.")
                        return None
                    self._paper_balance -= total_trade_value
                    
                if len(self._paper_orders) >= 10000:
                    stale_keys = [k for k in self._paper_orders
                                  if self._paper_orders[k].get("status") in ("executed", "canceled")]
                    if stale_keys:
                        for sk in stale_keys[:100]:
                            del self._paper_orders[sk]
                    else:
                        oldest_key = next(iter(self._paper_orders))
                        logger.warning(f"[SECURITY] FIFO evicting active paper order {oldest_key}. Potential TP monitor orphan.")
                        del self._paper_orders[oldest_key]
                    
                self._paper_orders[order_id] = {
                    "action": action.lower(),
                    "contract_id": contract_id,
                    "side": side.lower(),
                    "limit_price": limit_price,
                    "quantity": quantity,
                    "filled_quantity": 0,
                    "total_cost": Decimal("0.00"),
                    "status": "resting"
                }
            logger.warning(f"[PAPER ORDER PLACED] {action.upper()} {quantity}x {contract_id} '{side.upper()}' @ ${limit_price:.2f}")
            return order_id
            
        if self.rate_limited_until > time.time():
            return None
            
        path = "/portfolio/orders"
        method = "POST"
        current_time_ms = str(int(time.time() * 1000))
        price_cents = int(round(limit_price * Decimal("100.00")))
        
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
                    data = await self._read_json(response)
                    order_id = data.get("order", {}).get("order_id")
                    if not order_id: return None
                    return order_id
                else: 
                    try:
                        err_json = await self._read_json(response)
                        err_msg = err_json.get("error", {}).get("message", "Unknown API error")
                    except Exception:
                        err_msg = "Could not parse JSON error response."
                    logger.error(f"[API ERROR] Trade rejected (HTTP {response.status}): {sanitize_log_str(str(err_msg))[:250]}")
                    if response.status == 429:
                        self.rate_limited_until = time.time() + 60.0
                    return None
        except Exception as e: 
            logger.error("Error executing trade", exc_info=True)
            return None

def _extract_fill_price(details_dict: dict, limit_price: Decimal) -> Decimal:
    if not details_dict:
        return limit_price
    raw_avg_price = None
    for key in ("average_fill_price", "avg_price", "price"):
        val = details_dict.get(key)
        if val is not None and val != "":
            raw_avg_price = val
            break
            
    if raw_avg_price is not None:
        try:
            price_dec = Decimal(str(raw_avg_price))
            if price_dec >= Decimal("1.00"):
                return price_dec / Decimal("100.00")
            return price_dec
        except Exception:
            pass
    return limit_price
