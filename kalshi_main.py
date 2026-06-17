import os
import sys
import time
import kalshi_bot
import re
import math
import base64
import asyncio
import logging
import urllib.parse
import aiohttp
from aiohttp.abc import AbstractResolver
from aiohttp import web
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
import ipaddress
import socket
import gc
from functools import lru_cache
from decimal import Decimal, InvalidOperation, ROUND_UP
from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple, List, Set, Any
from abc import ABC, abstractmethod
from pydantic import BaseModel, Field, ValidationError

# AWS SDK for Secure Secret Management
import boto3
from botocore.exceptions import ClientError

# Cryptographic primitives for Kalshi V2 protocol signing
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.serialization import load_pem_private_key

# Optimize Garbage Collection thresholds to reduce latency jitter in high-frequency trading loops
gc.set_threshold(7000, 10, 10)

# ==========================================
# CONFIGURATION & LOGGING
# ==========================================
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s - %(message)s')
logger = logging.getLogger("KalshiQuantEngine")

@dataclass
class BotConfig:
    MAX_CONCURRENT_TRADES: int = 2
    TRADE_BUDGET_PCT: Decimal = Decimal(os.environ.get("TRADE_BUDGET_PCT", "0.03"))
    MAX_CONTRACTS_PER_TRADE: int = 100
    MAX_EXPOSURE_PER_EVENT: int = 100
    
    DRAWDOWN_LIMIT_PCT: Decimal = Decimal(os.environ.get("DRAWDOWN_LIMIT_PCT", "0.20"))
    STALE_BALANCE_TIMEOUT_SEC: float = 120.0
    
    BINANCE_LIQUIDATION_THRESHOLDS: Dict[str, Decimal] = field(default_factory=lambda: {
        "BTC-USD": Decimal(os.environ.get("BINANCE_LIQ_THRESHOLD_BTC", "1500000.0")),
        "HYPE-USD": Decimal(os.environ.get("BINANCE_LIQ_THRESHOLD_HYPE", "100000.0")),
        "SOL-USD": Decimal(os.environ.get("BINANCE_LIQ_THRESHOLD_SOL", "100000.0")),
        "ETH-USD": Decimal(os.environ.get("BINANCE_LIQ_THRESHOLD_ETH", "750000.0")),
        "DOGE-USD": Decimal(os.environ.get("BINANCE_LIQ_THRESHOLD_DOGE", "300000.0"))
    })
    MAX_ALLOWED_SPREAD: Decimal = Decimal(os.environ.get("MAX_ALLOWED_SPREAD", "0.25"))

    Z_SCORE_THRESHOLD: float = float(os.environ.get("Z_SCORE_THRESHOLD", "2.5"))
    MIN_EMA_TICKS: int = int(os.environ.get("MIN_EMA_TICKS", "1000"))
    MAX_FADE_PRICE: Decimal = Decimal(os.environ.get("MAX_FADE_PRICE", "0.52"))
    MAX_PRICE_DEVIATION_PCT: float = 0.15      
    CONSECUTIVE_OUTLIER_LIMIT: int = 5         
    STD_DEV_FLOOR: float = 0.05                

    LOCKOUT_BEFORE_SEC: float = float(os.environ.get("LOCKOUT_BEFORE_SEC", "1800.0"))
    LOCKOUT_AFTER_SEC: float = float(os.environ.get("LOCKOUT_AFTER_SEC", "1800.0"))
    
    TELEMETRY_LOG_INTERVAL_SEC: float = float(os.environ.get("TELEMETRY_LOG_INTERVAL_SEC", "300.0"))
    EMA_ALPHA: Decimal = Decimal(os.environ.get("EMA_ALPHA", "0.015"))
    
    # Dynamic TP Range parameters
    MIN_TP_ROI: Decimal = Decimal(os.environ.get("MIN_TP_ROI", "1.50"))
    MAX_TP_ROI: Decimal = Decimal(os.environ.get("MAX_TP_ROI", "1.85"))
    
    # Safety indicators thresholds
    STRIKE_SAFETY_BUFFER_SD: float = float(os.environ.get("STRIKE_SAFETY_BUFFER_SD", "0.5"))
    RSI_OVERBOUGHT_THRESHOLD: float = float(os.environ.get("RSI_OVERBOUGHT_THRESHOLD", "70.0"))
    RSI_OVERSOLD_THRESHOLD: float = float(os.environ.get("RSI_OVERSOLD_THRESHOLD", "30.0"))
    TREND_ALIGNMENT_THRESHOLD: float = float(os.environ.get("TREND_ALIGNMENT_THRESHOLD", "0.0"))
    
    # 88% ROI Take-Profit Trap. Entry cost multiplied by 1.88.
    TAKE_PROFIT_ROI: Decimal = Decimal(os.environ.get("TAKE_PROFIT_ROI", "1.88"))

config = BotConfig()

# Prevent redundant openSSL memory consumption by reusing context
GLOBAL_SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())

# SEC-01 Whitelist: Whitelist is only populated if explicitly defined in env variables
TRUSTED_INTERNAL_HOSTS = frozenset(
    h.strip().lower() for h in os.environ.get(
        "TRUSTED_INTERNAL_HOSTS", ""
    ).split(",") if h.strip()
)

# Precompiled regex patterns to maintain O(1) heap allocations in hot loops
DOLLAR_STRIKE_RE = re.compile(r'\$\s*(\d+(?:,\d+)*(?:\.\d+)?)')
GENERIC_NUMBER_RE = re.compile(r'\d+(?:,\d+)*(?:\.\d+)?')

# ==========================================
# SECURITY SANITIZATION & CACHING
# ==========================================
def sanitize_log_str(val: str) -> str:
    """Sanitizes strings prior to logging to prevent Log Injection attacks (CWE-117)."""
    return val.replace('\n', '\\n').replace('\r', '\\r')

@lru_cache(maxsize=1024)
def is_private_ip(ip_str: str) -> bool:
    """Optimized and cached private IP lookup to prevent connection overhead."""
    try:
        ip_addr = ipaddress.ip_address(ip_str)
        # Unpack IPv4-mapped IPv6 addresses (e.g. ::ffff:127.0.0.1)
        if isinstance(ip_addr, ipaddress.IPv6Address) and ip_addr.ipv4_mapped is not None:
            ip_addr = ip_addr.ipv4_mapped
        return (
            ip_addr.is_private or 
            ip_addr.is_loopback or 
            ip_addr.is_link_local or 
            ip_addr.is_multicast or 
            ip_addr.is_unspecified
        )
    except ValueError:
        return True  # Block malformed format matches by default

def safe_drain_queue(queue: asyncio.Queue) -> None:
    """
    Safely drains all elements from the queue and synchronizes 
    unfinished task counters to prevent internal asyncio state drift.
    """
    while True:
        try:
            queue.get_nowait()
            try:
                queue.task_done()
            except ValueError:
                pass
        except asyncio.QueueEmpty:
            break

# ==========================================
# SECURITY: Zero-Zero Payload Validation
# ==========================================
class EconomicEvent(BaseModel):
    event: str = Field(..., max_length=100, pattern=r"^[a-zA-Z0-9\s\-\(\)\%\.\/]+$")
    timestamp: float = Field(..., gt=0)
    impact: str = Field(..., pattern="^(HIGH|MEDIUM|LOW)$")

class EconomicCalendarResponse(BaseModel):
    events: List[EconomicEvent]

def validate_tick_data(data: dict) -> Optional[dict]:
    """Optimized validation of Coinbase tickers to bypass Pydantic overhead."""
    if data.get("type") != "ticker":
        return None

    prod_id = data.get("product_id")
    if prod_id not in ("BTC-USD", "HYPE-USD", "SOL-USD", "ETH-USD", "DOGE-USD"):
        return None
    
    raw_price = data.get("price")
    if raw_price is None:
        return None
    try:
        price_dec = Decimal(str(raw_price))
        if not (Decimal("0.01") < price_dec < Decimal("199999.0")):
            return None
    except (ValueError, TypeError, InvalidOperation):
        return None
        
    try:
        tick_volume = float(data.get("last_size", 0)) if data.get("last_size") else 0.0
    except (ValueError, TypeError):
        tick_volume = 0.0

    return {
        "product_id": prod_id,
        "price": price_dec,
        "volume": tick_volume
    }

def validate_binance_payload(data: dict) -> Optional[dict]:
    """Optimized validation of Binance liquidations to bypass Pydantic overhead."""
    if data.get("e") != "forceOrder":
        return None
    o = data.get("o")
    if not isinstance(o, dict):
        return None
    
    symbol = o.get("s")
    if not isinstance(symbol, str):
        return None
        
    side = o.get("S")
    if side not in ("BUY", "SELL"):
        return None
        
    try:
        q = Decimal(str(o.get("q")))
        p = Decimal(str(o.get("p")))
        notional = q * p
        if not (Decimal("0.00001") < q):
            return None
        if not (Decimal("0.01") < p < Decimal("200000.0")):
            return None
        if not (Decimal("10.0") < notional < Decimal("1000000000.0")):
            return None
    except (ValueError, TypeError, InvalidOperation):
        return None
        
    return {
        "o": {
            "s": symbol,
            "S": side,
            "q": q,
            "p": p
        }
    }

# ==========================================
# STATE MANAGEMENT
# ==========================================
class AssetState:
    def __init__(self):
        self.active_contract_id: str = ""
        self.strike_price: float = 0.0
        self.expiration_time: float = 0.0  
        self.last_price: Optional[float] = None
        self.cooldown_until: float = 0.0
        self.last_seen_contract_id: str = ""
        self.last_traded_event: str = ""

        # Transitioned to O(1) Exponential Variables
        self.tick_count: int = 0
        self.fast_indicators = kalshi_bot.FastIndicators(14, float(config.EMA_ALPHA))
        self.consecutive_outliers: int = 0  
        
        # Restored to satisfy strict interface contracts and prevent AttributeError
        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}
        
        self.positions: Dict[str, int] = {}       
        self.position_sides: Dict[str, str] = {}  

class PerformanceTracker:
    """O(1) rolling performance tracker per asset/hour-of-day to auto-throttle losing regimes."""
    def __init__(self):
        self.outcomes: Dict[Tuple[str, int], List] = {}  # (asset, hour) -> [wins, losses, total_pnl]

    def _clean_asset(self, asset_id: str) -> str:
        clean = asset_id.upper()
        if clean.startswith("KX-"):
            clean = clean[3:]
        elif clean.startswith("KX"):
            clean = clean[2:]
        parts = clean.split('-')
        symbol = parts[0] if parts else clean
        match = re.match(r'^[A-Z]+', symbol)
        return match.group(0) if match else symbol

    def record(self, asset: str, hour: int, won: bool, pnl: float):
        key = (self._clean_asset(asset), hour)
        if key not in self.outcomes:
            self.outcomes[key] = [0, 0, 0.0]
        self.outcomes[key][0 if won else 1] += 1
        self.outcomes[key][2] += pnl

    def should_trade(self, asset: str, hour: int, min_samples: int = 5) -> bool:
        key = (self._clean_asset(asset), hour)
        data = self.outcomes.get(key)
        if not data or (data[0] + data[1]) < min_samples:
            return True  # Not enough data, allow trading
        win_rate = data[0] / (data[0] + data[1])
        return win_rate > 0.35  # Require at least 35% win rate

# ==========================================
# TYPE-SAFE FINANCIAL PARSING
# ==========================================
def safe_decimal(val, default_val: str = "0.00") -> Decimal:
    """Safely converts input to Decimal, avoiding runtime type crashes."""
    if val is None:
        return Decimal(default_val)
    if type(val) is Decimal:
        return val
    try:
        if type(val) is float:
            return Decimal(str(val)) # Required for floats to prevent IEEE 754 drift
        return Decimal(val) # Natively handles strings and integers with 0 allocations
    except (ValueError, TypeError, InvalidOperation):
        return Decimal(default_val)

def safe_int(val, default_val: int = 0) -> int:
    """Safely converts input to integer, handling strings representing floats, avoiding runtime type crashes."""
    if val is None:
        return default_val
    if type(val) is int:
        return val
    try:
        return int(val) # Fast path: native string integer conversion without precision loss
    except (ValueError, OverflowError):
        try:
            return int(float(val)) # Fallback: handles floats or string-floats ("100.5")
        except (ValueError, TypeError, OverflowError):
            return default_val
    except TypeError:
        return default_val

# ==========================================
# SECURE NETWORK HOOKS (SSRF & DNS REBINDING DEFENSE)
# ==========================================
class SafeResolver(AbstractResolver):
    """
    Enforces SSRF boundary verification dynamically during active DNS resolution,
    neutralizing Time-of-Check to Time-of-Use (TOCTOU) DNS Rebinding exploits,
    and supports application-level static domain-to-IP overrides.
    """
    def __init__(self):
        self._resolver = aiohttp.DefaultResolver()

    async def resolve(self, host: str, port: int = 0, family: int = socket.AF_INET) -> List[Dict]:
        host_lower = host.lower()
        
        records = await self._resolver.resolve(host, port, family)
        safe_records = []
        
        is_trusted_host = host_lower in TRUSTED_INTERNAL_HOSTS
        
        for record in records:
            ip_str = record.get('host', '')
            if is_trusted_host:
                safe_records.append(record)
            elif not is_private_ip(ip_str):
                safe_records.append(record)
            else:
                logger.error(f"[SECURITY] DNS resolution to private space blocked for untrusted target '{host}': {sanitize_log_str(ip_str)}")
        
        if not safe_records:
            raise OSError(f"Access denied: Target host '{host}' resolved exclusively to restricted addresses.")
        return safe_records

    async def close(self):
        await self._resolver.close()

async def is_safe_destination_async(url_str: str) -> bool:
    """Asynchronously evaluates targets to avoid event loop blockages."""
    try:
        parsed = urllib.parse.urlparse(url_str)
        if parsed.scheme != "https":
            return False
        hostname = parsed.hostname
        if not hostname:
            return False
        
        hostname_lower = hostname.lower()
        
        is_trusted_host = hostname_lower in TRUSTED_INTERNAL_HOSTS
        if is_trusted_host:
            return True
            
        loop = asyncio.get_running_loop()
        addr_info = await loop.getaddrinfo(hostname, None)
        for family, _, _, _, sockaddr in addr_info:
            ip_str = sockaddr[0]
            if is_private_ip(ip_str):
                return False
        return True
    except Exception:
        return False

# ==========================================
# MACROECONOMIC CIRCUIT BREAKER
# ==========================================
class MacroCircuitBreaker:
    def __init__(self, lockout_before_sec: float = 1800.0, lockout_after_sec: float = 1800.0):
        self.lockout_before = lockout_before_sec
        self.lockout_after = lockout_after_sec
        self.active_events: List[EconomicEvent] = []
        self.calendar_url = os.environ.get("ECONOMIC_CALENDAR_URL", "")
        self._was_locked_out: bool = False

    def is_locked_out(self) -> bool:
        current_time = time.time()
        locked = False
        active_event_name = ""
        
        for ev in self.active_events:
            if ev.impact == "HIGH":
                start_lock = ev.timestamp - self.lockout_before
                end_lock = ev.timestamp + self.lockout_after
                if start_lock <= current_time <= end_lock:
                    locked = True
                    active_event_name = ev.event
                    break

        if locked:
            if not self._was_locked_out:
                logger.warning(f"[CIRCUIT BREAKER] Hard Lockout active near high-impact economic event: '{active_event_name}'. All entries blocked.")
                self._was_locked_out = True
        else:
            if self._was_locked_out:
                logger.warning("[CIRCUIT BREAKER] Locked macroeconomic window resolved. Trading systems reactivated.")
                self._was_locked_out = False

        return locked

    async def fetch_calendar(self, session: aiohttp.ClientSession) -> bool:
        if not self.calendar_url:
            logger.debug("[CIRCUIT BREAKER] No calendar URL configured. Skipping sync.")
            return False

        # 1. SSRF Network Security Boundary Check
        if not await is_safe_destination_async(self.calendar_url):
            logger.error("[CIRCUIT BREAKER] Aborting calendar fetch. Target fails boundary rules.")
            return False

        # 2. Transparent Service Identification (ToS Compliant)
        headers = {
            "User-Agent": "KalshiQuantEngine/1.0",
            "Accept": "application/json"
        }

        try:
            timeout = aiohttp.ClientTimeout(total=5.0)
            async with session.get(self.calendar_url, headers=headers, timeout=timeout, allow_redirects=False) as response:
                if response.status != 200:
                    logger.error(f"[CIRCUIT BREAKER] Calendar endpoint returned status: {response.status}")
                    return False

                # Protect against compression bombs (CWE-409)
                content_length = response.headers.get("Content-Length")
                if content_length is not None:
                    try:
                        if int(content_length) > 512 * 1024:
                            logger.error("[CIRCUIT BREAKER] Aborting calendar parsing. Payload size exceeds safe limits (512 KB).")
                            return False
                    except ValueError:
                        pass

                body_bytes = bytearray()
                while True:
                    chunk = await response.content.read(65536)
                    if not chunk:
                        break
                    body_bytes.extend(chunk)
                    if len(body_bytes) > 512 * 1024:
                        logger.error("[CIRCUIT BREAKER] Ingestion aborted. Calendar buffer length exceeded maximum limits.")
                        return False
                body_bytes = bytes(body_bytes)

                # Diagnostic & Hardened Parsing
                try:
                    parsed_json = orjson.loads(body_bytes)
                except orjson.JSONDecodeError:
                    import json
                    try:
                        parsed_json = json.loads(body_bytes.decode('utf-8', errors='ignore'), strict=False)
                    except json.JSONDecodeError as json_e:
                        preview = body_bytes[:250].decode('utf-8', errors='ignore')
                        logger.error(f"[CIRCUIT BREAKER] Invalid JSON structure. Error: {json_e}. Preview: {sanitize_log_str(preview)}")
                        return False

                if not isinstance(parsed_json, list):
                    logger.error("[CIRCUIT BREAKER] Expected JSON list from calendar API.")
                    return False
                
                # Schema Validation & Mapping (With USD Filter & Lookahead Bound)
                mapped_events = []
                current_time = time.time()
                # SEC-03: Max 7-day lookahead to prevent logic-level DoS
                lookahead_limit = current_time + 604800.0 

                for item in parsed_json:
                    try:
                        country = str(item.get("country", "")).upper()
                        if country != "USD":
                            continue

                        raw_date = item.get("date", "")
                        if not raw_date:
                            continue
                        
                        dt = datetime.datetime.fromisoformat(raw_date)
                        timestamp = dt.timestamp()
                        
                        # Apply sanity bounds to timestamps
                        if timestamp < current_time - self.lockout_after or timestamp > lookahead_limit:
                            continue

                        raw_event = item.get("title", "Economic Release")
                        clean_event = "".join(c for c in raw_event if c.isalnum() or c in " -()%./")
                        clean_event = clean_event[:100]
                        if not clean_event:
                            clean_event = "Economic Release"
                            
                        impact = str(item.get("impact", "LOW")).upper()
                        if impact not in ("HIGH", "MEDIUM", "LOW"):
                            impact = "LOW"
                            
                        mapped_events.append({
                            "event": clean_event,
                            "timestamp": timestamp,
                            "impact": impact
                        })
                    except Exception as parse_err:
                        logger.warning(
                            f"[CIRCUIT BREAKER] Failed parsing individual calendar event: "
                            f"{sanitize_log_str(str(parse_err))}"
                        )
                        continue
                
                reconstructed_response = {"events": mapped_events}
                validated_data = EconomicCalendarResponse(**reconstructed_response)
                self.active_events = validated_data.events
                return True
        except ValidationError as e:
            logger.error(f"[CIRCUIT BREAKER] Economic calendar structure schema mismatch: {sanitize_log_str(str(e))[:150]}")
            return False
        except Exception as e:
            logger.error(f"[CIRCUIT BREAKER] Synchronization channel failure: {type(e).__name__}")
            return False

# ==========================================
# GENERAL UTILITIES
# ==========================================
def calculate_backoff_delay(attempt: int, base: float = 1.0, max_delay: float = 60.0) -> float:
    delay = min(max_delay, base * (2.0 ** (attempt - 1)))
    jitter = random.uniform(0.0, 1.0)
    return delay + jitter

def log_exception_group(eg: BaseException):
    if hasattr(eg, 'exceptions'):
        for exc in eg.exceptions:
            log_exception_group(exc)
    else:
        logger.critical(f"TaskGroup sub-exception: {type(eg).__name__} - {sanitize_log_str(str(eg))}")

# ==========================================
# SECURE INTER-PROCESS HEALTH CHECKS
# ==========================================
async def handle_health_check(request: web.Request) -> web.Response:
    return web.json_response(
        {"status": "ok", "service": "kalshi-quant-engine"},
        dumps=lambda x: orjson.dumps(x).decode('utf-8')
    )

async def start_health_server() -> web.AppRunner:
    app = web.Application()
    app.router.add_get('/', handle_health_check)
    app.router.add_get('/health', handle_health_check)
    
    runner = web.AppRunner(app)
    await runner.setup()
    
    port = int(os.environ.get("PORT", "8080"))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    logger.debug(f"[HEALTH SERVER] Micro HTTP health responder online, listening on port {port}")
    return runner

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
        strike = round(current_price / 50) * 50
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
        self._paper_balance: Decimal = safe_decimal(os.environ.get("PAPER_BALANCE"), "1000.00")
        self.VALID_ACTIONS = frozenset({"buy", "sell"})
        self.VALID_SIDES = frozenset({"yes", "no"})
        
        self.timeout_short = aiohttp.ClientTimeout(total=2.0)
        self.timeout_long = aiohttp.ClientTimeout(total=3.0)
        self.private_key = private_key

    async def start(self):
        resolver = SafeResolver()
        connector = aiohttp.TCPConnector(ssl=GLOBAL_SSL_CONTEXT, resolver=resolver)
        self.session = aiohttp.ClientSession(connector=connector)

    async def close(self):
        if self.session:
            await self.session.close()
        if hasattr(self, "private_key"):
            self.private_key = None
        if hasattr(self, "key_id"):
            self.key_id = None
        gc.collect()

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
                        unfilled = safe_decimal(order.get("unfilled_count", 0))
                        price_val = order.get("yes_price") or order.get("no_price")
                        price = safe_decimal(price_val, "0.00")
                        locked_capital += (unfilled * (price / Decimal("100.00")))
        except Exception as e:
            logger.error(f"[API] Error fetching resting orders: {type(e).__name__}", exc_info=True)
        return locked_capital

    async def get_positions(self) -> Optional[Dict[str, Tuple[int, str]]]:
        if self.paper_trade:
            return {}
            
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
                    return None
        except Exception as e:
            logger.error(f"[API] Error fetching positions: {type(e).__name__}", exc_info=True)
            return None

    async def get_active_market(self, asset_symbol: str, current_price: float) -> Tuple[str, float, float]:
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
                                    # Fallback to general numbers if no dollar signs are present
                                    numbers = GENERIC_NUMBER_RE.findall(subtitle)
                                    if numbers:
                                        strike_val = float(numbers[-1].replace(',', ''))
                                    else:
                                        strike_val = 0.0
                            except Exception:
                                strike_val = 0.0
                                
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
                        if not yes_bids or not no_bids: return None
                        best_yes_bid = safe_decimal(yes_bids[0][0])
                        best_yes_qty = safe_int(yes_bids[0][1])
                        best_no_bid = safe_decimal(no_bids[0][0])
                        best_no_qty = safe_int(no_bids[0][1])
                    elif ob_standard:
                        yes_bids = ob_standard.get("yes", [])
                        no_bids = ob_standard.get("no", [])
                        if not yes_bids or not no_bids: return None
                        best_yes_bid = safe_decimal(yes_bids[0][0]) / Decimal("100.00")
                        best_yes_qty = safe_int(yes_bids[0][1])
                        best_no_bid = safe_decimal(no_bids[0][0]) / Decimal("100.00")
                        best_no_qty = safe_int(no_bids[0][1])
                    else: return None
                    
                    if side.lower() == "yes": 
                        # Best Yes Bid is the highest Yes price someone is buying at.
                        # Best Yes Ask is 1.00 - Best No Bid (someone buying No is selling Yes).
                        return best_yes_bid, (Decimal("1.00") - best_no_bid), best_yes_qty, best_no_qty
                    else: 
                        # Best No Bid is the highest No price someone is buying at.
                        # Best No Ask is 1.00 - Best Yes Bid.
                        return best_no_bid, (Decimal("1.00") - best_yes_bid), best_no_qty, best_yes_qty
        except Exception as e: 
            logger.error("Orderbook fetch error", exc_info=True)
        return None

    async def get_order_details(self, order_id: str, simulate: bool = True, cached_best_vals=None, **kwargs) -> dict:
        if order_id.startswith("paper-"):
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

            # High-Fidelity Paper Trading Simulation Engine: Query the real exchange orderbook 
            # to verify if our resting paper limit order has legitimately met execution boundaries.
            contract_id = order_data["contract_id"]
            side = order_data["side"]
            limit_price = order_data["limit_price"]
            action = order_data["action"]
            quantity = order_data["quantity"]
            filled_so_far = order_data["filled_quantity"]
            remaining = quantity - filled_so_far
            
            best_vals = cached_best_vals if cached_best_vals else await self.get_best_bid_ask(contract_id, side)
            if order_data.get("status") == "canceled":
                return {
                    "status": "canceled",
                    "executed_count": str(order_data["filled_quantity"]),
                    "unfilled_count": str(order_data["quantity"] - order_data["filled_quantity"]),
                    "average_fill_price": str(avg_price)
                }

            if best_vals:
                best_bid, best_ask, bid_depth, ask_depth = best_vals
                
                new_fills = 0
                if action == "buy" and best_ask <= limit_price:
                    # We are buying, so we consume the ASK depth.
                    new_fills = min(remaining, ask_depth)
                    if new_fills > 0:
                        order_data["filled_quantity"] += new_fills
                        order_data["total_cost"] += Decimal(new_fills) * best_ask
                        # Refund price improvement
                        self._paper_balance += (limit_price - best_ask) * Decimal(new_fills)
                        if cached_best_vals is not None:
                            cached_best_vals[3] -= new_fills
                        logger.warning(f"[PAPER BROKER PARTIAL] BUY fill: {new_fills}x {contract_id} '{side.upper()}' @ ${best_ask:.2f} (Total: {order_data['filled_quantity']}/{quantity})")
                elif action == "sell" and best_bid >= limit_price:
                    # We are selling, so we consume the BID depth.
                    new_fills = min(remaining, bid_depth)
                    if new_fills > 0:
                        order_data["filled_quantity"] += new_fills
                        order_data["total_cost"] += Decimal(new_fills) * best_bid
                        if cached_best_vals is not None:
                            cached_best_vals[2] -= new_fills
                        # Credit actual execution price (best_bid)
                        total_credit = best_bid * Decimal(new_fills)
                        self._paper_balance += total_credit
                        logger.warning(f"[PAPER BROKER PARTIAL] SELL fill: {new_fills}x {contract_id} '{side.upper()}' @ ${best_bid:.2f} (Total: {order_data['filled_quantity']}/{quantity})")
                
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
        except Exception as e: 
            logger.error(f"Error fetching order details for {order_id}", exc_info=True)
        return {}

    async def get_order_by_client_id(self, client_order_id: str) -> dict:
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
        except Exception as e: 
            logger.error(f"Error fetching order by client ID {client_order_id[:20]}...", exc_info=True)  
        return {}

    async def cancel_order(self, order_id: str) -> bool:
        if order_id.startswith("paper-"):
            order_data = self._paper_orders.get(order_id)
            if order_data and order_data["status"] == "resting":
                order_data["status"] = "canceled"
                if order_data["action"] == "buy":
                    # Refund ONLY the unfilled portion of the locked paper balance
                    unfilled_qty = order_data["quantity"] - order_data["filled_quantity"]
                    if unfilled_qty > 0:
                        refund_val = order_data["limit_price"] * Decimal(unfilled_qty)
                        self._paper_balance += refund_val
                        logger.info(f"[PAPER BROKER] Cancelled {order_id}. Refunded {unfilled_qty}x @ ${order_data['limit_price']:.2f}")
                return True
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
                return response.status in [200, 201]
        except Exception as e: 
            logger.error(f"Error cancelling order {order_id}", exc_info=True)
            return False

    async def execute_trade(self, action: str, contract_id: str, side: str, limit_price: Decimal, quantity: int, client_order_id: str = None) -> Optional[str]:
        if quantity <= 0:
            logger.error(f"Invalid quantity: {quantity}")
            return None
        if action.lower() not in self.VALID_ACTIONS or side.lower() not in self.VALID_SIDES: return None
            
        if self.paper_trade:
            order_id = f"paper-{uuid.uuid4().hex}"
            total_trade_value = limit_price * Decimal(quantity)
            if action.lower() == "buy":
                if self._paper_balance < total_trade_value:
                    logger.error(f"[PAPER BROKER] Insufficient paper balance for BUY order.")
                    return None
                self._paper_balance -= total_trade_value
                
            if len(self._paper_orders) >= 1000:
                oldest_key = next(iter(self._paper_orders))
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

# ==========================================
# QUANTITATIVE PRICING ENGINE
# ==========================================
class LiveTradingEngine:
    def __init__(self, broker: ExecutionBroker):
        self.broker = broker
        self.starting_balance: Decimal = Decimal("0.00")
        self.available_balance: Decimal = Decimal("0.00")
        self.capital_in_flight: Decimal = Decimal("0.00") 
        self.consecutive_api_failures: int = 0
        self.last_sync_time: float = 0.0
        self.last_telemetry_log_time: float = 0.0  
        self.state_sequence: int = 0
        
        self.active_trade_count: int = 0
        self._binance_events_received: int = 0 
        self.shutting_down: bool = False
        self.engine_start_time: float = time.time()

        self.circuit_breaker = MacroCircuitBreaker(
            lockout_before_sec=config.LOCKOUT_BEFORE_SEC,
            lockout_after_sec=config.LOCKOUT_AFTER_SEC
        )
        
        env_starting_bal = os.environ.get("STARTING_BALANCE")
        if env_starting_bal is not None:
            try:
                self.starting_balance = Decimal(env_starting_bal)
                logger.debug(f"[RISK MANAGER] Bound to absolute starting balance: ${self.starting_balance:.2f}")
            except (ValueError, InvalidOperation):
                logger.warning(f"Malformed STARTING_BALANCE env var: {env_starting_bal}. Falling back to dynamic initialization.")

        if os.environ.get("BOT_ENV", "simulation").lower() == "simulation":
            mock_event = EconomicEvent(
                event="Mock Federal Reserve FOMC Statement",
                timestamp=time.time() + 600.0,
                impact="HIGH"
            )
            self.circuit_breaker.active_events.append(mock_event)
            logger.debug(f"[CIRCUIT BREAKER] Offline simulation detected. Injected mock HIGH-impact event.")
        
        self.balance_lock = asyncio.Lock() 
        self.trade_cap_lock = asyncio.Lock()
        self.api_failure_lock = asyncio.Lock()
        
        self.assets: Dict[str, AssetState] = {
            "BTC-USD": AssetState(),
            "HYPE-USD": AssetState(),
            "SOL-USD": AssetState(),
            "ETH-USD": AssetState(),
            "DOGE-USD": AssetState(),
        }
        self._pending_tasks: Set[asyncio.Task] = set()
        self.performance_tracker = PerformanceTracker()
        self.active_tp_orders: Dict[str, asyncio.Queue] = {}
        self.orphan_fills: Dict[str, List[dict]] = {}

        if sys.platform != 'win32':
            fd, self.heartbeat_file = tempfile.mkstemp(prefix="kalshi_heartbeat_", suffix=".tick")
            os.close(fd)
                
            def cleanup_heartbeat(path):
                try:
                    if os.path.exists(path):
                        os.remove(path)
                except Exception as e:
                    logger.warning(f"Failed to remove heartbeat file: {e}")
            atexit.register(cleanup_heartbeat, self.heartbeat_file)

    async def _update_local_state(self, available_delta: Decimal, flight_delta: Decimal, state: AssetState = None, contract_id: str = None, qty_delta: int = 0):
        async with self.balance_lock:
            self.available_balance += available_delta
            self.capital_in_flight = max(Decimal("0.00"), self.capital_in_flight + flight_delta)
            if state and contract_id and qty_delta != 0:
                state.positions[contract_id] = max(0, state.positions.get(contract_id, 0) + qty_delta)
                if state.positions[contract_id] <= 0:
                    state.position_sides.pop(contract_id, None)
                    state.positions.pop(contract_id, None)
            self.state_sequence += 1

    def _get_filled_qty_from_details(self, details: dict, requested_qty: int) -> int:
        try:
            exec_fill = safe_decimal(details.get("executed_count"), "0.00")
            if exec_fill > 0:
                return int(exec_fill)
            
            maker_fill = safe_decimal(details.get("maker_fill_count"), "0.00")
            taker_fill = safe_decimal(details.get("taker_fill_count"), "0.00")
            total_fill = int(maker_fill + taker_fill)
            if total_fill > 0:
                return total_fill
        except (ValueError, InvalidOperation, TypeError):
            pass
        if details.get("status") == "executed": return requested_qty
        return 0

    async def _decrement_trade_cap(self):
        async with self.trade_cap_lock:
            self.active_trade_count = max(0, self.active_trade_count - 1)

    def _handle_task_done(self, task: asyncio.Task):
        """Single callback method to clear tracked task references."""
        self._pending_tasks.discard(task)
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.critical(f"Task crashed: {sanitize_log_str(str(e))}", exc_info=True)

    async def shutdown(self):
        self.shutting_down = True
        if self._pending_tasks:
            logger.debug(f"Draining {len(self._pending_tasks)} in-flight tasks...")
            done, pending = await asyncio.wait(self._pending_tasks, timeout=10.0)
            if pending:
                logger.warning(f"Cancelling {len(pending)} unresolved execution tasks before broker closure...")
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
            logger.debug("Task drain complete.")

    def purge_memory(self, queue: asyncio.Queue = None):
        current_time = time.time()
        for symbol, state in self.assets.items():
            state.cooldown_until = current_time + 15.0
            state.bids.clear()
            state.asks.clear()
        if queue:
            # Drain queue safely while keeping counter synchronized
            safe_drain_queue(queue)

    async def sync_balance_loop(self):
        try:
            self.capital_in_flight = await self.broker.get_locked_capital()
        except Exception as e: 
            logger.warning("Startup reconciliation failed to fetch locked capital", exc_info=True)

        backoff = 60
        while not self.shutting_down:
            # Safe gate: Defer synchronization during active entry execution to avoid race overwrite
            async with self.trade_cap_lock:
                active_trades = self.active_trade_count
            if active_trades > 0:
                await asyncio.sleep(5)
                continue

            async with self.balance_lock:
                start_seq = self.state_sequence

            try:
                locked_cap = await self.broker.get_locked_capital()
            except Exception as e:
                logger.warning("Failed to fetch locked capital in balance loop", exc_info=True)
                locked_cap = None

            positions_data = None
            try:
                positions_data = await self.broker.get_positions()
            except Exception as e:
                logger.warning("Failed to fetch broker positions in balance loop", exc_info=True)

            balance_data = await self.broker.get_balance()
            if balance_data is not None:
                available_bal, portfolio_val = balance_data
                state_mutated = False
                async with self.balance_lock:
                    if self.state_sequence != start_seq:
                        state_mutated = True
                    else:
                        self.available_balance = available_bal
                        self.last_sync_time = time.time()
                        if locked_cap is not None:
                            self.capital_in_flight = locked_cap
                        
                        if positions_data is not None:
                            for symbol, state in self.assets.items():
                                base_asset = symbol.split('-')[0]
                                valid_prefixes = (f"KX{base_asset}15M", f"KX{base_asset}-15M")
                                asset_positions = {}
                                asset_position_sides = {}
                                for cid, (qty, side) in positions_data.items():
                                    if any(cid.startswith(p) for p in valid_prefixes) and qty > 0:
                                        asset_positions[cid] = qty
                                        asset_position_sides[cid] = side
                                state.positions = asset_positions
                                state.position_sides = asset_position_sides
                        
                        if self.starting_balance == Decimal("0.00"):
                            self.starting_balance = portfolio_val
                        
                        if self.starting_balance > 0:
                            drawdown = (self.starting_balance - portfolio_val) / self.starting_balance
                            if drawdown >= config.DRAWDOWN_LIMIT_PCT:
                                logger.critical(f"DRAWDOWN LIMIT REACHED ({drawdown*100:.1f}%). Halting operations.")
                                self.shutting_down = True
                
                if state_mutated:
                    logger.warning("State mutated during fetch; discarding sync data to prevent race overwrite.")
                    await asyncio.sleep(1.0)  # Prevent zero-delay API spin
                    continue
                
                current_time = time.time()
                if current_time - self.last_telemetry_log_time >= config.TELEMETRY_LOG_INTERVAL_SEC:
                    self.last_telemetry_log_time = current_time
                    
                    asset_status_summaries = []
                    for symbol, state in self.assets.items():
                        if state.last_price is not None:
                            mean, upper, lower = state.fast_indicators.get_bollinger_bands()
                            std_dev = (upper - mean) / 2.0
                            
                            if std_dev >= config.STD_DEV_FLOOR:
                                z_score_val = (state.last_price - mean) / std_dev if std_dev > 0 else 0.0
                                z_score_repr = f"{z_score_val:+.2f}"
                            else:
                                z_score_repr = "N/A (Low Volatility)"
                                
                            asset_status_summaries.append(
                                f"{symbol} [Price: ${state.last_price:.2f} | Mean: ${mean:.2f} | Ticks: {state.tick_count}/{config.MIN_EMA_TICKS} | Z-Score: {z_score_repr}]"
                            )
                        else:
                            asset_status_summaries.append(f"{symbol} [Warmup: No Tick Matches Ingested]")
                            
                    telemetry_block = " | ".join(asset_status_summaries)
                    logger.info(
                        f"[RISK MANAGER] Wallet Synchronized | Available Cash: ${self.available_balance:.2f} | Net Asset Value: ${portfolio_val:.2f} | "
                        f"In-Flight: ${self.capital_in_flight:.2f} || Telemetry: {telemetry_block}"
                    )
                
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
                    logger.debug(f"[MARKET ROUTER] {symbol} Locked onto valid contract: {contract_id}")
                    state.active_contract_id = contract_id
                    state.strike_price = strike
                    state.expiration_time = exp_time 
            await asyncio.sleep(30)

    async def sync_macro_calendar_loop(self):
        while not self.shutting_down:
            if os.environ.get("BOT_ENV", "simulation").lower() == "simulation":
                await asyncio.sleep(3600)
                continue
                
            try:
                session = getattr(self.broker, "session", None)
                if not session or session.closed:
                    resolver = SafeResolver()
                    connector = aiohttp.TCPConnector(ssl=GLOBAL_SSL_CONTEXT, resolver=resolver)
                    async with aiohttp.ClientSession(connector=connector) as temp_session:
                        success = await self.circuit_breaker.fetch_calendar(temp_session)
                else:
                    success = await self.circuit_breaker.fetch_calendar(session)

                if success:
                    logger.debug(f"[CIRCUIT BREAKER] Economic calendar synchronized.")
                else:
                    logger.warning("[CIRCUIT BREAKER] Calendar synchronization returned no updates or failed validation.")
            except Exception as e:
                logger.error(f"[CIRCUIT BREAKER] Critical calendar update routine failure: {type(e).__name__}")
                
            await asyncio.sleep(21600)  # Refresh every 6 hours

    async def _monitor_take_profit(self, state: AssetState, contract_id: str, side: str, entry_price: Decimal, quantity: int, seconds_left: float):
        """Asynchronous O(1) Background Task: Laddered Take-Profit with adaptive pricing."""
        if quantity <= 0: return
        try:
            min_tp = config.MIN_TP_ROI
            max_tp = config.MAX_TP_ROI  # 1.85 = 85% ROI
            clamped_seconds = max(180.0, min(600.0, float(seconds_left)))
            dynamic_multiplier = min_tp + Decimal(str((clamped_seconds - 180.0) / 420.0)) * (max_tp - min_tp)
            
            # Adaptive TP: Check current orderbook depth to anchor TP to real liquidity
            tp_check = await self.broker.get_best_bid_ask(contract_id, side)
            if tp_check:
                current_best_bid, _, _, _ = tp_check
                max_realistic_tp = max(
                    entry_price + Decimal("0.01"), 
                    entry_price * Decimal("1.02"), 
                    current_best_bid * Decimal("1.30")
                )
            else:
                max_realistic_tp = Decimal("0.99")
            
            # === LADDERED EXIT STRATEGY ===
            # Tranche 1 (40%): Conservative — quick-fill exit
            t1_qty = max(1, round(quantity * 0.40))
            raw_t1 = (entry_price * Decimal("1.50")).quantize(Decimal("0.01"), rounding=ROUND_UP)  # 50% ROI
            t1_price = min(Decimal("0.99"), max_realistic_tp, raw_t1)
            
            # Tranche 2 (35%): Dynamic multiplier target
            t2_qty = max(0, round(quantity * 0.35))
            if t1_qty + t2_qty > quantity:
                t2_qty = quantity - t1_qty
            raw_t2 = (entry_price * dynamic_multiplier).quantize(Decimal("0.01"), rounding=ROUND_UP)
            t2_price = min(Decimal("0.99"), max(t1_price + Decimal("0.01"), raw_t2))
            
            # Tranche 3 (25%): Aggressive moonshot
            t3_qty = max(0, quantity - t1_qty - t2_qty)
            raw_t3 = (entry_price * Decimal("1.95")).quantize(Decimal("0.01"), rounding=ROUND_UP)  # 95% ROI
            t3_price = min(Decimal("0.99"), max(t2_price + Decimal("0.01"), raw_t3))
            
            safe_contract_id = urllib.parse.quote(contract_id, safe='')
            
            # Minor execution delay to allow Kalshi portfolio settlement
            await asyncio.sleep(0.5)
            
            # Place all tranches
            order_ids = []
            tranches = []
            if t1_qty > 0:
                tranches.append((t1_qty, t1_price, "T1-conservative"))
            if t2_qty > 0:
                tranches.append((t2_qty, t2_price, "T2-dynamic"))
            if t3_qty > 0:
                tranches.append((t3_qty, t3_price, "T3-aggressive"))
            
            for tq, tp, label in tranches:
                tp_client_oid = f"tp-{label}-{uuid.uuid4().hex[:12]}"
                logger.info(f"[{contract_id}] Routing {label} Take-Profit: Sell {tq} '{side.upper()}' @ ${tp:.2f}")
                tp_order_id = await self.broker.execute_trade(
                    action="sell",
                    contract_id=contract_id,
                    side=side,
                    limit_price=tp,
                    quantity=tq,
                    client_order_id=tp_client_oid
                )
                if tp_order_id:
                    self.active_tp_orders[tp_order_id] = asyncio.Queue()  # Register immediately to prevent TOCTOU fill loss
                    if tp_order_id in self.orphan_fills:
                        for fill_msg in self.orphan_fills.pop(tp_order_id):
                            self.active_tp_orders[tp_order_id].put_nowait(fill_msg)
                    order_ids.append((tp_order_id, tq, tp, label))
                else:
                    logger.warning(f"[{contract_id}] Failed to route {label} TP order.")
            
            if not order_ids:
                logger.warning(f"[{contract_id}] All TP tranches failed. Holding to expiration.")
                return
            
            # (Orders already registered in active_tp_orders dispatch mapping above)
            
            # Monitor all tranches until buzzer
            poll_interval = 5.0
            elapsed = 0.0
            timeout = max(0.0, seconds_left - 10.0)
            total_filled = 0
            total_proceeds = Decimal("0.00")
            
            # Track previously reported fills per order to update local state incrementally
            last_reported_fill = {oid: 0 for oid, _, _, _ in order_ids}
            ws_accumulated_fills = {oid: 0 for oid, _, _, _ in order_ids}
            rest_accumulated_fills = {oid: 0 for oid, _, _, _ in order_ids}
            completed_orders = set()
            start_time = time.time()
            
            try:
                while time.time() - start_time < timeout and len(completed_orders) < len(order_ids):
                    active_oids = [oid for oid, _, _, _ in order_ids if oid not in completed_orders]
                    if not active_oids:
                        break
                    
                    # Gather pending queue read tasks
                    pending_tasks = {
                        asyncio.create_task(self.active_tp_orders[oid].get()): oid
                        for oid in active_oids
                    }
                    
                    time_remaining = timeout - (time.time() - start_time)
                    if time_remaining <= 0:
                        for task in pending_tasks.keys():
                            task.cancel()
                        break
                    
                    # Wait for first event or max 10 seconds (REST fallback threshold)
                    wait_timeout = min(10.0, time_remaining)
                    try:
                        done, pending = await asyncio.wait(
                            pending_tasks.keys(),
                            timeout=wait_timeout,
                            return_when=asyncio.FIRST_COMPLETED
                        )
                    finally:
                        # Safely cancel all tasks that did not complete, avoiding NameError risks on unbound 'pending'
                        for task in pending_tasks.keys():
                            if not task.done():
                                task.cancel()
                    
                    if done:
                        # Process WebSocket events
                        for task in done:
                            oid = pending_tasks[task]
                            try:
                                fill_data = task.result()
                                
                                # Resolve specific tranche details from order_ids
                                oqty, oprice, olabel = next(
                                    (q, p, l) for i, q, p, l in order_ids if i == oid
                                )
                                
                                raw_fills = int(fill_data.get("count", 0))
                                ws_accumulated_fills[oid] = ws_accumulated_fills.get(oid, 0) + raw_fills
                                max_known = max(ws_accumulated_fills[oid], rest_accumulated_fills.get(oid, 0))
                                new_fills = min(max_known - last_reported_fill.get(oid, 0), oqty - last_reported_fill.get(oid, 0))
                                tp_status = fill_data.get("status", "unknown")
                                
                                # Incrementally update positions (WS count is incremental)
                                if new_fills > 0:
                                    total_filled += new_fills
                                    total_proceeds += Decimal(new_fills) * oprice
                                    await self._update_local_state(Decimal("0.00"), Decimal("0.00"), state, contract_id, -new_fills)
                                    last_reported_fill[oid] = last_reported_fill.get(oid, 0) + new_fills
                                
                                if last_reported_fill[oid] >= oqty or tp_status == "executed":
                                    logger.warning(f"[{contract_id}] 🎯 {olabel} TP HIT (WS)! Sold {last_reported_fill[oid]}x @ ${oprice:.2f}")
                                    completed_orders.add(oid)
                                elif tp_status in ["canceled", "cancelled"]:
                                    completed_orders.add(oid)
                            except Exception as ex:
                                logger.debug(f"[{contract_id}] Error reading WS fill event: {ex}")
                    else:
                        # Fallback REST poll: We didn't receive any WS messages for 10.0 seconds
                        logger.debug(f"[{contract_id}] WS quiet for {wait_timeout:.1f}s. Running fallback REST poll.")
                        for oid, oqty, oprice, olabel in order_ids:
                            if oid in completed_orders:
                                continue
                            try:
                                tp_details = await self.broker.get_order_details(oid, simulate=False)
                                tp_status = tp_details.get("status", "unknown")
                                tp_filled_qty = self._get_filled_qty_from_details(tp_details, oqty)
                                
                                rest_accumulated_fills[oid] = tp_filled_qty
                                max_known = max(ws_accumulated_fills.get(oid, 0), rest_accumulated_fills[oid])
                                new_fills = min(max_known - last_reported_fill.get(oid, 0), oqty - last_reported_fill.get(oid, 0))
                                
                                if new_fills > 0:
                                    total_filled += new_fills
                                    total_proceeds += Decimal(new_fills) * oprice
                                    await self._update_local_state(Decimal("0.00"), Decimal("0.00"), state, contract_id, -new_fills)
                                    last_reported_fill[oid] = tp_filled_qty
                                
                                if tp_filled_qty >= oqty or tp_status == "executed":
                                    logger.warning(f"[{contract_id}] 🎯 {olabel} TP HIT (Fallback REST)! Sold {tp_filled_qty}x @ ${oprice:.2f}")
                                    completed_orders.add(oid)
                                elif tp_status in ["canceled", "cancelled"]:
                                    completed_orders.add(oid)
                            except Exception as e:
                                logger.debug(f"[{contract_id}] Fallback REST poll error: {e}")
            finally:
                # Cleanup registry
                for oid, _, _, _ in order_ids:
                    self.active_tp_orders.pop(oid, None)
            
            # Buzzer: Cancel all remaining orders
            logger.info(f"[{contract_id}] Take-Profit lifecycle ending. Cancelling remaining orders...")
            import random
            for oid, oqty, oprice, olabel in order_ids:
                if oid in completed_orders:
                    continue
                try:
                    # Retry cancellation up to 3 times
                    for c_attempt in range(3):
                        try:
                            cancel_success = await self.broker.cancel_order(oid)
                            if cancel_success:
                                break
                        except Exception as ce:
                            if c_attempt == 2:
                                raise ce
                        await asyncio.sleep(0.5 * (1.5 ** c_attempt))
                except Exception as ce:
                    logger.warning(f"[{contract_id}] Error cancelling {olabel} TP after retries: {ce}")
                
                # Fetch final details with jittered backoff regardless of cancellation success
                try:
                    for attempt in range(5):
                        try:
                            details = await self.broker.get_order_details(oid, simulate=False)
                            status = details.get("status", "unknown")
                            if status in ["canceled", "cancelled", "executed"]:
                                filled = self._get_filled_qty_from_details(details, oqty)
                                new_fills = filled - last_reported_fill.get(oid, 0)
                                if new_fills > 0:
                                    total_filled += new_fills
                                    total_proceeds += Decimal(new_fills) * oprice
                                    await self._update_local_state(Decimal("0.00"), Decimal("0.00"), state, contract_id, -new_fills)
                                    last_reported_fill[oid] = filled
                                    logger.info(f"[{contract_id}] {olabel} partial fill at buzzer: {filled}/{oqty}")
                                break
                        except Exception as ex:
                            if attempt == 4:
                                logger.warning(f"[{contract_id}] Failed to retrieve final {olabel} TP details: {ex}")
                        backoff_delay = (1.5 ** attempt) + random.uniform(0.1, 0.5)
                        await asyncio.sleep(backoff_delay)
                except Exception as ve:
                    logger.warning(f"[{contract_id}] Error verifying final details for {olabel} TP: {ve}")
            
            # Record performance using correct asset parsing key and true net P&L
            current_hour = int(datetime.datetime.now(datetime.timezone.utc).hour)
            total_cost = Decimal(quantity) * entry_price
            net_pnl = total_proceeds - total_cost
            won = net_pnl > 0
            
            self.performance_tracker.record(contract_id, current_hour, won, float(net_pnl))

            if total_filled > 0:
                logger.warning(f"[{contract_id}] 🎯 TOTAL TP FILLED: {total_filled}/{quantity} across all tranches. Net P&L: ${net_pnl:.2f}")
            else:
                logger.info(f"[{contract_id}] No TP fills across any tranche. Held to expiration. Net P&L: ${net_pnl:.2f}")
                
        except Exception as e:
            logger.error(f"[{contract_id}] Unhandled error in Take Profit monitor.", exc_info=True)

    async def paper_fill_dispatcher(self):
        """Background loop for paper trading: simulates incremental order fills and pushes to active_tp_orders."""
        last_sent_fill = {}  # Track cumulative fills sent so far: order_id -> int
        while not self.shutting_down:
            try:
                active_ids = list(self.active_tp_orders.keys())
                # Clean up stale keys to prevent memory leak
                for oid in list(last_sent_fill.keys()):
                    if oid not in self.active_tp_orders:
                        last_sent_fill.pop(oid, None)
                
                # Pre-fetch orderbook for unique contracts to eliminate N+1 polling bottleneck
                contract_books = {}
                for oid in active_ids:
                    if oid.startswith("paper-"):
                        order_data = self.broker._paper_orders.get(oid)
                        if order_data and order_data["status"] not in ("executed", "canceled"):
                            cid = order_data["contract_id"]
                            side = order_data["side"]
                            cache_key = (cid, side)
                            if cache_key not in contract_books:
                                best_vals = await self.broker.get_best_bid_ask(cid, side)
                                contract_books[cache_key] = list(best_vals) if best_vals else None
                
                for oid in active_ids:
                    if oid.startswith("paper-"):
                        order_data = self.broker._paper_orders.get(oid)
                        if order_data:
                            cid = order_data["contract_id"]
                            side = order_data["side"]
                            details = await self.broker.get_order_details(oid, simulate=True, cached_best_vals=contract_books.get((cid, side)))
                        else:
                            details = await self.broker.get_order_details(oid)
                        if details:
                            queue = self.active_tp_orders.get(oid)
                            if queue:
                                executed = safe_int(details.get("executed_count", 0))
                                diff = executed - last_sent_fill.get(oid, 0)
                                if diff > 0:
                                    msg = {
                                        "order_id": oid,
                                        "count": diff,
                                        "status": details.get("status", "resting")
                                    }
                                    await queue.put(msg)
                                    last_sent_fill[oid] = executed
            except Exception as e:
                logger.debug(f"Paper fill dispatcher error: {e}")
            await asyncio.sleep(1.0)

    async def execute_and_hold_entry(self, state: AssetState, contract_id: str, side: str, limit_price: Decimal, quantity: int, total_cost: Decimal, seconds_left: float):
        order_id = None
        locked_capital = total_cost

        try:
            safe_contract_id = urllib.parse.quote(contract_id, safe='')
            client_entry_oid = f"entry-{uuid.uuid4().hex[:16]}"
            
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
                    
                logger.info(f"[{contract_id}] BUY Order active. Verifying fill status...")

                filled_qty = 0
                poll_interval = 0.2
                elapsed = 0.0
                timeout = 0.8 

                while elapsed < timeout:
                    try:
                        details = await self.broker.get_order_details(order_id)
                        status = details.get("status", "unknown")
                        filled_qty = self._get_filled_qty_from_details(details, quantity)

                        if filled_qty > 0 or status in ["executed", "canceled"]:
                            break
                    except Exception as poll_err:
                        logger.warning(f"[{contract_id}] Transient error polling order details: {poll_err}. Retrying status fetch...")

                    await asyncio.sleep(poll_interval)
                    elapsed += poll_interval

                if filled_qty > 0:
                    if filled_qty < quantity:
                        cancel_success = await self.broker.cancel_order(order_id)
                        # Verify final order details with jittered exponential backoff for rate-limiting resilience
                        import random
                        details = None
                        status = "unknown"
                        for attempt in range(5):
                            try:
                                details = await self.broker.get_order_details(order_id)
                                status = details.get("status", "unknown")
                                if status in ["canceled", "cancelled", "executed"]:
                                    break
                            except Exception as err:
                                if attempt == 4:
                                    raise RuntimeError(f"Failed to verify order details after partial cancel: {err}")
                            backoff_delay = (1.5 ** attempt) + random.uniform(0.1, 0.5)
                            await asyncio.sleep(backoff_delay)
                                
                        filled_qty = self._get_filled_qty_from_details(details, quantity)
                        
                        if filled_qty == quantity or status == "executed":
                            await self._update_local_state(Decimal("0.00"), -locked_capital)
                            logger.info(f"[{contract_id}] Order filled ({quantity}/{quantity}).")
                            
                            tp_task = asyncio.create_task(self._monitor_take_profit(state, contract_id, side, _extract_fill_price(details, limit_price), quantity, seconds_left))
                            self._pending_tasks.add(tp_task)
                            tp_task.add_done_callback(self._handle_task_done)
                            return
                        elif status in ["canceled", "cancelled"]:
                            unfilled_qty = quantity - filled_qty
                            if unfilled_qty <= 0:
                                await self._update_local_state(Decimal("0.00"), -locked_capital)
                                logger.info(f"[{contract_id}] Order filled ({quantity}/{quantity}).")
                                
                                tp_task = asyncio.create_task(self._monitor_take_profit(state, contract_id, side, _extract_fill_price(details, limit_price), quantity, seconds_left))
                                self._pending_tasks.add(tp_task)
                                tp_task.add_done_callback(self._handle_task_done)
                                return
                            else:
                                refund = Decimal(str(unfilled_qty)) * limit_price
                                await self._update_local_state(refund, -refund, state, contract_id, -unfilled_qty)
                                locked_capital -= refund
                                logger.info(f"[{contract_id}] Partial Fill ({filled_qty}/{quantity}).")
                                
                                tp_task = asyncio.create_task(self._monitor_take_profit(state, contract_id, side, _extract_fill_price(details, limit_price), filled_qty, seconds_left))
                                self._pending_tasks.add(tp_task)
                                tp_task.add_done_callback(self._handle_task_done)
                                return
                        else:
                            logger.critical(f"[{contract_id}] Partial cancel failed. Status: {status}. Locking exposed capital.")
                            return
                    else:
                        logger.info(f"[{contract_id}] Order filled ({filled_qty}/{quantity}).")

                        await self._update_local_state(Decimal("0.00"), -locked_capital)
                        
                        tp_task = asyncio.create_task(self._monitor_take_profit(state, contract_id, side, _extract_fill_price(details, limit_price), filled_qty, seconds_left))
                        self._pending_tasks.add(tp_task)
                        tp_task.add_done_callback(self._handle_task_done)
                        
                        logger.warning(f"[{contract_id}] Position Secured. Execution task safely terminating.")
                        return
                else:
                    logger.warning(f"[{contract_id}] Limit buy missed fill window. Canceling.")
                    cancel_success = await self.broker.cancel_order(order_id)
                    
                    # Verify final order details with jittered exponential backoff for rate-limiting resilience
                    import random
                    details = None
                    status = "unknown"
                    for attempt in range(5):
                        try:
                            details = await self.broker.get_order_details(order_id)
                            status = details.get("status", "unknown")
                            if status in ["canceled", "cancelled", "executed"]:
                                break
                        except Exception as err:
                            if attempt == 4:
                                raise RuntimeError(f"Failed to verify order details after cancellation: {err}")
                        backoff_delay = (1.5 ** attempt) + random.uniform(0.1, 0.5)
                        await asyncio.sleep(backoff_delay)
                            
                    filled_qty = self._get_filled_qty_from_details(details, quantity)

                    if filled_qty == quantity or status == "executed":
                        await self._update_local_state(Decimal("0.00"), -locked_capital)
                        logger.warning(f"[{contract_id}] Order fully filled prior to cancellation.")
                        
                        tp_task = asyncio.create_task(self._monitor_take_profit(state, contract_id, side, _extract_fill_price(details, limit_price), quantity, seconds_left))
                        self._pending_tasks.add(tp_task)
                        tp_task.add_done_callback(self._handle_task_done)
                        return
                    elif status in ["canceled", "cancelled"]:
                        if filled_qty > 0:
                            unfilled_qty = quantity - filled_qty
                            refund = Decimal(str(unfilled_qty)) * limit_price
                            await self._update_local_state(refund, -refund, state, contract_id, -unfilled_qty)
                            logger.warning(f"[{contract_id}] Partially filled ({filled_qty}/{quantity}) prior to cancellation.")
                            
                            tp_task = asyncio.create_task(self._monitor_take_profit(state, contract_id, side, _extract_fill_price(details, limit_price), filled_qty, seconds_left))
                            self._pending_tasks.add(tp_task)
                            tp_task.add_done_callback(self._handle_task_done)
                            return
                        else:
                            await self._update_local_state(locked_capital, -locked_capital, state, contract_id, -quantity)
                            logger.info(f"[{contract_id}] Order fully cancelled.")
                            return
                    else:
                        logger.critical(f"[{contract_id}] Cancel failed or unconfirmed. Status: {status}. Leaving capital in-flight.")
                        return
            else:
                async with self.api_failure_lock:
                    self.consecutive_api_failures += 1
                    if self.consecutive_api_failures >= 5:
                        current_time = time.time()
                        for s in self.assets.values(): s.cooldown_until = current_time + 300.0
                        self.consecutive_api_failures = 0

                logger.critical(f"[{contract_id}] API Execution dropped. Releasing slot natively.")
                await self._update_local_state(locked_capital, -locked_capital, state, contract_id, -quantity)

        except Exception as e:
            logger.critical(f"[{contract_id}] Unhandled exception in entry manager. Forcing release.", exc_info=True)
            if locked_capital > 0:
                if not order_id:
                    await self._update_local_state(locked_capital, -locked_capital, state, contract_id, -quantity)
                else:
                    # Order was placed, but we crashed mid-lifecycle.
                    # Decrement capital_in_flight locally to avoid permanent leakage, and let sync loop reconcile.
                    await self._update_local_state(Decimal("0.00"), -locked_capital, state, contract_id, -quantity)
            raise
        finally:
            await asyncio.shield(self._decrement_trade_cap())

    # ==========================================
    # CORE QUANTITATIVE ENGINE: Mean-Reversion
    # ==========================================
    async def process_live_tick(self, raw_bytes: bytes):
        if self.shutting_down: return
        if self.last_sync_time == 0.0: return 
        if time.time() - self.last_sync_time > config.STALE_BALANCE_TIMEOUT_SEC: return

        try:
            parsed_dict = orjson.loads(raw_bytes)
        except orjson.JSONDecodeError: return

        tick_dict = validate_tick_data(parsed_dict)
        if not tick_dict: 
            return

        product_id = tick_dict["product_id"]
        if product_id not in self.assets: return
        state = self.assets[product_id]

        tick_price = float(tick_dict["price"])

        mean, upper, lower = state.fast_indicators.get_bollinger_bands()

        # Anomaly Filter with Safe Baseline Reset (SEC-01)
        if mean > 0.0:
            percentage_deviation = abs(tick_price - mean) / mean
            if percentage_deviation > config.MAX_PRICE_DEVIATION_PCT:
                state.consecutive_outliers += 1
                if state.consecutive_outliers >= config.CONSECUTIVE_OUTLIER_LIMIT:
                    logger.warning(f"[OUTLIER SHIELD] Detected {state.consecutive_outliers} consecutive outlier ticks. Forcing baseline reset.")
                    state.fast_indicators = kalshi_bot.FastIndicators(14, float(config.EMA_ALPHA))
                    state.fast_indicators.add_price(tick_price)
                    state.tick_count = 1                  
                    state.consecutive_outliers = 0
                    state.last_price = tick_price
                    return  
                else:
                    logger.warning(f"[OUTLIER SHIELD] Ignored anomalous price change on {product_id}: ${tick_price:.2f} vs Mean: ${mean:.2f}")
                    return
            else:
                state.consecutive_outliers = 0

        current_time = time.time()
        
        # O(1) Variance & Mean Update via Rust PyO3 Extension
        state.tick_count += 1
        tick_volume = tick_dict.get("volume", 0.0)
        if tick_volume and tick_volume > 0:
            usd_notional_k = (float(tick_price) * float(tick_volume)) / 1000.0
            state.fast_indicators.add_price_with_volume(tick_price, usd_notional_k)
        else:
            state.fast_indicators.add_price(tick_price)

        if current_time < state.cooldown_until:
            state.last_price = float(tick_price)
            return

        if self.circuit_breaker.is_locked_out():
            state.last_price = tick_price
            return

        last_price = state.last_price
        state.last_price = tick_price
        if not last_price: return

        # (Dead code for indicator caching and pre-execution guards removed per Security Audit ADV-1)
        
        # -------------------------------------------------------------
        # Z-SCORE MOMENTUM BREAKOUT SNIPER
        # -------------------------------------------------------------
        if state.tick_count < config.MIN_EMA_TICKS: return
        
        z_score = state.fast_indicators.get_z_score()
        if abs(z_score) < 2.5: return

        if not state.active_contract_id: return
        
        seconds_left = state.expiration_time - current_time if state.expiration_time else 900.0
        # The Golden Window: 8 minutes to 3 minutes remaining
        if seconds_left < 180.0 or seconds_left > 480.0: return
        
        trade_side = "YES" if z_score > 0 else "NO"
        executing_contract_id = state.active_contract_id
        
        # One trade per asset per event guard
        if executing_contract_id == getattr(state, "last_traded_event", ""): return
        
        slot_acquired = False
        local_mutated = False
        try:
            async with self.trade_cap_lock:
                if self.active_trade_count >= config.MAX_CONCURRENT_TRADES: return
                self.active_trade_count += 1
                slot_acquired = True
                
            state.cooldown_until = current_time + 15.0
            best_vals = await self.broker.get_best_bid_ask(executing_contract_id, trade_side)
            if not best_vals:
                return
                
            # TOCTOU Security Fixes
            current_time = time.time()
            seconds_left = state.expiration_time - current_time if state.expiration_time else 900.0
            if seconds_left < 180.0 or seconds_left > 480.0:
                return
            if executing_contract_id != state.active_contract_id:
                return
                
            best_bid, best_ask, bid_depth, ask_depth = best_vals
            spread = best_ask - best_bid
            
            max_spread = min(config.MAX_ALLOWED_SPREAD, max(Decimal("0.05"), best_bid * Decimal("0.30")))
            if best_ask >= Decimal("0.85") or best_bid < Decimal("0.01") or best_ask < Decimal("0.15") or spread > max_spread:
                return
                
            limit_price = max(Decimal("0.01"), min(Decimal("0.99"), best_ask))
            
            should_decrement = False
            async with self.balance_lock:
                if executing_contract_id == getattr(state, "last_traded_event", ""):
                    should_decrement = True
                else:
                    current_pos_side = state.position_sides.get(executing_contract_id)
                    if current_pos_side and current_pos_side != trade_side:
                        should_decrement = True
                    else:
                        actual_pos_size = state.positions.get(executing_contract_id, 0)
                        remaining_exposure = config.MAX_EXPOSURE_PER_EVENT - actual_pos_size
                        if remaining_exposure <= 0:
                            should_decrement = True
                        else:
                            trade_budget = self.available_balance * config.TRADE_BUDGET_PCT
                            raw_quantity = int(trade_budget / limit_price)
                            quantity = min(raw_quantity, config.MAX_CONTRACTS_PER_TRADE, remaining_exposure)
                            
                            if quantity < 30:
                                should_decrement = True
                            else:
                                total_cost = Decimal(quantity) * limit_price
                                self.available_balance -= total_cost
                                self.capital_in_flight += total_cost
                                
                                state.position_sides[executing_contract_id] = trade_side
                                state.positions[executing_contract_id] = actual_pos_size + quantity
                                state.last_traded_event = executing_contract_id
                                state.cooldown_until = current_time + 15.0
                                self.state_sequence += 1
                                local_mutated = True
            
            if should_decrement:
                return
            
            logger.warning(f"[{product_id}] Z-SCORE BREAKOUT ({z_score:.2f})! Ask: ${best_ask:.2f} | Sniping {quantity} contracts.")
            
            exec_task = asyncio.create_task(
                self.execute_and_hold_entry(
                    state, executing_contract_id, trade_side, limit_price, quantity, total_cost, seconds_left
                )
            )
            # Safe handoff: explicitly secure strong reference first
            self._pending_tasks.add(exec_task)
            exec_task.add_done_callback(self._handle_task_done)
            slot_acquired = False
        except Exception as e:
            logger.error("Z-Score Momentum processing fault", exc_info=True) 
            if 'exec_task' in locals() and not exec_task.done():
                exec_task.cancel()
            if local_mutated and slot_acquired:
                # Revert leaked local balance if task creation failed
                await asyncio.shield(self._update_local_state(total_cost, -total_cost, state, executing_contract_id, -quantity))
        finally:
            if slot_acquired:
                await asyncio.shield(self._decrement_trade_cap())

    # ==========================================
    # BINANCE LIQUIDATION SNIPER
    # ==========================================
    async def process_binance_liquidation(self, raw_bytes: bytes):
        if self.shutting_down: return
        
        self._binance_events_received += 1
        if self._binance_events_received % 2000 == 0:
            events = self._binance_events_received
            if events >= 1_000_000:
                fmt_events = f"{events / 1_000_000:.1f}m".replace(".0m", "m")
            elif events >= 1_000:
                fmt_events = f"{events / 1_000:.1f}k".replace(".0k", "k")
            else:
                fmt_events = str(events)
            logger.info(f"[HEARTBEAT] Binance Liquidation Sniper active and scanning... ({fmt_events} liquidations processed).")
            
        try:
            parsed_dict = orjson.loads(raw_bytes)
            # Support combined stream format where actual payload is wrapped in a "data" object
            event_data = parsed_dict.get("data", parsed_dict)
            
            payload_dict = validate_binance_payload(event_data)
            if not payload_dict: 
                return
                
            symbol = payload_dict["o"]["s"]
            if symbol.startswith("BTCUSDT") or symbol == "BTCUSD_PERP": asset_symbol = "BTC-USD"
            elif symbol.startswith("HYPEUSDT") or symbol == "HYPEUSD_PERP": asset_symbol = "HYPE-USD"
            elif symbol.startswith("SOLUSDT") or symbol == "SOLUSD_PERP": asset_symbol = "SOL-USD"
            elif symbol.startswith("ETHUSDT") or symbol == "ETHUSD_PERP": asset_symbol = "ETH-USD"
            elif symbol.startswith("DOGEUSDT") or symbol == "DOGEUSD_PERP": asset_symbol = "DOGE-USD"
            else: return
            
            state = self.assets.get(asset_symbol)
            if not state or not state.active_contract_id: return
            
            notional = payload_dict["o"]["p"] * payload_dict["o"]["q"]
            threshold = config.BINANCE_LIQUIDATION_THRESHOLDS.get(asset_symbol)
            if not threshold or notional < threshold: return 

            if payload_dict["o"]["S"] == "SELL": 
                trade_side = "NO"
            elif payload_dict["o"]["S"] == "BUY": 
                trade_side = "YES"
            else: 
                return

            if self.circuit_breaker.is_locked_out():
                return

            if self.last_sync_time == 0.0 or time.time() - self.last_sync_time > config.STALE_BALANCE_TIMEOUT_SEC: 
                logger.warning(f"[{asset_symbol}] Dropping liquidation event — balance data is stale.")
                return
            
            current_time = time.time()
            if current_time < state.cooldown_until: return
            
            seconds_left = state.expiration_time - current_time if state.expiration_time else 900.0
            if seconds_left < 180.0: return
            
            executing_contract_id = state.active_contract_id
            
            # One trade per asset per event guard
            if executing_contract_id == getattr(state, "last_traded_event", ""): return
            
            slot_acquired = False
            local_mutated = False
            try:
                async with self.trade_cap_lock:
                    if self.active_trade_count >= config.MAX_CONCURRENT_TRADES: return
                    self.active_trade_count += 1
                    slot_acquired = True
                    
                state.cooldown_until = current_time + 15.0  # Lock cooldown immediately before yielding event loop
                best_vals = await self.broker.get_best_bid_ask(executing_contract_id, trade_side)
                if not best_vals:
                    return
                    
                # TOCTOU Security Fixes
                current_time = time.time()
                seconds_left = state.expiration_time - current_time if state.expiration_time else 900.0
                if seconds_left < 180.0:
                    return
                if executing_contract_id != state.active_contract_id:
                    return
                    
                best_bid, best_ask, bid_depth, ask_depth = best_vals
                spread = best_ask - best_bid
                
                # Dynamic spread limit based on bid price
                max_spread = min(config.MAX_ALLOWED_SPREAD, max(Decimal("0.05"), best_bid * Decimal("0.30")))
                if best_ask >= Decimal("0.85") or best_bid < Decimal("0.01") or best_ask < Decimal("0.15") or spread > max_spread:
                    return
                    
                limit_price = max(Decimal("0.01"), min(Decimal("0.99"), best_ask))
                
                should_decrement = False
                async with self.balance_lock:
                    if executing_contract_id == getattr(state, "last_traded_event", ""):
                        should_decrement = True
                    else:
                        current_pos_side = state.position_sides.get(executing_contract_id)
                        if current_pos_side and current_pos_side != trade_side:
                            should_decrement = True
                        else:
                            actual_pos_size = state.positions.get(executing_contract_id, 0)
                            remaining_exposure = config.MAX_EXPOSURE_PER_EVENT - actual_pos_size
                            if remaining_exposure <= 0:
                                should_decrement = True
                            else:
                                trade_budget = self.available_balance * config.TRADE_BUDGET_PCT
                                raw_quantity = int(trade_budget / limit_price)
                                quantity = min(raw_quantity, config.MAX_CONTRACTS_PER_TRADE, remaining_exposure)
                                
                                if quantity < 30:
                                    should_decrement = True
                                else:
                                    total_cost = Decimal(quantity) * limit_price
                                    self.available_balance -= total_cost
                                    self.capital_in_flight += total_cost
                                    
                                    state.position_sides[executing_contract_id] = trade_side
                                    state.positions[executing_contract_id] = actual_pos_size + quantity
                                    state.last_traded_event = executing_contract_id
                                    state.cooldown_until = current_time + 15.0
                                    self.state_sequence += 1
                                    local_mutated = True
                
                if should_decrement:
                    return
                
                logger.warning(f"[{asset_symbol}] BINANCE LIQUIDATION SIGNAL (${notional:,.2f})! Ask: ${best_ask:.2f} | Sniping {quantity} contracts.")
                
                exec_task = asyncio.create_task(
                    self.execute_and_hold_entry(
                        state, executing_contract_id, trade_side, limit_price, quantity, total_cost, seconds_left
                    )
                )
                self._pending_tasks.add(exec_task)
                exec_task.add_done_callback(self._handle_task_done)
                slot_acquired = False
            except Exception as inner_e:
                logger.error("Liquidation execution fault", exc_info=True)
                if 'exec_task' in locals() and not exec_task.done():
                    exec_task.cancel()
                if local_mutated and slot_acquired:
                    await asyncio.shield(self._update_local_state(total_cost, -total_cost, state, executing_contract_id, -quantity))
                raise
            finally:
                if slot_acquired:
                    await asyncio.shield(self._decrement_trade_cap())
                
        except Exception as e:
            logger.error("Liquidation processing fault", exc_info=True) 

# ==========================================
# ASYNC QUEUES
# ==========================================
async def coinbase_websocket_consumer(engine: LiveTradingEngine, queue: asyncio.Queue):
    uri = "wss://ws-feed.exchange.coinbase.com" 
    subscribe_message = {
        "type": "subscribe",
        "product_ids": ["BTC-USD", "HYPE-USD", "SOL-USD", "ETH-USD", "DOGE-USD"],
        "channels": ["ticker"]
    }

    attempt = 0
    max_attempts = 30
    while not engine.shutting_down:
        conn_start = time.time()
        reset_done = False
        try:
            async with websockets.connect(uri, ssl=GLOBAL_SSL_CONTEXT, max_size=1048576, max_queue=256) as ws:
                logger.debug("Connected to Coinbase Live Spot Feed.")
                await ws.send(orjson.dumps(subscribe_message).decode('utf-8'))
                
                async for message in ws:
                    if engine.shutting_down: break
                    if not reset_done and time.time() - conn_start > 10.0:
                        attempt = 0  
                        reset_done = True
                    try: 
                        queue.put_nowait(message)
                    except asyncio.QueueFull: 
                        engine.purge_memory(queue) 
                        try:
                            queue.put_nowait(message)
                        except asyncio.QueueFull:
                            pass
                    
        except Exception as e:
            engine.purge_memory(queue) 
            if not reset_done and time.time() - conn_start > 10.0:
                attempt = 0
            attempt += 1
            if attempt > max_attempts:
                logger.critical("[FATAL] Coinbase connection limit reached. Stopping consumer.")
                engine.shutting_down = True
                break
            delay = calculate_backoff_delay(attempt)
            logger.warning(f"Coinbase WS error ({type(e).__name__}). Retrying in {delay:.2f}s...")
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
    uri = "wss://fstream.binance.com/market/stream?streams=!forceOrder@arr"
    
    attempt = 0
    max_attempts = 30
    while not engine.shutting_down:
        conn_start = time.time()
        reset_done = False
        try:
            async with websockets.connect(uri, ssl=GLOBAL_SSL_CONTEXT, max_size=1048576, max_queue=256) as ws:
                logger.debug("Connected to Binance Futures Feed.")
                async for message in ws:
                    if engine.shutting_down: break
                    if not reset_done and time.time() - conn_start > 10.0:
                        attempt = 0  
                        reset_done = True
                    try: 
                        queue.put_nowait(message)
                    except asyncio.QueueFull: 
                        logger.warning("Binance queue overflow - purging and retrying.")
                        safe_drain_queue(queue)
                        try:
                            queue.put_nowait(message)
                        except asyncio.QueueFull:
                            pass
                
                logger.warning("Binance WebSocket closed cleanly. Draining stale queue signals...")
                safe_drain_queue(queue)
        except Exception as e:
            logger.warning(f"Binance WebSocket error ({type(e).__name__}). Draining stale queue signals...")
            safe_drain_queue(queue)

            if not reset_done and time.time() - conn_start > 10.0:
                attempt = 0
            attempt += 1
            if attempt > max_attempts:
                logger.critical("[FATAL] Binance connection limit reached. Stopping consumer.")
                engine.shutting_down = True
                break
            delay = calculate_backoff_delay(attempt)
            logger.warning(f"Binance WS error ({type(e).__name__}). Retrying in {delay:.2f}s...")
            await asyncio.sleep(delay)

async def kalshi_websocket_consumer(engine: LiveTradingEngine):
    if not isinstance(engine.broker, LiveKalshiBroker):
        return  # Only active for live/paper broker

    base_url = getattr(engine.broker, "base_url", "")
    if "demo" in base_url or "demo" in os.environ.get("BOT_ENV", "").lower():
        uri = "wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2"
    else:
        uri = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"

    attempt = 0
    max_attempts = 30
    
    while not engine.shutting_down:
        conn_start = time.time()
        reset_done = False
        try:
            current_time_ms = str(int(time.time() * 1000))
            # Signature generated for GET /trade-api/ws/v2 (paths starting with /trade-api/ bypass prefixing)
            sig = engine.broker._generate_signature(current_time_ms, "GET", "/trade-api/ws/v2")
            
            headers = {
                "KALSHI-ACCESS-KEY": engine.broker.key_id,
                "KALSHI-ACCESS-SIGNATURE": sig,
                "KALSHI-ACCESS-TIMESTAMP": current_time_ms
            }
            
            # Connect using standard websockets client
            async with websockets.connect(uri, extra_headers=headers, ssl=GLOBAL_SSL_CONTEXT, max_size=1048576, max_queue=256) as ws:
                logger.debug("Connected to Kalshi Private WebSocket Feed.")
                
                # Subscribe to private fill feed
                sub_message = {
                    "id": 1,
                    "cmd": "subscribe",
                    "params": {
                        "channels": ["fill"]
                    }
                }
                await ws.send(orjson.dumps(sub_message).decode('utf-8'))
                
                async for raw_msg in ws:
                    if engine.shutting_down: break
                    if not reset_done and time.time() - conn_start > 10.0:
                        attempt = 0  
                        reset_done = True
                        
                    try:
                        msg = orjson.loads(raw_msg)
                        msg_type = msg.get("type")
                        if msg_type == "fill":
                            fill_data = msg.get("msg")
                            if isinstance(fill_data, dict):
                                order_id = fill_data.get("order_id")
                                if order_id:
                                    if order_id in engine.active_tp_orders:
                                        await engine.active_tp_orders[order_id].put(fill_data)
                                    else:
                                        engine.orphan_fills.setdefault(order_id, []).append(fill_data)
                                        if len(engine.orphan_fills) > 50:
                                            engine.orphan_fills.pop(next(iter(engine.orphan_fills)))
                    except Exception as pe:
                        logger.warning(f"Error parsing Kalshi WS frame: {pe}")
                    
        except Exception as e:
            if not reset_done and time.time() - conn_start > 10.0:
                attempt = 0
            attempt += 1
            if attempt > max_attempts:
                logger.critical("[FATAL] Kalshi Private WS connection limit reached. Stopping consumer.")
                engine.shutting_down = True
                break
            delay = calculate_backoff_delay(attempt)
            logger.warning(f"Kalshi WS error ({type(e).__name__}). Retrying in {delay:.2f}s...")
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
def get_kalshi_credentials(secret_name: str, region_name: str = "us-east-1") -> Tuple[str, Any]:
    """
    Fetches credentials from Secrets Manager, instantly decodes 
    and loads the private key, and minimizes GC residency.
    """
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    try:
        response = client.get_secret_value(SecretId=secret_name)
        resp_json = response['SecretString']
        
        # Scrub original response object immediately
        del response
        
        resp_dict = orjson.loads(resp_json)
        del resp_json
        
        key_id = resp_dict["KEY_ID"]
        private_key_pem = bytearray(resp_dict["PRIVATE_KEY"], 'utf-8')
        
        # Scrub raw dictionary immediately
        del resp_dict
        
        private_key = load_pem_private_key(private_key_pem, password=None)
        
        # Scrub mutable bytearray buffer
        ctypes.memset((ctypes.c_char * len(private_key_pem)).from_buffer(private_key_pem), 0, len(private_key_pem))
        del private_key_pem
        
        return key_id, private_key
    except ClientError as e:
        logger.critical(f"AWS Secrets Manager client failure: {e.response['Error']['Code']}")
        sys.exit(1)
    except Exception as e:
        logger.critical(f"Failed to retrieve secrets from AWS: {type(e).__name__}")
        sys.exit(1)
    finally:
        # Force garbage collector to immediately wipe unreferenced immutable string pages
        gc.collect()

# Lock all module-level imports, classes, and configurations into the permanent 
# GC generation to prevent the garbage collector from scanning them during hot loops.
gc.freeze()

# ==========================================
# BOOTSTRAPPER
# ==========================================
if __name__ == "__main__":
    async def main():
        env_mode = os.environ.get("BOT_ENV", "simulation").lower()
        
        if env_mode in ["live", "paper"]:
            key_id, private_key = get_kalshi_credentials("prod/kalshi/api-keys", region_name="us-east-1")
            if env_mode == "live":
                confirm = os.environ.get("LIVE_TRADING_CONFIRM", "")
                if confirm != "I_ACCEPT_FINANCIAL_RISK":
                    logger.critical("Live mode blocked. Halting.")
                    sys.exit(1)
                logger.warning("!!! INITIALIZING LIVE TRADING BROKER !!!")
                broker = LiveKalshiBroker(key_id=key_id, private_key=private_key, paper_trade=False)
            else:
                logger.debug("Initializing PAPER TRADING Broker.")
                broker = LiveKalshiBroker(key_id=key_id, private_key=private_key, paper_trade=True)
            del private_key
            del key_id
            gc.collect()
        else:
            logger.debug("Initializing SIMULATION Broker.")
            broker = SimExecutionBroker()

        await broker.start()
        engine = LiveTradingEngine(broker)
        
        if sys.platform != "win32":
            import signal
            loop = asyncio.get_running_loop()
            def _on_sigterm():
                logger.warning("SIGTERM received from OS. Initiating shutdown...")
                engine.shutting_down = True
                for task in asyncio.all_tasks(loop):
                    name = task.get_name()
                    if name and ("consumer" in name or "worker" in name or "sync" in name):
                        task.cancel()
            loop.add_signal_handler(signal.SIGTERM, _on_sigterm)
        
        health_runner = None
        try:
            tick_queue = asyncio.Queue(maxsize=10000) 
            binance_queue = asyncio.Queue(maxsize=1000)
            
            health_runner = await start_health_server()
            
            async with asyncio.TaskGroup() as tg:
                tg.create_task(engine.sync_balance_loop(), name="sync_balance")
                tg.create_task(engine.sync_markets_loop(), name="sync_markets")
                tg.create_task(engine.sync_macro_calendar_loop(), name="sync_macro_calendar")
                tg.create_task(coinbase_websocket_consumer(engine, tick_queue), name="consumer")
                tg.create_task(market_worker_loop(engine, tick_queue), name="worker")
                tg.create_task(binance_websocket_consumer(engine, binance_queue), name="binance_consumer")
                tg.create_task(binance_worker_loop(engine, binance_queue), name="binance_worker")
                
                if isinstance(broker, LiveKalshiBroker):
                    if broker.paper_trade:
                        tg.create_task(engine.paper_fill_dispatcher(), name="paper_fill_dispatcher")
                    else:
                        tg.create_task(kalshi_websocket_consumer(engine), name="kalshi_consumer")
                
        except Exception as e:
            log_exception_group(e)
        finally:
            logger.debug("Executing final engine shutdown protocols...")
            if health_runner:
                try:
                    await health_runner.cleanup()
                except Exception as ex:
                    logger.warning(f"Error during health server cleanup: {ex}")
            await engine.shutdown()
            await broker.close()

    try: 
        asyncio.run(main())
    except KeyboardInterrupt: 
        logger.debug("Bot halted manually.")