import os
import sys
import time
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
from decimal import Decimal, InvalidOperation
from dataclasses import dataclass
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
    TRADE_BUDGET_PCT: Decimal = Decimal("0.10")
    MAX_CONTRACTS_PER_TRADE: int = 500
    MAX_EXPOSURE_PER_EVENT: int = 1500
    
    DRAWDOWN_LIMIT_PCT: Decimal = Decimal(os.environ.get("DRAWDOWN_LIMIT_PCT", "0.20"))
    STALE_BALANCE_TIMEOUT_SEC: float = 120.0
    L2_MAX_DEPTH_PCT: float = 0.05
    
    BINANCE_LIQUIDATION_THRESHOLD: Decimal = Decimal(os.environ.get("BINANCE_LIQ_THRESHOLD", "1500000.0"))
    MAX_ALLOWED_SPREAD: Decimal = Decimal(os.environ.get("MAX_ALLOWED_SPREAD", "0.25"))

    Z_SCORE_THRESHOLD: float = float(os.environ.get("Z_SCORE_THRESHOLD", "2.5"))
    MIN_WELFORD_TICKS: int = int(os.environ.get("MIN_WELFORD_TICKS", "100"))
    MAX_FADE_PRICE: Decimal = Decimal(os.environ.get("MAX_FADE_PRICE", "0.45"))
    MAX_PRICE_DEVIATION_PCT: float = 0.15      
    CONSECUTIVE_OUTLIER_LIMIT: int = 5         
    STD_DEV_FLOOR: float = 0.05                

    LOCKOUT_BEFORE_SEC: float = float(os.environ.get("LOCKOUT_BEFORE_SEC", "1800.0"))
    LOCKOUT_AFTER_SEC: float = float(os.environ.get("LOCKOUT_AFTER_SEC", "1800.0"))
    
    TELEMETRY_LOG_INTERVAL_SEC: float = float(os.environ.get("TELEMETRY_LOG_INTERVAL_SEC", "300.0"))

config = BotConfig()

# Prevent redundant openSSL memory consumption by reusing context
GLOBAL_SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())

# SEC-01 Whitelist: Whitelist is only populated if explicitly defined in env variables
TRUSTED_INTERNAL_HOSTS = frozenset(
    h.strip().lower() for h in os.environ.get(
        "TRUSTED_INTERNAL_HOSTS", ""
    ).split(",") if h.strip()
)

# SSRF Note: Raw-IP bypass is mitigated by SafeResolver intercepting all DNS resolutions
# at the aiohttp connector level, and is_safe_destination_async validating all outbound URLs
# before connections are initiated. No additional monkeypatching required.

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
    prod_id = data.get("product_id")
    if prod_id not in ("BTC-USD", "ETH-USD"):
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
        
    return {
        "type": "ticker",
        "product_id": prod_id,
        "price": price_dec
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
        if not (Decimal("0.0001") < q < Decimal("5000.0")):
            return None
        if not (Decimal("0.01") < p < Decimal("200000.0")):
            return None
    except (ValueError, TypeError, InvalidOperation):
        return None
        
    return {
        "e": "forceOrder",
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

        self.welford_count: int = 0
        self.welford_mean: Decimal = Decimal("0.00")  
        self.welford_m2: Decimal = Decimal("0.00")
        self.ewma_price: Decimal = Decimal("0.00")
        self.consecutive_outliers: int = 0  

        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}
        
        self.positions: Dict[str, int] = {}       
        self.position_sides: Dict[str, str] = {}  

# ==========================================
# TYPE-SAFE FINANCIAL PARSING
# ==========================================
def safe_decimal(val, default_val: str = "0.00") -> Decimal:
    """Safely converts input to Decimal, avoiding runtime type crashes."""
    if val is None:
        return Decimal(default_val)
    try:
        return Decimal(str(val))
    except (ValueError, TypeError, InvalidOperation):
        return Decimal(default_val)

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
        
        # Static DNS mapping to bypass AWS ECS/VPC awsvpc extraHosts limitations
        if host_lower == "api.securesource.internal":
            calendar_ip = os.environ.get("ECONOMIC_CALENDAR_IP", "")
            if calendar_ip:
                return [
                    {
                        "hostname": host,
                        "host": calendar_ip,
                        "port": port,
                        "family": family,
                        "proto": 0,
                        "flags": 0,
                    }
                ]

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
        
        # Match application-level resolver override to prevent resolution failures on awsvpc
        if hostname_lower == "api.securesource.internal":
            calendar_ip = os.environ.get("ECONOMIC_CALENDAR_IP", "")
            if calendar_ip:
                return True
        
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
        self.calendar_secret_name = os.environ.get("ECONOMIC_CALENDAR_SECRET_NAME", "")
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

        headers = {}
        api_key_env = ""

        if self.calendar_secret_name:
            try:
                aws_session = boto3.session.Session()
                client = aws_session.client(service_name='secretsmanager', region_name="us-east-1")
                response = client.get_secret_value(SecretId=self.calendar_secret_name)
                secret_dict = orjson.loads(response['SecretString'])
                api_key_env = secret_dict.get("ECONOMIC_CALENDAR_API_KEY", "")
            except Exception as e:
                logger.error(f"[CIRCUIT BREAKER] Dynamic retrieval of credentials failed: {type(e).__name__}")
                return False
        else:
            api_key_env = os.environ.get("ECONOMIC_CALENDAR_API_KEY", "")

        # Build public FairEconomy request query parameters
        current_dt_utc = datetime.datetime.now(datetime.timezone.utc)
        from_date = current_dt_utc.date().isoformat()
        to_date = (current_dt_utc + datetime.timedelta(days=2)).date().isoformat()
        
        sep = "&" if "?" in self.calendar_url else "?"
        full_url = f"{self.calendar_url}{sep}from={from_date}&to={to_date}&apikey={api_key_env.strip()}"

        if not await is_safe_destination_async(full_url):
            logger.error("[CIRCUIT BREAKER] Aborting calendar fetch. Target fails boundary rules.")
            return False

        # FairEconomy / Forex Factory heavily proxies behind Cloudflare. 
        # Injecting a standard User-Agent prevents HTTP 403 blocks against Python aiohttp libraries.
        headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        headers["Accept"] = "application/json"

        try:
            timeout = aiohttp.ClientTimeout(total=5.0)
            async with session.get(full_url, headers=headers, timeout=timeout, allow_redirects=False) as response:
                if response.status != 200:
                    logger.error(f"[CIRCUIT BREAKER] FairEconomy calendar endpoint returned status: {response.status}")
                    return False

                content_length = response.headers.get("Content-Length")
                if content_length is not None:
                    try:
                        if int(content_length) > 512 * 1024:
                            logger.error("[CIRCUIT BREAKER] Aborting calendar parsing. Payload size exceeds safe limits (512 KB).")
                            return False
                    except ValueError:
                        pass

                body_bytes = await response.content.read(512 * 1024)
                if len(body_bytes) >= 512 * 1024:
                    logger.error("[CIRCUIT BREAKER] Ingestion aborted. Calendar buffer length exceeded maximum limits.")
                    return False

                parsed_json = orjson.loads(body_bytes)
                if not isinstance(parsed_json, list):
                    logger.error("[CIRCUIT BREAKER] Expected JSON list from FairEconomy API.")
                    return False
                
                # Integrated FairEconomy Adapter: Map third-party data structure to local EconomicEvent schemas
                mapped_events = []
                for item in parsed_json:
                    try:
                        raw_date = item.get("date", "")
                        if not raw_date:
                            continue
                        
                        # Parse FairEconomy ISO-8601 Datetime String (e.g. "2026-06-07T08:30:00-04:00")
                        dt = datetime.datetime.fromisoformat(raw_date)
                        timestamp = dt.timestamp()
                        
                        # FairEconomy maps the event title to "title"
                        raw_event = item.get("title", "Economic Release")
                        clean_event = "".join(c for c in raw_event if c.isalnum() or c in " -()%./")
                        clean_event = clean_event[:100]
                        if not clean_event:
                            clean_event = "Economic Release"
                            
                        # Normalize mixed-case impact levels to uppercase
                        impact = str(item.get("impact", "LOW")).upper()
                        if impact not in ("HIGH", "MEDIUM", "LOW"):
                            impact = "LOW"
                            
                        mapped_events.append({
                            "event": clean_event,
                            "timestamp": timestamp,
                            "impact": impact
                        })
                    except Exception as parse_err:
                        # CWE-117 Fix: Sanitize exception outputs containing untrusted input
                        logger.warning(
                            f"[CIRCUIT BREAKER] Failed parsing individual calendar event: "
                            f"{sanitize_log_str(str(parse_err))}"
                        )
                        continue
                
                reconstructed_response = {"events": mapped_events}
                validated_data = EconomicCalendarResponse(**reconstructed_response)

                current_time = time.time()
                self.active_events = [
                    ev for ev in validated_data.events
                    if ev.timestamp > current_time - self.lockout_after and ev.timestamp < current_time + 172800.0
                ]
                return True
        except ValidationError as e:
            logger.error(f"[CIRCUIT BREAKER] Economic calendar structure schema mismatch: {sanitize_log_str(str(e))[:150]}")
            return False
        except orjson.JSONDecodeError:
            logger.error("[CIRCUIT BREAKER] Invalid JSON payload from calendar source.")
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
    """Responds with standard HTTP status code using optimized serializer."""
    return web.json_response(
        {"status": "ok", "service": "kalshi-quant-engine"},
        dumps=lambda x: orjson.dumps(x).decode('utf-8')
    )

async def start_health_server() -> web.AppRunner:
    """Spawns lightweight background HTTP server to resolve AWS ALB status checks."""
    app = web.Application()
    app.router.add_get('/', handle_health_check)
    app.router.add_get('/health', handle_health_check)
    
    runner = web.AppRunner(app)
    await runner.setup()
    
    port = int(os.environ.get("PORT", "8080"))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    logger.info(f"[HEALTH SERVER] Micro HTTP health responder online, listening on port {port}")
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
    async def get_best_bid_ask(self, contract_id: str, side: str) -> Optional[Tuple[Decimal, Decimal]]: pass

    @abstractmethod
    async def get_order_details(self, order_id: str) -> dict: pass
    
    @abstractmethod
    async def get_order_by_client_id(self, client_order_id: str) -> dict: pass

    @abstractmethod
    async def cancel_order(self, order_id: str) -> bool: pass

    @abstractmethod
    async def execute_trade(self, action: str, contract_id: str, side: str, limit_price: Decimal, quantity: int, client_order_id: str = None) -> Optional[str]: pass

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
        logger.info("[SIMULATION] Offline Broker resources initialized.")

    async def close(self):
        logger.info("[SIMULATION] Broker resources cleaned up.")

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

    async def get_best_bid_ask(self, contract_id: str, side: str) -> Optional[Tuple[Decimal, Decimal]]:
        return Decimal("0.40"), Decimal("0.45")

    async def get_order_details(self, order_id: str) -> dict:
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

class LiveKalshiBroker(ExecutionBroker):
    def __init__(self, key_id: str, private_key: Any, paper_trade: bool = False):
        self.base_url = "https://external-api.kalshi.com/trade-api/v2"
        self.key_id = key_id
        self.session = None
        self.paper_trade = paper_trade
        self._paper_orders: Dict[str, int] = {}
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

    def _generate_signature(self, timestamp: str, method: str, path: str) -> str:
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
                    data = await response.json()
                    available_balance = safe_decimal(data.get("balance", 0)) / Decimal("100.00")
                    portfolio_value = safe_decimal(data.get("portfolio_value", 0)) / Decimal("100.00")
                    return available_balance, portfolio_value
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
                    data = await response.json()
                    orders = data.get("orders", [])
                    for order in orders:
                        unfilled = safe_decimal(order.get("unfilled_count", 0))
                        price_val = order.get("yes_price") or order.get("no_price")
                        price = safe_decimal(price_val, "0.00")
                        locked_capital += (unfilled * (price / Decimal("100.00")))
        except Exception as e:
            logger.error(f"[API] Error fetching resting orders: {type(e).__name__}", exc_info=True)
        return locked_capital

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

    async def get_best_bid_ask(self, contract_id: str, side: str) -> Optional[Tuple[Decimal, Decimal]]:
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
                        best_yes_bid = safe_decimal(yes_bids[0][0])
                        best_no_bid = safe_decimal(no_bids[0][0])
                    elif ob_standard:
                        yes_bids = ob_standard.get("yes", [])
                        no_bids = ob_standard.get("no", [])
                        if not yes_bids or not no_bids: return None
                        best_yes_bid = safe_decimal(yes_bids[0][0]) / Decimal("100.00")
                        best_no_bid = safe_decimal(no_bids[0][0]) / Decimal("100.00")
                    else: return None
                    
                    if side.lower() == "yes": 
                        return best_yes_bid, (Decimal("1.00") - best_no_bid)
                    else: 
                        return best_no_bid, (Decimal("1.00") - best_yes_bid)
        except Exception as e: 
            logger.error("Orderbook fetch error", exc_info=True)
        return None

    async def get_order_details(self, order_id: str) -> dict:
        if order_id.startswith("paper-"):
            qty = self._paper_orders.pop(order_id, 0)
            return {"status": "executed", "executed_count": str(qty), "unfilled_count": "0"}

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
                    data = await resp.json()
                    orders = data.get("orders", [])
                    if orders: return orders[0] 
        except Exception as e: 
            logger.error(f"Error fetching order by client ID {client_order_id[:20]}...", exc_info=True)  
        return {}

    async def cancel_order(self, order_id: str) -> bool:
        if order_id.startswith("paper-"): return True
        
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
        if action.lower() not in self.VALID_ACTIONS or side.lower() not in self.VALID_SIDES: return None
            
        if self.paper_trade:
            order_id = f"paper-{uuid.uuid4().hex}"
            self._paper_orders[order_id] = quantity
            total_trade_value = limit_price * Decimal(quantity)
            if action.lower() == "buy":
                self._paper_balance -= total_trade_value
            elif action.lower() == "sell":
                self._paper_balance += total_trade_value
                
            logger.warning(f"[PAPER TRADE EXECUTED] {action.upper()} {quantity}x {contract_id} '{side.upper()}' @ ${limit_price:.2f}")
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
                    logger.error(f"[API ERROR] Trade rejected (HTTP {response.status}): {sanitize_log_str(str(err_msg))[:250]}")
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
        self.starting_balance: Decimal = Decimal("0.00")
        self.available_balance: Decimal = Decimal("0.00")
        self.capital_in_flight: Decimal = Decimal("0.00") 
        self.consecutive_api_failures: int = 0
        self.last_sync_time: float = 0.0
        self.last_telemetry_log_time: float = 0.0  
        
        self.active_trade_count: int = 0
        self._binance_events_received: int = 0 
        self.shutting_down: bool = False

        self.circuit_breaker = MacroCircuitBreaker(
            lockout_before_sec=config.LOCKOUT_BEFORE_SEC,
            lockout_after_sec=config.LOCKOUT_AFTER_SEC
        )
        
        env_starting_bal = os.environ.get("STARTING_BALANCE")
        if env_starting_bal is not None:
            try:
                self.starting_balance = Decimal(env_starting_bal)
                logger.info(f"[RISK MANAGER] Bound to absolute starting balance: ${self.starting_balance:.2f}")
            except (ValueError, InvalidOperation):
                logger.warning(f"Malformed STARTING_BALANCE env var: {env_starting_bal}. Falling back to dynamic initialization.")

        if os.environ.get("BOT_ENV", "simulation").lower() == "simulation":
            mock_event = EconomicEvent(
                event="Mock Federal Reserve FOMC Statement",
                timestamp=time.time() + 600.0,
                impact="HIGH"
            )
            self.circuit_breaker.active_events.append(mock_event)
            logger.info(f"[CIRCUIT BREAKER] Offline simulation detected. Injected mock HIGH-impact event.")
        
        self.balance_lock = asyncio.Lock() 
        self.trade_cap_lock = asyncio.Lock()
        self.api_failure_lock = asyncio.Lock()
        
        self.assets: Dict[str, AssetState] = {
            "BTC-USD": AssetState(),
            "ETH-USD": AssetState(),
        }
        self._pending_tasks: Set[asyncio.Task] = set()

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

    def _get_filled_qty_from_details(self, details: dict, requested_qty: int) -> int:
        try:
            maker_fill = safe_decimal(details.get("maker_fill_count"), "0.00")
            taker_fill = safe_decimal(details.get("taker_fill_count"), "0.00")
            exec_fill = safe_decimal(details.get("executed_count"), "0.00")
            
            total_fill = int(maker_fill + taker_fill + exec_fill)
            if total_fill > 0: return total_fill
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
            logger.info(f"Draining {len(self._pending_tasks)} in-flight tasks...")
            done, pending = await asyncio.wait(self._pending_tasks, timeout=10.0)
            if pending:
                logger.warning(f"Cancelling {len(pending)} unresolved execution tasks before broker closure...")
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
            logger.info("Task drain complete.")

    def purge_memory(self, queue: asyncio.Queue = None):
        current_time = time.time()
        for symbol, state in self.assets.items():
            state.cooldown_until = current_time + 15.0
            state.bids.clear()
            state.asks.clear()
        if queue:
            # Drain queue safely while keeping counter synchronized
            safe_drain_queue(queue)

    def _prune_orderbook(self, ob: dict, current_price: float, is_bid: bool, max_depth: float = config.L2_MAX_DEPTH_PCT):
        threshold_price = current_price * (1.0 - max_depth) if is_bid else current_price * (1.0 + max_depth)
        stale_keys = [p for p in ob if (p < threshold_price if is_bid else p > threshold_price)]
        for k in stale_keys:
            del ob[k]
            
        if len(ob) > 1000:
            sorted_keys = sorted(ob.keys(), reverse=is_bid)
            for k in sorted_keys[1000:]:
                del ob[k]

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
                    
                    if self.starting_balance == Decimal("0.00"):
                        self.starting_balance = portfolio_val
                    
                    if self.starting_balance > 0:
                        drawdown = (self.starting_balance - portfolio_val) / self.starting_balance
                        if drawdown >= config.DRAWDOWN_LIMIT_PCT:
                            logger.critical(f"DRAWDOWN LIMIT REACHED ({drawdown*100:.1f}%). Halting operations.")
                            self.shutting_down = True
                
                current_time = time.time()
                if current_time - self.last_telemetry_log_time >= config.TELEMETRY_LOG_INTERVAL_SEC:
                    self.last_telemetry_log_time = current_time
                    
                    asset_status_summaries = []
                    for symbol, state in self.assets.items():
                        if state.last_price is not None:
                            variance = state.welford_m2 / Decimal(str(state.welford_count)) if state.welford_count > 0 else Decimal("0.00")
                            std_dev = math.sqrt(float(variance)) if variance > Decimal("0.00") else 0.0
                            
                            if std_dev >= config.STD_DEV_FLOOR:
                                z_score_val = float((Decimal(str(state.last_price)) - state.ewma_price) / Decimal(str(std_dev)))
                                z_score_repr = f"{z_score_val:+.2f}"
                            else:
                                z_score_repr = "N/A (Low Volatility)"
                                
                            asset_status_summaries.append(
                                f"{symbol} [Price: ${state.last_price:.2f} | EWMA: ${float(state.ewma_price):.2f} | Ticks: {state.welford_count}/{config.MIN_WELFORD_TICKS} | Z-Score: {z_score_repr}]"
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
                    logger.info(f"[MARKET ROUTER] {symbol} Locked onto valid contract: {contract_id}")
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
                    logger.info(f"[CIRCUIT BREAKER] Economic calendar synchronized.")
                else:
                    logger.warning("[CIRCUIT BREAKER] Calendar synchronization returned no updates or failed validation.")
            except Exception as e:
                logger.error(f"[CIRCUIT BREAKER] Critical calendar update routine failure: {type(e).__name__}")
                
            await asyncio.sleep(21600)  # Refresh every 6 hours

    async def execute_and_hold_entry(self, state: AssetState, contract_id: str, side: str, limit_price: Decimal, quantity: int, total_cost: Decimal, seconds_left: float):
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
                    
                logger.info(f"[{contract_id}] BUY Order active. Verifying fill status...")

                filled_qty = 0
                poll_interval = 2.0
                elapsed = 0.0
                timeout = 10.0 

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
                        await self.broker.cancel_order(order_id)
                        unfilled_qty = quantity - filled_qty
                        refund = Decimal(str(unfilled_qty)) * limit_price
                        async with self.balance_lock:
                            self.capital_in_flight = max(Decimal("0.00"), self.capital_in_flight - refund)
                            self.available_balance += refund
                            
                            state.positions[contract_id] = max(0, state.positions.get(contract_id, 0) - unfilled_qty)
                            if state.positions[contract_id] <= 0:
                                state.position_sides.pop(contract_id, None)
                                state.positions.pop(contract_id, None)
                        locked_capital -= refund
                        logger.info(f"[{contract_id}] Partial Fill ({filled_qty}/{quantity}).")
                    else:
                        logger.info(f"[{contract_id}] Order filled ({filled_qty}/{quantity}).")

                    async with self.balance_lock:
                        self.capital_in_flight = max(Decimal("0.00"), self.capital_in_flight - locked_capital)
                    
                    logger.warning(f"[{contract_id}] Position Secured. Execution task safely terminating.")
                    return
                else:
                    logger.warning(f"[{contract_id}] Limit buy missed fill window. Canceling.")
                    await self.broker.cancel_order(order_id)
                    async with self.balance_lock:
                        self.capital_in_flight = max(Decimal("0.00"), self.capital_in_flight - locked_capital)
                        self.available_balance += locked_capital
                        
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
                    self.capital_in_flight = max(Decimal("0.00"), self.capital_in_flight - locked_capital)
                    self.available_balance += locked_capital
                    
                    state.positions[contract_id] = max(0, state.positions.get(contract_id, 0) - quantity)
                    if state.positions[contract_id] <= 0:
                        state.position_sides.pop(contract_id, None)
                        state.positions.pop(contract_id, None)

        except Exception as e:
            logger.critical(f"[{contract_id}] Unhandled exception in entry manager. Forcing release.", exc_info=True)
            if locked_capital > 0:
                async with self.balance_lock:
                    self.capital_in_flight = max(Decimal("0.00"), self.capital_in_flight - locked_capital)
                    self.available_balance += locked_capital
                    
                    state.positions[contract_id] = max(0, state.positions.get(contract_id, 0) - quantity)
                    if state.positions[contract_id] <= 0:
                        state.position_sides.pop(contract_id, None)
                        state.positions.pop(contract_id, None)
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
            
            reference_price = state.last_price
            if not reference_price and state.bids and state.asks:
                try:
                    best_bid = max(state.bids.keys())
                    best_ask = min(state.asks.keys())
                    reference_price = (best_bid + best_ask) / 2.0
                except ValueError:
                    pass

            if reference_price:
                if len(state.bids) > 1000: self._prune_orderbook(state.bids, reference_price, is_bid=True)
                if len(state.asks) > 1000: self._prune_orderbook(state.asks, reference_price, is_bid=False)
            return

        if msg_type != "ticker": return
        
        tick_dict = validate_tick_data(parsed_dict)
        if not tick_dict: 
            return

        tick_price = tick_dict["price"]

        # Anomaly Filter with Safe Baseline Reset (SEC-01)
        if state.ewma_price > Decimal("0.00"):
            percentage_deviation = abs(tick_price - state.ewma_price) / state.ewma_price
            if percentage_deviation > Decimal(str(config.MAX_PRICE_DEVIATION_PCT)):
                state.consecutive_outliers += 1
                if state.consecutive_outliers >= config.CONSECUTIVE_OUTLIER_LIMIT:
                    logger.warning(f"[OUTLIER SHIELD] Detected {state.consecutive_outliers} consecutive outlier ticks. Forcing baseline reset.")
                    state.ewma_price = tick_price
                    state.consecutive_outliers = 0
                    state.welford_count = 1
                    state.welford_mean = tick_price
                    state.welford_m2 = Decimal("0.00")
                    state.last_price = float(tick_price)
                    return  
                else:
                    logger.warning(f"[OUTLIER SHIELD] Ignored anomalous price change on {product_id}: ${tick_price:.2f} vs EWMA: ${state.ewma_price:.2f}")
                    return
            else:
                state.consecutive_outliers = 0

        current_time = time.time()
        
        # Update Welford computations
        state.welford_count += 1
        delta = tick_price - state.welford_mean
        state.welford_mean += delta / Decimal(str(state.welford_count))
        delta2 = tick_price - state.welford_mean
        state.welford_m2 += delta * delta2
        
        # Update EMA Anchor
        if state.ewma_price == Decimal("0.00"):
            state.ewma_price = tick_price
        else:
            alpha = Decimal("0.01")  
            state.ewma_price = (tick_price * alpha) + (state.ewma_price * (Decimal("1.00") - alpha))

        if current_time < state.cooldown_until:
            state.last_price = float(tick_price)
            return

        if self.circuit_breaker.is_locked_out():
            state.last_price = float(tick_price)
            return

        last_price = state.last_price
        state.last_price = float(tick_price)
        if not last_price: return

        # Only discard tracked contracts if they both are inactive and have no active tracking positions (val == 0)
        if state.active_contract_id and state.active_contract_id != state.last_seen_contract_id:
            state.last_seen_contract_id = state.active_contract_id
            async with self.balance_lock:
                active_ids = {state.active_contract_id}
                state.positions = {cid: val for cid, val in state.positions.items() if cid in active_ids or val > 0}
                state.position_sides = {cid: s for cid, s in state.position_sides.items() if cid in active_ids or cid in state.positions}

        if state.ewma_price == Decimal("0.00") or not state.active_contract_id: return
        if state.expiration_time == 0.0: return
        seconds_left = state.expiration_time - current_time

        if seconds_left > 480.0: return 
        if seconds_left < 180.0: return 
        if state.welford_count < config.MIN_WELFORD_TICKS: return

        variance = state.welford_m2 / Decimal(str(state.welford_count))
        std_dev = Decimal(str(math.sqrt(float(variance)))) if variance > Decimal("0.00") else Decimal("0.00")
        
        if std_dev < Decimal(str(config.STD_DEV_FLOOR)):
            return
        
        z_score = float((tick_price - state.ewma_price) / std_dev)
        
        trade_side = None
        if z_score > config.Z_SCORE_THRESHOLD:
            trade_side = "NO"
        elif z_score < -config.Z_SCORE_THRESHOLD:
            trade_side = "YES"
            
        if not trade_side: return 

        executing_contract_id = state.active_contract_id
        current_pos_side = state.position_sides.get(executing_contract_id)

        if current_pos_side and current_pos_side != trade_side: return 

        # Use try/finally block to safely release slot if setup steps fail or raise exceptions
        async with self.trade_cap_lock:
            if self.active_trade_count >= config.MAX_CONCURRENT_TRADES: return
            self.active_trade_count += 1

        slot_acquired = True
        try:
            best_vals = await self.broker.get_best_bid_ask(executing_contract_id, trade_side)
            if not best_vals:
                return
            
            best_bid, best_ask = best_vals
            spread = best_ask - best_bid
            
            if spread > config.MAX_ALLOWED_SPREAD or best_ask > config.MAX_FADE_PRICE or best_bid < Decimal("0.01"):
                return
            
            limit_price = max(Decimal("0.01"), min(Decimal("0.99"), best_ask))
            
            should_decrement = False
            async with self.balance_lock:
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
                        total_cost = Decimal(quantity) * limit_price
                        self.available_balance -= total_cost
                        self.capital_in_flight += total_cost
                        
                        state.position_sides[executing_contract_id] = trade_side
                        state.positions[executing_contract_id] = actual_pos_size + quantity
                        state.cooldown_until = current_time + 15.0
            
            if should_decrement:
                return
            
            logger.warning(f"[{product_id}] EDGE FOUND. Z-Score: {z_score:.2f} | Ask: ${best_ask:.2f} | Sniping {quantity} contracts.")
            
            # Handover slot ownership to spawned background execution task
            slot_acquired = False 
            
            exec_task = asyncio.create_task(
                self.execute_and_hold_entry(
                    state, executing_contract_id, trade_side, limit_price, quantity, total_cost, seconds_left
                )
            )
            self._pending_tasks.add(exec_task)
            exec_task.add_done_callback(self._handle_task_done)
        finally:
            if slot_acquired:
                await self._decrement_trade_cap()
        return 

    # ==========================================
    # BINANCE LIQUIDATION SNIPER
    # ==========================================
    async def process_binance_liquidation(self, raw_bytes: bytes):
        if self.shutting_down: return
        
        self._binance_events_received += 1
        if self._binance_events_received % 500 == 0:
            logger.info(f"[BINANCE FEED] {self._binance_events_received} total events processed.")
            
        try:
            parsed_dict = orjson.loads(raw_bytes)
            
            payload_dict = validate_binance_payload(parsed_dict)
            if not payload_dict: 
                return
                
            symbol = payload_dict["o"]["s"]
            if "BTC" in symbol: asset_symbol = "BTC-USD"
            elif "ETH" in symbol: asset_symbol = "ETH-USD"
            else: return
            
            state = self.assets.get(asset_symbol)
            if not state or not state.active_contract_id: return
            
            notional = payload_dict["o"]["p"] * payload_dict["o"]["q"]
            if notional < config.BINANCE_LIQUIDATION_THRESHOLD: return 

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
            
            executing_contract_id = state.active_contract_id
            current_pos_side = state.position_sides.get(executing_contract_id)

            if current_pos_side and current_pos_side != trade_side: return
            
            # Use try/finally block to safely release slot if setup steps fail or raise exceptions
            async with self.trade_cap_lock:
                if self.active_trade_count >= config.MAX_CONCURRENT_TRADES: return
                self.active_trade_count += 1
                
            slot_acquired = True
            try:
                best_vals = await self.broker.get_best_bid_ask(executing_contract_id, trade_side)
                if not best_vals:
                    return
                    
                best_bid, best_ask = best_vals
                
                if best_ask >= Decimal("0.85") or best_bid < Decimal("0.01") or (best_ask - best_bid) > config.MAX_ALLOWED_SPREAD:
                    return
                    
                limit_price = max(Decimal("0.01"), min(Decimal("0.99"), best_ask))
                
                should_decrement = False
                async with self.balance_lock:
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
                            total_cost = Decimal(quantity) * limit_price
                            self.available_balance -= total_cost
                            self.capital_in_flight += total_cost
                            
                            state.position_sides[executing_contract_id] = trade_side
                            state.positions[executing_contract_id] = actual_pos_size + quantity
                            state.cooldown_until = current_time + 15.0
                
                if should_decrement:
                    return
                
                logger.warning(f"[{asset_symbol}] BINANCE LIQUIDATION SIGNAL (${notional:,.2f})! Ask: ${best_ask:.2f} | Sniping {quantity} contracts.")
                
                # Handover slot ownership to background execution task
                slot_acquired = False
                
                seconds_left = state.expiration_time - current_time if state.expiration_time else 900.0
                
                exec_task = asyncio.create_task(
                    self.execute_and_hold_entry(
                        state, executing_contract_id, trade_side, limit_price, quantity, total_cost, seconds_left
                    )
                )
                self._pending_tasks.add(exec_task)
                exec_task.add_done_callback(self._handle_task_done)
            finally:
                if slot_acquired:
                    await self._decrement_trade_cap()
                
        except Exception as e:
            logger.error("Liquidation processing fault", exc_info=True) 

# ==========================================
# ASYNC QUEUES
# ==========================================
async def coinbase_websocket_consumer(engine: LiveTradingEngine, queue: asyncio.Queue):
    uri = "wss://ws-feed.exchange.coinbase.com" 
    subscribe_message = {
        "type": "subscribe",
        "product_ids": ["BTC-USD", "ETH-USD"],
        "channels": ["ticker", "level2"]
    }

    attempt = 0
    max_attempts = 30
    while not engine.shutting_down:
        conn_start = time.time()
        reset_done = False
        try:
            async with websockets.connect(uri, ssl=GLOBAL_SSL_CONTEXT, max_size=1048576, max_queue=256) as ws:
                logger.info("Connected to Coinbase Live Spot Feed.")
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
    uri = "wss://fstream.binance.com/ws/!forceOrder@arr"
    
    attempt = 0
    max_attempts = 30
    while not engine.shutting_down:
        conn_start = time.time()
        reset_done = False
        try:
            async with websockets.connect(uri, ssl=GLOBAL_SSL_CONTEXT, max_size=1048576, max_queue=256) as ws:
                logger.info("Connected to Binance Futures Feed.")
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
    and loads the private key, and zeros out the intermediate PEM 
    material before returning.
    """
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    try:
        response = client.get_secret_value(SecretId=secret_name)
        resp_dict = orjson.loads(response['SecretString'])
        
        key_id = resp_dict["KEY_ID"]
        private_key_pem = bytearray(resp_dict["PRIVATE_KEY"], 'utf-8')
        
        # Load the key in-memory directly
        private_key = load_pem_private_key(private_key_pem, password=None)
        
        # Clean raw PEM buffer immediately inside stack frame
        ctypes.memset((ctypes.c_char * len(private_key_pem)).from_buffer(private_key_pem), 0, len(private_key_pem))
        
        return key_id, private_key
    except ClientError as e:
        logger.critical(f"AWS Secrets Manager client failure: {e.response['Error']['Code']}")
        sys.exit(1)
    except Exception as e:
        logger.critical(f"Failed to retrieve secrets from AWS: {type(e).__name__}")
        sys.exit(1)
    finally:
        # Purge temporary allocations from scope
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
                logger.info("Initializing PAPER TRADING Broker.")
                broker = LiveKalshiBroker(key_id=key_id, private_key=private_key, paper_trade=True)
        else:
            logger.info("Initializing SIMULATION Broker.")
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
                
        except Exception as e:
            log_exception_group(e)
        finally:
            logger.info("Executing final engine shutdown protocols...")
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
        logger.info("Bot halted manually.")