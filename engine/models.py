import re
from decimal import Decimal, InvalidOperation
from collections import deque
from typing import Dict, Optional, Tuple, List, Set, Any
from pydantic import BaseModel, Field, ValidationError

try:
    import kalshi_bot
except ImportError:
    class PyFastIndicators:
        def __init__(self, window_size: int, alpha: float):
            self.window_size = window_size
            self.alpha = alpha
            self.ema = 0.0
            self.var = 0.0
            self.initialized = False

        def add_price(self, price: float):
            if not self.initialized:
                self.ema = price
                self.initialized = True
            else:
                diff = price - self.ema
                self.ema += self.alpha * diff
                self.var = (1 - self.alpha) * (self.var + self.alpha * diff * diff)

        def add_price_with_volume(self, price: float, volume_k: float):
            self.add_price(price)

        def get_bollinger_bands(self) -> Tuple[float, float, float]:
            std_dev = (self.var ** 0.5) if self.var > 0 else 0.0
            return self.ema, self.ema + 2.0 * std_dev, self.ema - 2.0 * std_dev

    class DummyIndexLagTracker:
        def __init__(self, window_sec: float = 60.0): pass
        def add_tick(self, ts: float, price: float): pass
        def get_average(self) -> float: return 0.0
        def get_divergence(self, spot: float) -> float: return 0.0

    class DummyTakerOrderFlowTracker:
        def __init__(self, window_sec: float = 30.0): pass
        def add_trade(self, ts: float, vol: float, is_buy: bool): pass
        def get_metrics(self) -> Tuple[float, float, float]: return 0.0, 0.0, 1.0

    class DummyKalshiBotModule:
        FastIndicators = PyFastIndicators
        IndexLagTracker = DummyIndexLagTracker
        TakerOrderFlowTracker = DummyTakerOrderFlowTracker

    kalshi_bot = DummyKalshiBotModule()

from engine.config import config

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
        raw_vol = data.get("last_size")
        if raw_vol is None:
            return None
        tick_volume = float(raw_vol)
        if tick_volume <= 0.0:
            return None
    except (ValueError, TypeError):
        return None

    raw_side = data.get("side")
    if not isinstance(raw_side, str):
        return None
    side_clean = raw_side.lower()
    if side_clean not in ("buy", "sell"):
        return None

    return {
        "product_id": prod_id,
        "price": price_dec,
        "volume": tick_volume,
        "side": side_clean
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
        self.last_signal_time: float = 0.0

        # Transitioned to O(1) Exponential Variables
        self.tick_count: int = 0
        self.last_tick_time: float = 0.0
        self.fast_indicators = kalshi_bot.FastIndicators(14, float(config.EMA_ALPHA))
        self.index_lag_tracker = kalshi_bot.IndexLagTracker(60.0)
        self.taker_ofi_tracker = kalshi_bot.TakerOrderFlowTracker(30.0)
        self.consecutive_outliers: int = 0  
        
        # Restored to satisfy strict interface contracts and prevent AttributeError
        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}
        
        self.positions: Dict[str, int] = {}       
        self.position_sides: Dict[str, str] = {}  

class PerformanceTracker:
    """O(1) rolling performance tracker per asset/hour-of-day to auto-throttle losing regimes."""
    def __init__(self):
        # (asset, hour) -> deque of bools (won = True/False), capped at max 20 samples to avoid stale losses memory
        self.outcomes: Dict[Tuple[str, int], deque] = {}

    _KNOWN_BASES = frozenset({"BTC", "ETH", "SOL", "DOGE", "HYPE"})

    def _clean_asset(self, asset_id: str) -> str:
        """Extract base ticker symbol from Kalshi contract IDs.
        e.g. 'KXBTC15M-26JUN192015-15' -> 'BTC', 'KXDOGE15M-...' -> 'DOGE'
        """
        clean = asset_id.upper()
        # Strip known Kalshi prefixes
        for prefix in ("KXCRYPTO-", "KX"):
            if clean.startswith(prefix):
                clean = clean[len(prefix):]
                break
        # Extract leading alpha characters (e.g. "BTC" from "BTC15M-26JUN...")
        match = re.match(r'^([A-Z]+)', clean)
        base = match.group(1) if match else clean
        # Map to known ticker symbols to prevent partial matches like "BTCM"
        for known in self._KNOWN_BASES:
            if base.startswith(known):
                return known
        return base

    def record(self, asset: str, hour: int, won: bool, pnl: float):
        key = (self._clean_asset(asset), hour)
        if key not in self.outcomes:
            # SEC-29: Cap total tracked keys to prevent unbounded growth, enforcing O(1) space invariant
            if len(self.outcomes) >= 200:
                oldest_key = next(iter(self.outcomes))
                del self.outcomes[oldest_key]
            self.outcomes[key] = deque(maxlen=20)
        self.outcomes[key].append(won)

    def should_trade(self, asset: str, hour: int, min_samples: int = 5) -> bool:
        key = (self._clean_asset(asset), hour)
        data = self.outcomes.get(key)
        if not data or len(data) < min_samples:
            return True  # Not enough data, allow trading
        win_rate = sum(1 for w in data if w) / len(data)
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
