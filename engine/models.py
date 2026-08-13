"""
Kalshi Quantitative Trading Engine (KQTE) - Data Models & Indicator Fallbacks

This module defines data models, state containers, and validation functions.
It includes Python fallback classes for the Rust technical indicators
(Bollinger Bands, EMA, RSI) that activate when the native library is unavailable.
"""

import re
import math
from decimal import Decimal, InvalidOperation
from collections import deque
from typing import Dict, Optional, Tuple, List, Set, Any
from pydantic import BaseModel, Field, ValidationError

import logging
logger = logging.getLogger("KalshiQuantEngine")

try:
    import kalshi_bot
except ImportError:
    logger.critical(
        "[HARDENING] Native Rust 'kalshi_bot' C-extension failed to import! "
        "Engaging Python fallback trackers. Strategy 4 (Index Lag) and Strategy 5 (OFI) will be INACTIVE."
    )
    class PyFastIndicators:
        """
        O(1) Python Fallback Technical Indicator Processor.
        Computes Exponential Moving Averages (EMA), Welford variance, Bollinger Bands,
        and Relative Strength Index (RSI) when native Rust C-extension is unavailable.
        """
        def __init__(self, period: int, alpha: float):
            self.period = period if period > 0 else 1
            self.alpha = max(0.0, min(1.0, alpha))
            self.ema = 0.0
            self.var = 0.0
            self.slow_ema = 0.0
            self.slow_alpha = self.alpha / 5.0
            self.count = 0
            self.avg_gain = 0.0
            self.avg_loss = 0.0
            self.last_price = 0.0

        def add_price(self, price: float):
            if not math.isfinite(price): return
            self.count += 1

            if self.count == 1:
                self.ema = price
                self.slow_ema = price
                self.var = 0.0
            else:
                diff = price - self.ema
                self.ema += self.alpha * diff
                self.var = (1.0 - self.alpha) * (self.var + self.alpha * diff * diff)
                
                slow_diff = price - self.slow_ema
                self.slow_ema += self.slow_alpha * slow_diff

            if self.count > 1:
                delta = price - self.last_price
                current_gain = delta if delta > 0.0 else 0.0
                current_loss = abs(delta) if delta < 0.0 else 0.0

                if self.count <= self.period + 1:
                    self.avg_gain += current_gain
                    self.avg_loss += current_loss
                    if self.count == self.period + 1:
                        self.avg_gain /= float(self.period)
                        self.avg_loss /= float(self.period)
                else:
                    p = float(self.period)
                    self.avg_gain = (self.avg_gain * (p - 1.0) + current_gain) / p
                    self.avg_loss = (self.avg_loss * (p - 1.0) + current_loss) / p

            self.last_price = price

        def add_price_with_volume(self, price: float, volume: float):
            if not math.isfinite(price) or not math.isfinite(volume) or volume <= 0.0: return
            vol_weight = min(3.0, math.log(1.0 + volume))
            effective_alpha = min(0.99, self.alpha * vol_weight)
            effective_slow_alpha = min(0.99, self.slow_alpha * vol_weight)

            self.count += 1
            if self.count == 1:
                self.ema = price
                self.slow_ema = price
                self.var = 0.0
            else:
                diff = price - self.ema
                self.ema += effective_alpha * diff
                self.var = (1.0 - effective_alpha) * (self.var + effective_alpha * diff * diff)

                slow_diff = price - self.slow_ema
                self.slow_ema += effective_slow_alpha * slow_diff

            if self.count > 1:
                delta = price - self.last_price
                current_gain = delta if delta > 0.0 else 0.0
                current_loss = abs(delta) if delta < 0.0 else 0.0

                if self.count <= self.period + 1:
                    self.avg_gain += current_gain
                    self.avg_loss += current_loss
                    if self.count == self.period + 1:
                        self.avg_gain /= float(self.period)
                        self.avg_loss /= float(self.period)
                else:
                    p = float(self.period)
                    self.avg_gain = (self.avg_gain * (p - 1.0) + current_gain) / p
                    self.avg_loss = (self.avg_loss * (p - 1.0) + current_loss) / p

            self.last_price = price

        def get_trend_alignment(self) -> float:
            return self.ema - self.slow_ema

        def get_rsi(self) -> float:
            if self.count < self.period + 1: return 50.0
            if self.avg_gain == 0.0 and self.avg_loss == 0.0: return 50.0
            if self.avg_loss == 0.0: return 100.0
            rs = self.avg_gain / self.avg_loss
            rsi = 100.0 - (100.0 / (1.0 + rs))
            return 50.0 if math.isnan(rsi) else rsi

        def get_bollinger_bands(self) -> Tuple[float, float, float]:
            std_dev = (self.var ** 0.5) if self.var > 0.0 else 0.0
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
        def update_and_check_persistence(self, timestamp: float, target_ratio: float, min_vol: float, persistence_sec: float) -> Tuple[str, int, bool]: return "", 0, False

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
        if not math.isfinite(tick_volume) or tick_volume <= 0.0:
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
        self.last_ofi_check_time: float = 0.0
        self.last_ofi_side: str = ""
        self.ofi_persistence_count: int = 0

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
        # Map to known ticker symbols to prevent partial matches like "BTCM", sorting by length descending for deterministic longest-prefix match
        for known in sorted(self._KNOWN_BASES, key=len, reverse=True):
            if base.startswith(known):
                return known
        return base

    def record(self, asset: str, hour: int, won: bool, pnl: float):
        key = (self._clean_asset(asset), hour)
        if key not in self.outcomes:
            # Cap total tracked keys to prevent unbounded growth, preserving core active asset histories
            if len(self.outcomes) >= 200:
                # Evict non-core assets first, or the least active key
                non_core_keys = [k for k in self.outcomes if k[0] not in self._KNOWN_BASES]
                evict_key = non_core_keys[0] if non_core_keys else min(self.outcomes.keys(), key=lambda k: len(self.outcomes[k]))
                self.outcomes.pop(evict_key, None)
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
    """Safely converts input to Decimal, avoiding runtime type crashes and NaN/Inf poisoning."""
    if val is None:
        return Decimal(default_val)
    if type(val) is Decimal:
        if val.is_nan() or val.is_infinite():
            return Decimal(default_val)
        return val
    try:
        if type(val) is float:
            res = Decimal(str(val)) # Required for floats to prevent IEEE 754 drift
        else:
            res = Decimal(val) # Natively handles strings and integers with 0 allocations
        if res.is_nan() or res.is_infinite():
            return Decimal(default_val)
        return res
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
