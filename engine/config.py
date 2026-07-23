import os
import re
import ssl
import certifi
from decimal import Decimal
from dataclasses import dataclass, field
from typing import Dict

@dataclass
class BotConfig:
    MAX_CONCURRENT_TRADES: int = 2
    TRADE_BUDGET_PCT: Decimal = Decimal(os.environ.get("TRADE_BUDGET_PCT", "0.03"))
    MAX_CONTRACTS_PER_TRADE: int = 100
    MAX_EXPOSURE_PER_EVENT: int = 100
    
    DRAWDOWN_LIMIT_PCT: Decimal = Decimal(os.environ.get("DRAWDOWN_LIMIT_PCT", "0.25"))
    STALE_BALANCE_TIMEOUT_SEC: float = 120.0
    
    BINANCE_LIQUIDATION_THRESHOLDS: Dict[str, Decimal] = field(default_factory=lambda: {
        "BTC-USD": Decimal(os.environ.get("BINANCE_LIQ_THRESHOLD_BTC", "1500000.0")),
        "HYPE-USD": Decimal(os.environ.get("BINANCE_LIQ_THRESHOLD_HYPE", "100000.0")),
        "SOL-USD": Decimal(os.environ.get("BINANCE_LIQ_THRESHOLD_SOL", "300000.0")),
        "ETH-USD": Decimal(os.environ.get("BINANCE_LIQ_THRESHOLD_ETH", "750000.0")),
        "DOGE-USD": Decimal(os.environ.get("BINANCE_LIQ_THRESHOLD_DOGE", "300000.0"))
    })
    MAX_ALLOWED_SPREAD: Decimal = Decimal(os.environ.get("MAX_ALLOWED_SPREAD", "0.18"))
    MAX_ENTRY_PRICE_YES: Decimal = Decimal(os.environ.get("MAX_ENTRY_PRICE_YES", "0.55"))
    MAX_ENTRY_PRICE_NO: Decimal = Decimal(os.environ.get("MAX_ENTRY_PRICE_NO", "0.75"))

    MIN_EMA_TICKS: int = int(os.environ.get("MIN_EMA_TICKS", "1000"))
    MAX_PRICE_DEVIATION_PCT: float = 0.15      
    CONSECUTIVE_OUTLIER_LIMIT: int = 5         
    STD_DEV_FLOORS_PCT: Dict[str, float] = field(default_factory=lambda: {
        "BTC-USD": float(os.environ.get("STD_DEV_FLOOR_PCT_BTC", "0.00013")),
        "ETH-USD": float(os.environ.get("STD_DEV_FLOOR_PCT_ETH", "0.00016")),
        "SOL-USD": float(os.environ.get("STD_DEV_FLOOR_PCT_SOL", "0.00033")),
        "DOGE-USD": float(os.environ.get("STD_DEV_FLOOR_PCT_DOGE", "0.00066")),
        "HYPE-USD": float(os.environ.get("STD_DEV_FLOOR_PCT_HYPE", "0.005"))
    })

    LOCKOUT_BEFORE_SEC: float = float(os.environ.get("LOCKOUT_BEFORE_SEC", "1800.0"))
    LOCKOUT_AFTER_SEC: float = float(os.environ.get("LOCKOUT_AFTER_SEC", "1800.0"))
    
    TELEMETRY_LOG_INTERVAL_SEC: float = float(os.environ.get("TELEMETRY_LOG_INTERVAL_SEC", "300.0"))
    EMA_ALPHA: Decimal = Decimal(os.environ.get("EMA_ALPHA", "0.015"))
    
    # Dynamic TP Range parameters
    MIN_TP_ROI: Decimal = Decimal(os.environ.get("MIN_TP_ROI", "1.50"))
    MAX_TP_ROI: Decimal = Decimal(os.environ.get("MAX_TP_ROI", "1.85"))
    
    # Safety indicators thresholds
    STRIKE_SAFETY_BUFFER_SD: float = float(os.environ.get("STRIKE_SAFETY_BUFFER_SD", "0.5"))

    # Strategy 4: Index Lag Arbitrage Config
    ENABLE_INDEX_LAG_STRATEGY: bool = os.environ.get("ENABLE_INDEX_LAG_STRATEGY", "true").lower() == "true"
    INDEX_LAG_MIN_DIVERGENCE: Decimal = Decimal(os.environ.get("INDEX_LAG_MIN_DIVERGENCE", "0.0012"))

    # Strategy 5: Taker Order Flow Imbalance (OFI) Config
    ENABLE_OFI_STRATEGY: bool = os.environ.get("ENABLE_OFI_STRATEGY", "true").lower() == "true"
    OFI_BUY_SELL_RATIO: Decimal = Decimal(os.environ.get("OFI_BUY_SELL_RATIO", "3.5"))
    OFI_MIN_VOLUME_NOTIONAL: Decimal = Decimal(os.environ.get("OFI_MIN_VOLUME_NOTIONAL", "50000.0"))

    def __post_init__(self):
        """SEC-06: Bounds-validate all environment-sourced config to prevent adversarial misconfiguration."""
        if not (Decimal("0.001") <= self.TRADE_BUDGET_PCT <= Decimal("0.20")):
            raise ValueError(f"TRADE_BUDGET_PCT={self.TRADE_BUDGET_PCT} out of safe range [0.001, 0.20]")
        if not (Decimal("0.05") <= self.DRAWDOWN_LIMIT_PCT <= Decimal("0.50")):
            raise ValueError(f"DRAWDOWN_LIMIT_PCT={self.DRAWDOWN_LIMIT_PCT} out of safe range [0.05, 0.50]")
        if not (0.0 <= self.LOCKOUT_BEFORE_SEC <= 7200.0):
            raise ValueError(f"LOCKOUT_BEFORE_SEC={self.LOCKOUT_BEFORE_SEC} out of safe range [0, 7200]")
        if not (0.0 <= self.LOCKOUT_AFTER_SEC <= 7200.0):
            raise ValueError(f"LOCKOUT_AFTER_SEC={self.LOCKOUT_AFTER_SEC} out of safe range [0, 7200]")
        if not (Decimal("0.01") <= self.MAX_ALLOWED_SPREAD <= Decimal("0.50")):
            raise ValueError(f"MAX_ALLOWED_SPREAD={self.MAX_ALLOWED_SPREAD} out of safe range [0.01, 0.50]")
        if not (Decimal("0.10") <= self.MAX_ENTRY_PRICE_YES <= Decimal("0.60")):
            raise ValueError(f"MAX_ENTRY_PRICE_YES={self.MAX_ENTRY_PRICE_YES} out of safe range [0.10, 0.60]")
        if not (Decimal("0.10") <= self.MAX_ENTRY_PRICE_NO <= Decimal("0.85")):
            raise ValueError(f"MAX_ENTRY_PRICE_NO={self.MAX_ENTRY_PRICE_NO} out of safe range [0.10, 0.85]")
        if not (Decimal("0.0001") <= self.INDEX_LAG_MIN_DIVERGENCE <= Decimal("0.05")):
            raise ValueError(f"INDEX_LAG_MIN_DIVERGENCE={self.INDEX_LAG_MIN_DIVERGENCE} out of safe range [0.0001, 0.05]")
        if not (Decimal("1.5") <= self.OFI_BUY_SELL_RATIO <= Decimal("20.0")):
            raise ValueError(f"OFI_BUY_SELL_RATIO={self.OFI_BUY_SELL_RATIO} out of safe range [1.5, 20.0]")
        if not (Decimal("1000.0") <= self.OFI_MIN_VOLUME_NOTIONAL <= Decimal("10000000.0")):
            raise ValueError(f"OFI_MIN_VOLUME_NOTIONAL={self.OFI_MIN_VOLUME_NOTIONAL} out of safe range [1000, 10000000]")
        for asset, floor_val in self.STD_DEV_FLOORS_PCT.items():
            if not (0.0 <= floor_val <= 1.0):
                raise ValueError(f"STD_DEV_FLOORS_PCT[{asset}]={floor_val} out of safe range [0.0, 1.0]")

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
# SEC-05: Strip ANSI escape sequences to prevent terminal manipulation via crafted log data
_ANSI_ESCAPE_RE = re.compile(r'\x1b\[[0-9;]*[a-zA-Z]')
