"""
Kalshi Quantitative Trading Engine (KQTE) - Strategy Engine

This module runs the main trading engine and executes three strategies:
1. Binance Liquidation Sniper (Strategy 1)
2. CF Benchmarks Index Lag Arbitrage (Strategy 4)
3. Taker Order Flow Imbalance (Strategy 5)

It also manages WebSocket data feeds from Coinbase, Binance, Bybit,
Hyperliquid, and Kalshi, and handles Take-Profit exit scaling.
"""

import os
import sys
import time
import math
import base64
import asyncio
import logging
import urllib.parse
import aiohttp
import orjson
import websockets
import uuid
import ctypes
import atexit
import datetime
import tempfile
import random
import gc
from decimal import Decimal, InvalidOperation, ROUND_UP
from typing import Dict, Optional, Tuple, List, Set, Any

import boto3
from botocore.exceptions import ClientError
from cryptography.hazmat.primitives.serialization import load_pem_private_key
import kalshi_bot

from engine.config import config, BotConfig, GLOBAL_SSL_CONTEXT
from engine.models import (
    AssetState, PerformanceTracker, validate_tick_data, validate_binance_payload,
    safe_decimal, safe_int, EconomicEvent
)
from engine.security import (
    sanitize_log_str, safe_drain_queue, SafeResolver, is_safe_destination_async,
    MacroCircuitBreaker, calculate_backoff_delay
)
from engine.broker import ExecutionBroker, LiveKalshiBroker, _extract_fill_price

logger = logging.getLogger("KalshiQuantEngine")

# ==========================================
# QUANTITATIVE PRICING ENGINE
# ==========================================
class LiveTradingEngine:
    def __init__(self, broker: ExecutionBroker):
        self.broker = broker
        self.starting_balance: Decimal = Decimal("0.00")
        self.peak_balance: Decimal = Decimal("0.00")  # Rolling high-water mark for drawdown calculation
        self.available_balance: Decimal = Decimal("0.00")
        self.capital_in_flight: Decimal = Decimal("0.00") 
        self.consecutive_api_failures: int = 0
        self.last_sync_time: float = 0.0
        self.last_telemetry_log_time: float = 0.0  
        self.state_sequence: int = 0
        self.macro_trend: Dict[str, str] = {}
        self.last_blocked_log_time: Dict[str, float] = {}
        self._last_drop_log_time: float = 0.0
        
        self.active_trade_count: int = 0
        self.execution_in_flight: Set[str] = set()
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
                self.peak_balance = self.starting_balance
                logger.debug(f"[RISK MANAGER] Bound to absolute starting balance: ${self.starting_balance:.2f} (peak initialized)")
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

            # Inline Peak-to-Trough Drawdown check (Sub-millisecond latency gate)
            est_portfolio_val = self.available_balance + self.capital_in_flight
            if est_portfolio_val > self.peak_balance:
                self.peak_balance = est_portfolio_val
            
            if self.peak_balance > 0:
                drawdown = (self.peak_balance - est_portfolio_val) / self.peak_balance
                if drawdown >= config.DRAWDOWN_LIMIT_PCT:
                    logger.critical(
                        f"[INLINE RISK MONITOR] DRAWDOWN LIMIT REACHED ({drawdown*100:.1f}% from peak ${self.peak_balance:.2f} → est. portfolio value ${est_portfolio_val:.2f}). Halting operations."
                    )
                    self.shutting_down = True

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

    async def _safe_shield(self, coro):
        task = asyncio.create_task(coro)
        self._pending_tasks.add(task)
        task.add_done_callback(self._handle_task_done)
        return await asyncio.shield(task)

    async def _decrement_trade_cap(self, contract_id: Optional[str] = None):
        async with self.trade_cap_lock:
            self.active_trade_count = max(0, self.active_trade_count - 1)
            if contract_id:
                self.execution_in_flight.discard(contract_id)

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

    def purge_memory(self, queue: Optional[asyncio.Queue] = None, target_symbol: Optional[str] = None):
        """SEC-25: Purges stale orderbook depth and sets temporary cooldown on affected asset during backlog surges."""
        logger.warning(f"Purging queue memory backlog and resetting orderbook depth (Target: {target_symbol or 'ALL'}).")
        current_time = time.time()
        symbols_to_purge = [target_symbol] if target_symbol and target_symbol in self.assets else list(self.assets.keys())
        for symbol in symbols_to_purge:
            state = self.assets[symbol]
            state.cooldown_until = current_time + 15.0
            if hasattr(state, "bids") and isinstance(state.bids, dict):
                state.bids.clear()
            if hasattr(state, "asks") and isinstance(state.asks, dict):
                state.asks.clear()
        if queue:
            safe_drain_queue(queue)
        gc.collect()

    def _validate_orderbook_entry(self, asset_symbol: str, state: AssetState, trade_side: str, best_vals: Tuple[Decimal, Decimal, int, int], signal_tag: Optional[str] = None, is_mean_reversion_post: bool = False) -> Optional[Decimal]:
        best_bid, best_ask, bid_depth, ask_depth = best_vals
        if best_ask.is_nan() or best_bid.is_nan():
            if signal_tag is None:
                logger.warning(f"[{asset_symbol}] Orderbook rejected: NaN price detected in quotes (Bid: {best_bid}, Ask: {best_ask}).")
            return None
        
        spread = best_ask - best_bid
        max_spread = min(config.MAX_ALLOWED_SPREAD, max(Decimal("0.05"), best_bid * Decimal("0.30")))
        min_entry_price = config.MIN_ENTRY_PRICE_YES if trade_side == "YES" else config.MIN_ENTRY_PRICE_NO
        max_entry_price = config.MAX_ENTRY_PRICE_YES if trade_side == "YES" else config.MAX_ENTRY_PRICE_NO

        rejection_reason = None
        if best_ask > max_entry_price:
            rejection_reason = f"Ask ${best_ask:.2f} exceeds {trade_side} MAX_ENTRY_PRICE (${max_entry_price:.2f})"
        elif best_ask < min_entry_price:
            rejection_reason = f"Ask ${best_ask:.2f} is below {trade_side} MIN_ENTRY_PRICE (${min_entry_price:.2f})"
        elif best_bid < Decimal("0.01"):
            rejection_reason = f"Bid ${best_bid:.2f} is below minimum bid floor ($0.01)"
        elif best_ask < best_bid:
            rejection_reason = f"Crossed orderbook detected (Bid ${best_bid:.4f} > Ask ${best_ask:.4f})"
        elif spread > max_spread:
            rejection_reason = f"Spread ${spread:.4f} exceeds max_spread (${max_spread:.4f})"

        if rejection_reason:
            if signal_tag:
                logger.warning(f"[{asset_symbol}] [{signal_tag}] Orderbook rejected: {rejection_reason} (Bid: ${best_bid:.4f}, Ask: ${best_ask:.4f}).")
            else:
                logger.warning(f"[{asset_symbol}] Orderbook rejected: {rejection_reason} (Bid: ${best_bid:.4f}, Ask: ${best_ask:.4f}).")
            return None

        limit_price = max(Decimal("0.01"), min(Decimal("0.99"), best_ask))

        if state.strike_price and state.last_price:
            spot_price = Decimal(str(state.last_price))
            strike_price = Decimal(str(state.strike_price))
            mean, upper, lower = state.fast_indicators.get_bollinger_bands()
            std_dev = (upper - mean) / 2.0
            floor_pct = config.STD_DEV_FLOORS_PCT.get(asset_symbol, 0.0005)
            floor = floor_pct * float(state.last_price)
            std_dev_dec = Decimal(str(max(std_dev, floor)))

            if trade_side == "YES":
                if spot_price >= strike_price:
                    logger.info(f"[{asset_symbol}] Drop: YES trade blocked. ITM entry (Spot ${spot_price} >= Strike ${strike_price}).")
                    return None
                distance = strike_price - spot_price
                if not is_mean_reversion_post and distance > Decimal("0.8") * std_dev_dec:
                    if signal_tag:
                        logger.info(f"[{asset_symbol}] [{signal_tag}] Drop: YES trade blocked. Spot ${spot_price} is too far below Strike ${strike_price}.")
                    else:
                        logger.info(f"[{asset_symbol}] Drop: YES trade blocked. Spot ${spot_price} is too far below Strike ${strike_price} (Dist: {distance:.2f} > 0.8 * StdDev: {Decimal('0.8') * std_dev_dec:.2f}).")
                    return None
            elif trade_side == "NO":
                if spot_price <= strike_price:
                    logger.info(f"[{asset_symbol}] Drop: NO trade blocked. ITM entry (Spot ${spot_price} <= Strike ${strike_price}).")
                    return None
                distance = spot_price - strike_price
                if not is_mean_reversion_post and distance > Decimal("0.8") * std_dev_dec:
                    if signal_tag:
                        logger.info(f"[{asset_symbol}] [{signal_tag}] Drop: NO trade blocked. Spot ${spot_price} is too far above Strike ${strike_price}.")
                    else:
                        logger.info(f"[{asset_symbol}] Drop: NO trade blocked. Spot ${spot_price} is too far above Strike ${strike_price} (Dist: {distance:.2f} > 0.8 * StdDev: {Decimal('0.8') * std_dev_dec:.2f}).")
                    return None

        return limit_price

    async def _acquire_execution_slot(self, state: AssetState, executing_contract_id: str, trade_side: str, limit_price: Decimal, current_time: float) -> Optional[Tuple[int, Decimal, bool]]:
        async with self.balance_lock:
            if executing_contract_id == getattr(state, "last_traded_event", ""):
                return None
            current_pos_side = state.position_sides.get(executing_contract_id)
            if current_pos_side and current_pos_side != trade_side:
                return None
            
            actual_pos_size = state.positions.get(executing_contract_id, 0)
            remaining_exposure = config.MAX_EXPOSURE_PER_EVENT - actual_pos_size
            if remaining_exposure <= 0:
                return None
            
            trade_budget = self.available_balance * config.TRADE_BUDGET_PCT
            raw_quantity = int(trade_budget / limit_price)
            quantity = min(raw_quantity, config.MAX_CONTRACTS_PER_TRADE, remaining_exposure)
            
            if quantity < 30:
                return None
            
            total_cost = Decimal(quantity) * limit_price
            if self.available_balance >= total_cost:
                self.available_balance -= total_cost
                self.capital_in_flight += total_cost
                
                state.position_sides[executing_contract_id] = trade_side
                state.positions[executing_contract_id] = actual_pos_size + quantity
                state.last_traded_event = executing_contract_id
                state.cooldown_until = current_time + 15.0
                self.state_sequence += 1
                return quantity, total_cost, True
            else:
                return None

    async def sync_balance_loop(self):
        try:
            self.capital_in_flight = await self.broker.get_locked_capital()
        except Exception as e: 
            logger.warning("Startup reconciliation failed to fetch locked capital", exc_info=True)

        backoff = 60
        while not self.shutting_down:
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
                        
                        if portfolio_val > self.peak_balance:
                            self.peak_balance = portfolio_val
                        
                        if self.peak_balance > 0:
                            drawdown = (self.peak_balance - portfolio_val) / self.peak_balance
                            if drawdown >= config.DRAWDOWN_LIMIT_PCT:
                                logger.critical(
                                    f"DRAWDOWN LIMIT REACHED ({drawdown*100:.1f}% from peak ${self.peak_balance:.2f} → ${portfolio_val:.2f}). Halting operations."
                                )
                                self.shutting_down = True
                
                if state_mutated:
                    logger.warning("State mutated during fetch; discarding sync data to prevent race overwrite.")
                    await asyncio.sleep(1.0)
                    continue
                
                current_time = time.time()
                if current_time - self.last_telemetry_log_time >= config.TELEMETRY_LOG_INTERVAL_SEC:
                    self.last_telemetry_log_time = current_time
                    
                    asset_status_summaries = []
                    for symbol, state in self.assets.items():
                        if state.last_price is not None:
                            mean, upper, lower = state.fast_indicators.get_bollinger_bands()
                            std_dev = (upper - mean) / 2.0
                            
                            floor_pct = config.STD_DEV_FLOORS_PCT.get(symbol, 0.0005)
                            floor = floor_pct * float(state.last_price)
                            if std_dev >= floor:
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
        attempt = 0
        while not self.shutting_down:
            try:
                for symbol, state in self.assets.items():
                    last_price = getattr(state, 'last_price', None)
                    if not last_price: continue
                        
                    contract_id, strike, exp_time = await self.broker.get_active_market(symbol, last_price)
                    if contract_id:
                        if state.active_contract_id != contract_id:
                            logger.debug(f"[MARKET ROUTER] {symbol} Locked onto valid contract: {contract_id}")
                            state.active_contract_id = contract_id
                        if strike > 0.0:
                            state.strike_price = strike
                        if exp_time > 0.0:
                            state.expiration_time = exp_time 
                attempt = 0
                await asyncio.sleep(30)
            except Exception as e:
                attempt += 1
                delay = min(60.0, calculate_backoff_delay(attempt, base=2.0))
                logger.error(f"[MARKET ROUTER] Error in sync_markets_loop: {type(e).__name__} - {sanitize_log_str(str(e))}. Retrying in {delay:.2f}s...")
                await asyncio.sleep(delay)

    async def sync_macro_calendar_loop(self):
        attempt = 0
        while not self.shutting_down:
            if os.environ.get("BOT_ENV", "simulation").lower() == "simulation":
                await asyncio.sleep(3600)
                continue
                
            success = False
            try:
                session = getattr(self.broker, "session", None)
                if not session or session.closed:
                    resolver = SafeResolver()
                    connector = aiohttp.TCPConnector(ssl=GLOBAL_SSL_CONTEXT, resolver=resolver, ttl_dns_cache=300)
                    async with aiohttp.ClientSession(connector=connector) as temp_session:
                        success = await self.circuit_breaker.fetch_calendar(temp_session)
                else:
                    success = await self.circuit_breaker.fetch_calendar(session)

                if success:
                    logger.debug(f"[CIRCUIT BREAKER] Economic calendar synchronized.")
                    attempt = 0
                else:
                    logger.warning("[CIRCUIT BREAKER] Calendar synchronization returned no updates or failed validation.")
            except Exception as e:
                logger.error(f"[CIRCUIT BREAKER] Critical calendar update routine failure: {type(e).__name__}")
                
            if success:
                await asyncio.sleep(21600)
            else:
                attempt += 1
                delay = min(300.0, calculate_backoff_delay(attempt, base=5.0))
                logger.warning(f"[CIRCUIT BREAKER] Calendar sync failed. Retrying in {delay:.2f}s...")
                await asyncio.sleep(delay)

    async def sync_macro_trend_loop(self):
        """Fetches the 3-hour trend for major assets using Coinbase REST API."""
        attempt = 0
        while not self.shutting_down:
            try:
                resolver = SafeResolver()
                connector = aiohttp.TCPConnector(ssl=GLOBAL_SSL_CONTEXT, resolver=resolver, ttl_dns_cache=300)
                async with aiohttp.ClientSession(connector=connector) as session:
                    while not self.shutting_down:
                        try:
                            for asset in ["BTC-USD", "SOL-USD", "ETH-USD", "DOGE-USD"]:
                                try:
                                    url = f"https://api.exchange.coinbase.com/products/{asset}/candles?granularity=3600"
                                    if not await is_safe_destination_async(url): continue
                                    
                                    headers = {"User-Agent": "KalshiQuantEngine/1.0", "Accept": "application/json"}
                                    timeout = aiohttp.ClientTimeout(total=5.0)
                                    async with session.get(url, headers=headers, timeout=timeout, allow_redirects=False) as response:
                                        if response.status == 200:
                                            body_bytes = await response.content.read(524288)
                                            if not body_bytes: continue
                                            data = orjson.loads(body_bytes)
                                            if len(data) >= 3:
                                                current_close = float(data[0][4])
                                                old_open = float(data[2][3])
                                                if current_close > old_open * 1.002:
                                                    self.macro_trend[asset] = "UP"
                                                elif current_close < old_open * 0.998:
                                                    self.macro_trend[asset] = "DOWN"
                                                else:
                                                    self.macro_trend[asset] = "FLAT"
                                except Exception as e:
                                    logger.debug(f"[{asset}] Macro sync internal error: {type(e).__name__}")
                            
                            try:
                                url = "https://fapi.binance.com/fapi/v1/klines?symbol=HYPEUSDT&interval=1h&limit=3"
                                if await is_safe_destination_async(url):
                                    headers = {"User-Agent": "KalshiQuantEngine/1.0", "Accept": "application/json"}
                                    timeout = aiohttp.ClientTimeout(total=5.0)
                                    async with session.get(url, headers=headers, timeout=timeout, allow_redirects=False) as response:
                                        if response.status == 200:
                                            body_bytes = await response.content.read(524288)
                                            if not body_bytes: pass
                                            else:
                                                data = orjson.loads(body_bytes)
                                                if len(data) >= 3:
                                                    current_close = float(data[-1][4])
                                                    old_open = float(data[0][1])
                                                    if current_close > old_open * 1.002:
                                                        self.macro_trend["HYPE-USD"] = "UP"
                                                    elif current_close < old_open * 0.998:
                                                        self.macro_trend["HYPE-USD"] = "DOWN"
                                                    else:
                                                        self.macro_trend["HYPE-USD"] = "FLAT"
                            except Exception as e:
                                logger.debug(f"[HYPE-USD] Macro sync internal error: {type(e).__name__}")
                            
                            attempt = 0
                            await asyncio.sleep(1800)
                        except Exception as inner_e:
                            logger.error(f"Macro trend loop internal pass error: {type(inner_e).__name__}")
                            attempt += 1
                            delay = min(300.0, calculate_backoff_delay(attempt, base=10.0))
                            await asyncio.sleep(delay)
            except Exception as outer_e:
                logger.error(f"Macro trend session management failure: {type(outer_e).__name__}")
                attempt += 1
                delay = min(300.0, calculate_backoff_delay(attempt, base=10.0))
                await asyncio.sleep(delay)

    async def _credit_settlement_payout(self, contract_id: str, payout: Decimal, label: str = ""):
        """Thread-safe settlement balance credit helper for paper and simulation brokers (CRIT-02 & ARCH-04)."""
        async with self.balance_lock:
            if hasattr(self.broker, 'simulated_balance'):
                self.broker.simulated_balance += payout
                logger.info(f"[{contract_id}] 💸 PAPER PAYOUT {label}: Added ${payout:.2f} to simulated balance.")
            elif getattr(self.broker, 'paper_trade', False) and hasattr(self.broker, '_paper_balance'):
                async with getattr(self.broker, 'paper_orders_lock', asyncio.Lock()):
                    self.broker._paper_balance += payout
                logger.info(f"[{contract_id}] 💸 PAPER PAYOUT {label}: Added ${payout:.2f} to paper balance.")
            await self._safe_shield(self._update_local_state(Decimal("0.00"), Decimal("0.00")))

    async def _monitor_take_profit(self, state: AssetState, contract_id: str, side: str, entry_price: Decimal, quantity: int, seconds_left: float, hold_to_settle: bool = False, strike_price: Optional[float] = None):
        """Asynchronous O(1) Background Task: Laddered Take-Profit with adaptive pricing."""
        if quantity <= 0: return
        if state.expiration_time > 0.0:
            seconds_left = max(0.0, state.expiration_time - time.time())
        target_strike = strike_price if strike_price is not None else getattr(state, 'strike_price', 0.0)
        total_filled = 0
        total_proceeds = Decimal("0.00")
        
        order_ids = []
        completed_orders = set()
        last_reported_fill = {}
        accumulated_fills = {}
        
        try:
            if not hold_to_settle:
                min_tp = config.MIN_TP_ROI
                max_tp = config.MAX_TP_ROI
                clamped_seconds = max(180.0, min(600.0, float(seconds_left)))
                dynamic_multiplier = min_tp + Decimal(str((clamped_seconds - 180.0) / 420.0)) * (max_tp - min_tp)
                
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
                
                t1_qty = max(1, round(quantity * 0.40))
                raw_t1 = (entry_price * Decimal("1.50")).quantize(Decimal("0.01"), rounding=ROUND_UP)
                t1_price = min(Decimal("0.99"), max_realistic_tp, raw_t1)
                
                t2_qty = max(0, round(quantity * 0.35))
                if t1_qty + t2_qty > quantity:
                    t2_qty = quantity - t1_qty
                raw_t2 = (entry_price * dynamic_multiplier).quantize(Decimal("0.01"), rounding=ROUND_UP)
                t2_price = min(Decimal("0.99"), max(t1_price + Decimal("0.01"), raw_t2))
                
                t3_qty = max(0, quantity - t1_qty - t2_qty)
                raw_t3 = (entry_price * Decimal("1.95")).quantize(Decimal("0.01"), rounding=ROUND_UP)
                t3_price = min(Decimal("0.99"), max(t2_price + Decimal("0.01"), raw_t3))
                
                await asyncio.sleep(0.5)
                
                tranches = []
                if t1_qty > 0:
                    tranches.append((t1_qty, t1_price, "t1"))
                if t2_qty > 0:
                    tranches.append((t2_qty, t2_price, "t2"))
                if t3_qty > 0:
                    tranches.append((t3_qty, t3_price, "t3"))

                capped_count = sum(1 for _, tp, _ in tranches if tp == Decimal("0.99"))
                if len(tranches) > 0 and capped_count >= 2:
                    logger.info(f"[{contract_id}] {capped_count}/3 TP tranches capped at $0.99. Bypassing TP routing and falling back to Hold-to-Settle to minimize fees.")
                    hold_to_settle = True

            if hold_to_settle:
                logger.info(f"[{contract_id}] 💎 Theta Harvester active. Holding {quantity} contracts to settlement to avoid maker/taker fees.")
            
                sleep_time = max(0.0, state.expiration_time - time.time() - 2.0) if state.expiration_time else max(0.0, seconds_left - 2.0)
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)
                    
                tp_check = await self.broker.get_best_bid_ask(contract_id, side)
                won = False
                net_pnl = Decimal("0.00") - (Decimal(quantity) * entry_price)
                if tp_check:
                    final_bid, _, _, _ = tp_check
                    if final_bid >= Decimal("0.90"):
                        logger.info(f"[{contract_id}] 🏆 THE BUZZER: Final '{side.upper()}' bid is ${final_bid:.2f}. WIN HIGHLY PROBABLE! Settlement pending.")
                        payout = Decimal(quantity) * Decimal("1.00")
                        net_pnl = payout - (Decimal(quantity) * entry_price)
                        won = True
                        if hasattr(self.broker, 'simulated_balance'):
                            self.broker.simulated_balance += payout
                            logger.info(f"[{contract_id}] 💸 PAPER PAYOUT: Added ${payout:.2f} to simulated balance.")
                            await self._safe_shield(self._update_local_state(Decimal("0.00"), Decimal("0.00")))
                        elif getattr(self.broker, 'paper_trade', False) and hasattr(self.broker, '_paper_balance'):
                            self.broker._paper_balance += payout
                            logger.info(f"[{contract_id}] 💸 PAPER PAYOUT: Added ${payout:.2f} to paper balance.")
                            await self._safe_shield(self._update_local_state(Decimal("0.00"), Decimal("0.00")))
                    elif final_bid <= Decimal("0.10"):
                        logger.warning(f"[{contract_id}] 💀 THE BUZZER: Final '{side.upper()}' bid is ${final_bid:.2f}. LOSS HIGHLY PROBABLE. Settlement pending.")
                        won = False
                    else:
                        logger.info(f"[{contract_id}] ⚖️ THE BUZZER: Final '{side.upper()}' bid is ${final_bid:.2f}. TOSS UP. Settlement pending.")
                        payout = Decimal(quantity) * final_bid
                        net_pnl = payout - (Decimal(quantity) * entry_price)
                        won = net_pnl > 0
                        if hasattr(self.broker, 'simulated_balance'):
                            self.broker.simulated_balance += payout
                            logger.info(f"[{contract_id}] 💸 PAPER PAYOUT (TOSS UP): Added ${payout:.2f} to simulated balance.")
                            await self._safe_shield(self._update_local_state(Decimal("0.00"), Decimal("0.00")))
                        elif getattr(self.broker, 'paper_trade', False) and hasattr(self.broker, '_paper_balance'):
                            self.broker._paper_balance += payout
                            logger.info(f"[{contract_id}] 💸 PAPER PAYOUT (TOSS UP): Added ${payout:.2f} to paper balance.")
                            await self._safe_shield(self._update_local_state(Decimal("0.00"), Decimal("0.00")))
                else:
                    logger.info(f"[{contract_id}] Event expired. Orderbook closed. Background settlement watch complete.")
                    settled_won = None
                    if state.last_price is not None and target_strike > 0.0:
                        try:
                            last_price_dec = Decimal(str(state.last_price))
                            strike_price_dec = Decimal(str(target_strike))
                            if side.upper() == "YES":
                                settled_won = last_price_dec > strike_price_dec
                            else:
                                settled_won = last_price_dec <= strike_price_dec
                        except Exception as ex_settle:
                            logger.error(f"[{contract_id}] Fallback settlement calculation error: {ex_settle}")
                            
                    if settled_won is not None:
                        if settled_won:
                            logger.info(f"[{contract_id}] 🏆 SETTLED WIN: Price ${state.last_price} vs Strike ${target_strike} for '{side.upper()}'.")
                            payout = Decimal(quantity) * Decimal("1.00")
                            net_pnl = payout - (Decimal(quantity) * entry_price)
                            won = True
                            if hasattr(self.broker, 'simulated_balance'):
                                self.broker.simulated_balance += payout
                                logger.info(f"[{contract_id}] 💸 PAPER PAYOUT (SETTLED WIN): Added ${payout:.2f} to simulated balance.")
                                await self._safe_shield(self._update_local_state(Decimal("0.00"), Decimal("0.00")))
                            elif getattr(self.broker, 'paper_trade', False) and hasattr(self.broker, '_paper_balance'):
                                self.broker._paper_balance += payout
                                logger.info(f"[{contract_id}] 💸 PAPER PAYOUT (SETTLED WIN): Added ${payout:.2f} to paper balance.")
                                await self._safe_shield(self._update_local_state(Decimal("0.00"), Decimal("0.00")))
                        else:
                            logger.warning(f"[{contract_id}] 💀 SETTLED LOSS: Price ${state.last_price} vs Strike ${target_strike} for '{side.upper()}'.")
                            won = False
                            payout = Decimal("0.00")
                            net_pnl = Decimal("0.00") - (Decimal(quantity) * entry_price)
                    else:
                        if getattr(self.broker, 'paper_trade', False) and entry_price < Decimal("0.50"):
                            payout = Decimal(quantity) * Decimal("0.50")
                            net_pnl = payout - (Decimal(quantity) * entry_price)
                            won = net_pnl > 0
                            if hasattr(self.broker, 'simulated_balance'):
                                self.broker.simulated_balance += payout
                                await self._safe_shield(self._update_local_state(Decimal("0.00"), Decimal("0.00")))
                            elif hasattr(self.broker, '_paper_balance'):
                                self.broker._paper_balance += payout
                                await self._safe_shield(self._update_local_state(Decimal("0.00"), Decimal("0.00")))
                
                current_hour = int(datetime.datetime.now(datetime.timezone.utc).hour)
                self.performance_tracker.record(contract_id, current_hour, won, float(net_pnl))
                return
            
            try:
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
                        self.active_tp_orders[tp_order_id] = asyncio.Queue()
                        if tp_order_id in self.orphan_fills:
                            for fill_msg in self.orphan_fills.pop(tp_order_id):
                                self.active_tp_orders[tp_order_id].put_nowait(fill_msg)
                        order_ids.append((tp_order_id, tq, tp, label))
                    else:
                        logger.warning(f"[{contract_id}] Failed to route {label} TP order.")
                
                if not order_ids:
                    logger.warning(f"[{contract_id}] All TP tranches failed. Holding to expiration.")
                    current_hour = int(datetime.datetime.now(datetime.timezone.utc).hour)
                    total_cost = Decimal(quantity) * entry_price
                    self.performance_tracker.record(contract_id, current_hour, False, float(-total_cost))
                    return
                
                poll_interval = 5.0
                elapsed = 0.0
                timeout = max(0.0, seconds_left - 20.0)
                
                for oid, _, _, _ in order_ids:
                    last_reported_fill[oid] = 0
                    accumulated_fills[oid] = 0
                start_time = time.time()
                
                while time.time() - start_time < timeout and len(completed_orders) < len(order_ids):
                    active_oids = [oid for oid, _, _, _ in order_ids if oid not in completed_orders]
                    if not active_oids:
                        break
                    
                    pending_tasks = {
                        asyncio.create_task(self.active_tp_orders[oid].get()): oid
                        for oid in active_oids
                    }
                    
                    time_remaining = timeout - (time.time() - start_time)
                    if time_remaining <= 0:
                        for task in pending_tasks.keys():
                            task.cancel()
                        break
                    
                    wait_timeout = min(10.0, time_remaining)
                    try:
                        done, pending = await asyncio.wait(
                            pending_tasks.keys(),
                            timeout=wait_timeout,
                            return_when=asyncio.FIRST_COMPLETED
                        )
                    finally:
                        for task in pending_tasks.keys():
                            if not task.done():
                                task.cancel()
                    
                    if done:
                        for task in done:
                            oid = pending_tasks[task]
                            try:
                                fill_data = task.result()
                                oqty, oprice, olabel = next(
                                    (q, p, l) for i, q, p, l in order_ids if i == oid
                                )
                                raw_fills = int(fill_data.get("count", 0))
                                accumulated_fills[oid] = accumulated_fills.get(oid, 0) + raw_fills
                                new_fills = min(accumulated_fills[oid] - last_reported_fill.get(oid, 0), oqty - last_reported_fill.get(oid, 0))
                                tp_status = fill_data.get("status", "unknown")
                                
                                if new_fills > 0:
                                    total_filled += new_fills
                                    total_proceeds += Decimal(new_fills) * oprice
                                    await self._safe_shield(self._update_local_state(Decimal(new_fills) * oprice, Decimal("0.00"), state, contract_id, -new_fills))
                                    last_reported_fill[oid] = last_reported_fill.get(oid, 0) + new_fills
                                
                                if last_reported_fill[oid] >= oqty or tp_status == "executed":
                                    logger.warning(f"[{contract_id}] 🎯 {olabel} TP HIT (WS)! Sold {last_reported_fill[oid]}x @ ${oprice:.2f}")
                                    completed_orders.add(oid)
                                elif tp_status in ["canceled", "cancelled"]:
                                    completed_orders.add(oid)
                            except Exception as ex:
                                logger.debug(f"[{contract_id}] Error reading WS fill event: {ex}")
                    else:
                        logger.debug(f"[{contract_id}] WS quiet for {wait_timeout:.1f}s. Running fallback REST poll.")
                        for oid, oqty, oprice, olabel in order_ids:
                            if oid in completed_orders:
                                continue
                            try:
                                tp_details = await self.broker.get_order_details(oid, simulate=False)
                                tp_status = tp_details.get("status", "unknown")
                                tp_filled_qty = self._get_filled_qty_from_details(tp_details, oqty)
                                
                                accumulated_fills[oid] = max(accumulated_fills.get(oid, 0), tp_filled_qty)
                                new_fills = min(accumulated_fills[oid] - last_reported_fill.get(oid, 0), oqty - last_reported_fill.get(oid, 0))
                                
                                if new_fills > 0:
                                    total_filled += new_fills
                                    total_proceeds += Decimal(new_fills) * oprice
                                    await self._safe_shield(self._update_local_state(Decimal(new_fills) * oprice, Decimal("0.00"), state, contract_id, -new_fills))
                                    last_reported_fill[oid] = last_reported_fill.get(oid, 0) + new_fills
                                
                                if tp_filled_qty >= oqty or tp_status == "executed":
                                    logger.warning(f"[{contract_id}] 🎯 {olabel} TP HIT (Fallback REST)! Sold {tp_filled_qty}x @ ${oprice:.2f}")
                                    completed_orders.add(oid)
                                elif tp_status in ["canceled", "cancelled"]:
                                    completed_orders.add(oid)
                            except Exception as e:
                                logger.debug(f"[{contract_id}] Fallback REST poll error: {e}")
            finally:
                for oid, _, _, _ in order_ids:
                    self.active_tp_orders.pop(oid, None)
            
            logger.info(f"[{contract_id}] Take-Profit lifecycle ending. Cancelling remaining orders...")
            
            async def cancel_and_reconcile_single(oid, oqty, oprice, olabel):
                nonlocal total_filled, total_proceeds
                if oid in completed_orders:
                    return
                try:
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
                
                try:
                    for attempt in range(5):
                        try:
                            details = await self.broker.get_order_details(oid, simulate=False)
                            status = details.get("status", "unknown")
                            if status in ["canceled", "cancelled", "executed"]:
                                filled = self._get_filled_qty_from_details(details, oqty)
                                accumulated_fills[oid] = max(accumulated_fills.get(oid, 0), filled)
                                new_fills = accumulated_fills[oid] - last_reported_fill.get(oid, 0)
                                if new_fills > 0:
                                    total_filled += new_fills
                                    total_proceeds += Decimal(new_fills) * oprice
                                    await self._safe_shield(self._update_local_state(Decimal(new_fills) * oprice, Decimal("0.00"), state, contract_id, -new_fills))
                                    last_reported_fill[oid] = accumulated_fills[oid]
                                    logger.info(f"[{contract_id}] {olabel} partial fill at buzzer: {filled}/{oqty}")
                                break
                        except Exception as ex:
                            if attempt == 4:
                                logger.warning(f"[{contract_id}] Failed to retrieve final {olabel} TP details: {ex}")
                        backoff_delay = (1.5 ** attempt) + random.uniform(0.1, 0.5)
                        await asyncio.sleep(backoff_delay)
                except Exception as ve:
                    logger.warning(f"[{contract_id}] Error verifying final details for {olabel} TP: {ve}")

            reconcile_tasks = [
                cancel_and_reconcile_single(oid, oqty, oprice, olabel)
                for oid, oqty, oprice, olabel in order_ids
            ]
            if reconcile_tasks:
                await asyncio.gather(*reconcile_tasks)
            
            current_hour = int(datetime.datetime.now(datetime.timezone.utc).hour)
            total_cost = Decimal(quantity) * entry_price
            
            unfilled_at_settlement = quantity - total_filled
            settlement_proceeds = Decimal("0.00")
            if unfilled_at_settlement > 0:
                tp_check = await self.broker.get_best_bid_ask(contract_id, side)
                settled_won = None
                if tp_check:
                    final_bid, _, _, _ = tp_check
                    if final_bid >= Decimal("0.90"):
                        settled_won = True
                    elif final_bid <= Decimal("0.10"):
                        settled_won = False
                    else:
                        settlement_proceeds = Decimal(str(unfilled_at_settlement)) * final_bid
                else:
                    if state.last_price is not None and target_strike > 0.0:
                        try:
                            last_price_dec = Decimal(str(state.last_price))
                            strike_price_dec = Decimal(str(target_strike))
                            if side.upper() == "YES":
                                settled_won = last_price_dec > strike_price_dec
                            else:
                                settled_won = last_price_dec <= strike_price_dec
                        except Exception as e:
                            logger.warning(f"[{contract_id}] Error calculating target strike settlement: {e}")
                
                if settled_won is True:
                    settlement_proceeds = Decimal(str(unfilled_at_settlement)) * Decimal("1.00")
                    logger.info(f"[{contract_id}] 🏆 SETTLED WIN (Post-TP): {unfilled_at_settlement}x '{side.upper()}' settled ITM.")
                elif settled_won is False:
                    logger.warning(f"[{contract_id}] 💀 SETTLED LOSS (Post-TP): {unfilled_at_settlement}x '{side.upper()}' settled OTM.")
                
                if settlement_proceeds > 0:
                    total_proceeds += settlement_proceeds
                    if hasattr(self.broker, 'simulated_balance'):
                        self.broker.simulated_balance += settlement_proceeds
                        await self._safe_shield(self._update_local_state(settlement_proceeds, Decimal("0.00")))
                    elif getattr(self.broker, 'paper_trade', False) and hasattr(self.broker, '_paper_balance'):
                        self.broker._paper_balance += settlement_proceeds
                        await self._safe_shield(self._update_local_state(settlement_proceeds, Decimal("0.00")))

            net_pnl = total_proceeds - total_cost
            won = net_pnl > 0
            
            self.performance_tracker.record(contract_id, current_hour, won, float(net_pnl))

            if total_filled > 0:
                logger.warning(f"[{contract_id}] 🎯 TOTAL TP FILLED: {total_filled}/{quantity} across all tranches. Net P&L: ${net_pnl:.2f}")
            else:
                logger.info(f"[{contract_id}] No TP fills across any tranche. Held to expiration. Net P&L: ${net_pnl:.2f}")
                
        except Exception as e:
            logger.error(f"[{contract_id}] Unhandled error in Take Profit monitor.", exc_info=True)
        finally:
            async def run_cleanup():
                nonlocal total_filled, total_proceeds
                async def cancel_and_reconcile_cleanup(oid, oqty, oprice, olabel):
                    if oid in completed_orders:
                        return
                    try:
                        logger.warning(f"[{contract_id}] Take-Profit task interrupted or ended with dangling orders. Cancelling {oid} ({olabel}) on exchange.")
                        await self.broker.cancel_order(oid)
                        try:
                            details = await self.broker.get_order_details(oid, simulate=False)
                            filled = self._get_filled_qty_from_details(details, oqty)
                            accumulated_fills[oid] = max(accumulated_fills.get(oid, 0), filled)
                            new_fills = accumulated_fills[oid] - last_reported_fill.get(oid, 0)
                            if new_fills > 0:
                                total_filled += new_fills
                                total_proceeds += Decimal(new_fills) * oprice
                                await self._update_local_state(Decimal(new_fills) * oprice, Decimal("0.00"), state, contract_id, -new_fills)
                                last_reported_fill[oid] = accumulated_fills[oid]
                                logger.info(f"[{contract_id}] TP partial fill reconciled during cleanup: {filled}/{oqty}")
                        except Exception as ex:
                            logger.warning(f"[{contract_id}] Failed to reconcile order details during cancellation: {ex}")
                        completed_orders.add(oid)
                    except Exception as ce:
                        logger.error(f"[{contract_id}] Failed to cancel order {oid} during TP cleanup: {ce}")

                cleanup_tasks = [
                    cancel_and_reconcile_cleanup(oid, oqty, oprice, olabel)
                    for oid, oqty, oprice, olabel in order_ids
                ]
                if cleanup_tasks:
                    await asyncio.gather(*cleanup_tasks)
                            
                unfilled = quantity - total_filled
                if unfilled > 0:
                    if hasattr(self.broker, 'positions') and isinstance(self.broker.positions, dict):
                        self.broker.positions.pop((contract_id, side.lower()), None)
                    # Only apply local position clearance if we are in paper-trading/simulation mode.
                    # In live trading, let the sync loop naturally reconcile the settled state.
                    if getattr(self.broker, 'paper_trade', False) or hasattr(self.broker, 'simulated_balance'):
                        await self._update_local_state(Decimal("0.00"), Decimal("0.00"), state, contract_id, -unfilled)

            await self._safe_shield(run_cleanup())

    async def paper_fill_dispatcher(self):
        """Background loop for paper trading: simulates incremental order fills and pushes to active_tp_orders."""
        last_sent_fill = {}
        while not self.shutting_down:
            try:
                active_ids = list(self.active_tp_orders.keys())
                for oid in list(last_sent_fill.keys()):
                    if oid not in self.active_tp_orders:
                        last_sent_fill.pop(oid, None)
                
                contract_books = {}
                for oid in active_ids:
                    if oid.startswith("paper-"):
                        async with self.broker.paper_orders_lock:
                            order_data = self.broker._paper_orders.get(oid)
                            if order_data and order_data["status"] not in ("executed", "canceled"):
                                cid = order_data["contract_id"]
                                side = order_data["side"]
                            else:
                                continue
                        cache_key = (cid, side)
                        if cache_key not in contract_books:
                            best_vals = await self.broker.get_best_bid_ask(cid, side)
                            contract_books[cache_key] = list(best_vals) if best_vals else None
                
                for oid in active_ids:
                    if oid.startswith("paper-"):
                        async with self.broker.paper_orders_lock:
                            order_data = self.broker._paper_orders.get(oid)
                            if order_data:
                                cid = order_data["contract_id"]
                                side = order_data["side"]
                            else:
                                cid = None
                                side = None
                        if cid is not None:
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
            await asyncio.sleep(3.0)

    async def execute_and_hold_entry(self, state: AssetState, contract_id: str, side: str, limit_price: Decimal, quantity: int, total_cost: Decimal, seconds_left: float, hold_to_settle: bool = False):
        executing_strike = getattr(state, "strike_price", 0.0)
        order_id = None
        locked_capital = total_cost

        async def release_locked_capital(avail_delta: Decimal, qty_delta: int = 0):
            nonlocal locked_capital
            _lc = locked_capital
            locked_capital = Decimal("0.00")
            if _lc > 0:
                await self._safe_shield(self._update_local_state(avail_delta, -_lc, state, contract_id, qty_delta))

        try:
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
                            await release_locked_capital(Decimal("0.00"))
                            logger.info(f"[{contract_id}] Order filled ({quantity}/{quantity}).")
                            
                            tp_task = asyncio.create_task(self._monitor_take_profit(state, contract_id, side, _extract_fill_price(details, limit_price), quantity, seconds_left, hold_to_settle=hold_to_settle, strike_price=executing_strike))
                            self._pending_tasks.add(tp_task)
                            tp_task.add_done_callback(self._handle_task_done)
                            return
                        elif status in ["canceled", "cancelled"]:
                            unfilled_qty = quantity - filled_qty
                            if unfilled_qty <= 0:
                                await release_locked_capital(Decimal("0.00"))
                                logger.info(f"[{contract_id}] Order filled ({quantity}/{quantity}).")
                                
                                tp_task = asyncio.create_task(self._monitor_take_profit(state, contract_id, side, _extract_fill_price(details, limit_price), quantity, seconds_left, hold_to_settle=hold_to_settle, strike_price=executing_strike))
                                self._pending_tasks.add(tp_task)
                                tp_task.add_done_callback(self._handle_task_done)
                                return
                            else:
                                refund = Decimal(str(unfilled_qty)) * limit_price
                                await release_locked_capital(refund, -unfilled_qty)
                                logger.info(f"[{contract_id}] Partial Fill ({filled_qty}/{quantity}).")
                                
                                tp_task = asyncio.create_task(self._monitor_take_profit(state, contract_id, side, _extract_fill_price(details, limit_price), filled_qty, seconds_left, hold_to_settle=hold_to_settle, strike_price=executing_strike))
                                self._pending_tasks.add(tp_task)
                                tp_task.add_done_callback(self._handle_task_done)
                                return
                        else:
                            logger.critical(f"[{contract_id}] Partial cancel status unknown ({status}). Spawning TP for {filled_qty} confirmed fills.")
                            unfilled_qty = quantity - filled_qty
                            refund = Decimal(str(unfilled_qty)) * limit_price
                            await release_locked_capital(refund, -unfilled_qty)
                            tp_task = asyncio.create_task(self._monitor_take_profit(state, contract_id, side, _extract_fill_price(details, limit_price), filled_qty, seconds_left, hold_to_settle=hold_to_settle, strike_price=executing_strike))
                            self._pending_tasks.add(tp_task)
                            tp_task.add_done_callback(self._handle_task_done)
                            return
                    else:
                        logger.info(f"[{contract_id}] Order filled ({filled_qty}/{quantity}).")

                        await release_locked_capital(Decimal("0.00"))
                        
                        tp_task = asyncio.create_task(self._monitor_take_profit(state, contract_id, side, _extract_fill_price(details, limit_price), filled_qty, seconds_left, hold_to_settle=hold_to_settle, strike_price=executing_strike))
                        self._pending_tasks.add(tp_task)
                        tp_task.add_done_callback(self._handle_task_done)
                        
                        logger.warning(f"[{contract_id}] Position Secured. Execution task safely terminating.")
                        return
                else:
                    logger.warning(f"[{contract_id}] Limit buy missed fill window. Canceling.")
                    cancel_success = await self.broker.cancel_order(order_id)
                    
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
                        await release_locked_capital(Decimal("0.00"))
                        logger.warning(f"[{contract_id}] Order fully filled prior to cancellation.")
                        
                        tp_task = asyncio.create_task(self._monitor_take_profit(state, contract_id, side, _extract_fill_price(details, limit_price), quantity, seconds_left, hold_to_settle=hold_to_settle, strike_price=executing_strike))
                        self._pending_tasks.add(tp_task)
                        tp_task.add_done_callback(self._handle_task_done)
                        return
                    elif status in ["canceled", "cancelled"]:
                        if filled_qty > 0:
                            unfilled_qty = quantity - filled_qty
                            refund = Decimal(str(unfilled_qty)) * limit_price
                            await release_locked_capital(refund, -unfilled_qty)
                            logger.warning(f"[{contract_id}] Partially filled ({filled_qty}/{quantity}) prior to cancellation.")
                            
                            tp_task = asyncio.create_task(self._monitor_take_profit(state, contract_id, side, _extract_fill_price(details, limit_price), filled_qty, seconds_left, hold_to_settle=hold_to_settle, strike_price=executing_strike))
                            self._pending_tasks.add(tp_task)
                            tp_task.add_done_callback(self._handle_task_done)
                            return
                        else:
                            await release_locked_capital(locked_capital, -quantity)
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
                await release_locked_capital(locked_capital, -quantity)

        except (Exception, asyncio.CancelledError) as e:
            logger.critical(f"[{contract_id}] Unhandled exception or cancellation in entry manager. Forcing release.", exc_info=True)
            if order_id:
                async def cancel_and_reconcile():
                    filled_qty = 0
                    try:
                        logger.warning(f"[{contract_id}] Entry manager cancelled/faulted. Cancelling order {order_id}.")
                        await self.broker.cancel_order(order_id)
                    except Exception as ce:
                        logger.error(f"[{contract_id}] Failed to cancel order during cleanup: {ce}")
                    
                    try:
                        details = await self.broker.get_order_details(order_id)
                        filled_qty = self._get_filled_qty_from_details(details, quantity)
                    except Exception as de:
                        logger.error(f"[{contract_id}] Failed to get final details: {de}")
                    
                    if filled_qty > 0:
                        unfilled_qty = quantity - filled_qty
                        refund = Decimal(str(unfilled_qty)) * limit_price
                        await release_locked_capital(refund, -unfilled_qty)
                        try:
                            tp_task = asyncio.create_task(self._monitor_take_profit(
                                state, contract_id, side, _extract_fill_price(details, limit_price) if details else limit_price,
                                filled_qty, seconds_left, hold_to_settle=hold_to_settle, strike_price=executing_strike
                            ))
                            self._pending_tasks.add(tp_task)
                            tp_task.add_done_callback(self._handle_task_done)
                        except Exception as tp_spawn_err:
                            logger.critical(f"[{contract_id}] Failed to spawn TP task: {tp_spawn_err}")
                    else:
                        await release_locked_capital(locked_capital, -quantity)

                await self._safe_shield(cancel_and_reconcile())
            else:
                if locked_capital > 0:
                    await self._safe_shield(release_locked_capital(locked_capital, -quantity))
            raise
        finally:
            await self._safe_shield(self._decrement_trade_cap(contract_id))

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

        if mean > 0.0:
            percentage_deviation = abs(tick_price - mean) / mean
            if percentage_deviation > config.MAX_PRICE_DEVIATION_PCT:
                state.consecutive_outliers += 1
                if state.consecutive_outliers >= config.CONSECUTIVE_OUTLIER_LIMIT:
                    logger.warning(f"[OUTLIER SHIELD] Detected {state.consecutive_outliers} consecutive outlier ticks. Forcing baseline reset.")
                    state.fast_indicators = kalshi_bot.FastIndicators(14, float(config.EMA_ALPHA))
                    state.fast_indicators.add_price(tick_price)
                    state.tick_count = 1                  
                    state.last_tick_time = time.time()
                    state.consecutive_outliers = 0
                    state.last_price = tick_price
                    return  
                else:
                    logger.warning(f"[OUTLIER SHIELD] Ignored anomalous price change on {product_id}: ${tick_price:.2f} vs Mean: ${mean:.2f}")
                    return
            else:
                state.consecutive_outliers = 0

        current_time = time.time()
        
        state.tick_count += 1
        state.last_tick_time = time.time()
        tick_volume = tick_dict.get("volume", 0.0)
        tick_side = tick_dict.get("side", "buy")
        if tick_volume and tick_volume > 0:
            usd_notional_k = (float(tick_price) * float(tick_volume)) / 1000.0
            state.fast_indicators.add_price_with_volume(tick_price, usd_notional_k)
            state.taker_ofi_tracker.add_trade(current_time, float(tick_price) * float(tick_volume), tick_side == "buy")
        else:
            state.fast_indicators.add_price(tick_price)

        state.index_lag_tracker.add_tick(current_time, tick_price)
        state.last_price = float(tick_price)

        if config.ENABLE_INDEX_LAG_STRATEGY:
            await self._evaluate_index_lag_entry(product_id, state, current_time)
        if config.ENABLE_OFI_STRATEGY:
            await self._evaluate_ofi_entry(product_id, state, current_time)

    # ==========================================
    # BINANCE LIQUIDATION SNIPER
    # ==========================================
    async def process_binance_liquidation(self, event_data: Any):
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
            if isinstance(event_data, dict):
                payload_dict = validate_binance_payload(event_data)
            else:
                parsed_dict = orjson.loads(event_data)
                nested_data = parsed_dict.get("data", parsed_dict)
                payload_dict = validate_binance_payload(nested_data)
                
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
            
            liq_price = float(payload_dict["o"]["p"])
            is_stale = (state.last_price is None or 
                        (asset_symbol != "HYPE-USD" and time.time() - getattr(state, "last_tick_time", 0.0) > 15.0))
            if is_stale or asset_symbol == "HYPE-USD":
                state.last_price = liq_price
                state.fast_indicators.add_price(liq_price)
                state.tick_count += 1
                state.last_tick_time = time.time()
                
            current_hour = int(datetime.datetime.now(datetime.timezone.utc).hour)
            if not self.performance_tracker.should_trade(asset_symbol, current_hour):
                logger.warning(f"[{asset_symbol}] Auto-throttling active: trade blocked for hour {current_hour} due to poor performance history.")
                return
            
            notional = payload_dict["o"]["p"] * payload_dict["o"]["q"]
            threshold = config.BINANCE_LIQUIDATION_THRESHOLDS.get(asset_symbol)
            if not threshold or notional < threshold: return 

            current_time = time.time()
            seconds_left = state.expiration_time - current_time if state.expiration_time else 720.0
            if seconds_left < config.STRATEGY_1_MIN_SECONDS_LEFT or seconds_left > config.STRATEGY_1_MAX_SECONDS_LEFT:
                return
            
            is_mean_reversion = (seconds_left > 480.0)
            
            if payload_dict["o"]["S"] == "SELL": 
                trade_side = "YES" if is_mean_reversion else "NO"
            elif payload_dict["o"]["S"] == "BUY": 
                trade_side = "NO" if is_mean_reversion else "YES"
            else: 
                return

            if self.circuit_breaker.is_locked_out():
                return

            if getattr(self.broker, "rate_limited_until", 0.0) > time.time():
                return

            if self.last_sync_time == 0.0 or time.time() - self.last_sync_time > config.STALE_BALANCE_TIMEOUT_SEC: 
                logger.warning(f"[{asset_symbol}] Dropping liquidation event — balance data is stale.")
                return
            
            if current_time < state.cooldown_until: return
            
            macro = self.macro_trend.get(asset_symbol, "FLAT")
            if trade_side == "YES" and macro == "DOWN":
                logger.info(f"[{asset_symbol}] Liquidation Sniper blocked by 3H Trend Shield: Trade is YES (bullish), but macro trend is DOWN.")
                return
            if trade_side == "NO" and macro == "UP":
                logger.info(f"[{asset_symbol}] Liquidation Sniper blocked by 3H Trend Shield: Trade is NO (bearish), but macro trend is UP.")
                return

            if is_mean_reversion:
                if state.tick_count < 50:
                    logger.info(f"[{asset_symbol}] Early-window mean-reversion blocked: Indicator warmup in progress ({state.tick_count}/50 ticks).")
                    return
                
                mean, upper, lower = state.fast_indicators.get_bollinger_bands()
                std_dev = (upper - mean) / 2.0
                current_spot = float(state.last_price) if state.last_price is not None else 0.0
                floor_pct = config.STD_DEV_FLOORS_PCT.get(asset_symbol, 0.0005)
                floor = floor_pct * current_spot
                if std_dev < floor:
                    logger.info(f"[{asset_symbol}] Early-window mean-reversion blocked: Volatility too low (StdDev: {std_dev:.4f} < Floor: {floor:.4f}).")
                    return
                if upper > lower:
                    current_spot = float(state.last_price)
                    band_proximity = std_dev * 0.5
                    if trade_side == "YES":
                        if current_spot > lower + band_proximity:
                            logger.info(f"[{asset_symbol}] Early-window mean-reversion blocked: Spot ${current_spot:.2f} has not approached lower Bollinger Band ${lower:.2f} (proximity: {band_proximity:.2f}).")
                            return
                    elif trade_side == "NO":
                        if current_spot < upper - band_proximity:
                            logger.info(f"[{asset_symbol}] Early-window mean-reversion blocked: Spot ${current_spot:.2f} has not approached upper Bollinger Band ${upper:.2f} (proximity: {band_proximity:.2f}).")
                            return
            
            executing_contract_id = state.active_contract_id
            if executing_contract_id == getattr(state, "last_traded_event", ""): return
            
            slot_acquired = False
            local_mutated = False
            try:
                async with self.trade_cap_lock:
                    if self.active_trade_count >= config.MAX_CONCURRENT_TRADES: return
                    if executing_contract_id in self.execution_in_flight: return
                    self.active_trade_count += 1
                    self.execution_in_flight.add(executing_contract_id)
                    slot_acquired = True
                best_vals = await self.broker.get_best_bid_ask(executing_contract_id, trade_side)
                if not best_vals:
                    return
                    
                if self.circuit_breaker.is_locked_out():
                    return
                current_time = time.time()
                seconds_left = state.expiration_time - current_time if state.expiration_time else 720.0
                if seconds_left < config.STRATEGY_1_MIN_SECONDS_LEFT or seconds_left > config.STRATEGY_1_MAX_SECONDS_LEFT:
                    return
                
                is_mean_reversion_post = (seconds_left > 480.0)
                if is_mean_reversion_post != is_mean_reversion:
                    logger.warning(f"[{asset_symbol}] Regime shift detected during network yield (Mean Reversion was {is_mean_reversion}, now {is_mean_reversion_post}). Aborting trade.")
                    return
                
                if executing_contract_id != state.active_contract_id:
                    return

                limit_price = self._validate_orderbook_entry(
                    asset_symbol, state, trade_side, best_vals,
                    signal_tag=None, is_mean_reversion_post=is_mean_reversion_post
                )
                if limit_price is None:
                    return

                slot_res = await self._acquire_execution_slot(state, executing_contract_id, trade_side, limit_price, current_time)
                if not slot_res:
                    return
                quantity, total_cost, local_mutated = slot_res
                
                logger.warning(f"[{asset_symbol}] BINANCE LIQUIDATION SIGNAL (${notional:,.2f})! Ask: ${best_vals[1]:.2f} | Sniping {quantity} contracts.")
                
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
                if 'exec_task' in locals():
                    if not exec_task.done():
                        exec_task.cancel()
                if local_mutated and slot_acquired:
                    await self._safe_shield(self._update_local_state(total_cost, -total_cost, state, executing_contract_id, -quantity))
                    if getattr(state, "last_traded_event", "") == executing_contract_id:
                        state.last_traded_event = ""
                raise
            finally:
                if slot_acquired:
                    await self._safe_shield(self._decrement_trade_cap(executing_contract_id))
                
        except Exception as e:
            logger.error("Liquidation processing fault", exc_info=True) 

    # ==========================================
    # STRATEGY 4: INDEX LAG ARBITRAGE
    # ==========================================
    async def _evaluate_index_lag_entry(self, asset_symbol: str, state: AssetState, current_time: float):
        if self.shutting_down or not state.active_contract_id or current_time < state.cooldown_until:
            return
        if state.active_contract_id == getattr(state, "last_traded_event", ""):
            return
        if current_time - getattr(state, "last_signal_time", 0.0) < float(config.SIGNAL_EVAL_THROTTLE_SECS):
            return
        if self.circuit_breaker.is_locked_out():
            return
            
        seconds_left = state.expiration_time - current_time if state.expiration_time else 720.0
        if seconds_left < config.STRATEGY_4_MIN_SECONDS_LEFT or seconds_left > config.STRATEGY_4_MAX_SECONDS_LEFT:
            return

        current_hour = int(datetime.datetime.now(datetime.timezone.utc).hour)
        if not self.performance_tracker.should_trade(asset_symbol, current_hour):
            return

        if state.last_price is None or state.last_price <= 0.0:
            return
        current_spot = float(state.last_price)
        divergence = state.index_lag_tracker.get_divergence(current_spot)
        min_div = float(config.INDEX_LAG_MIN_DIVERGENCE_ETH) if asset_symbol == "ETH-USD" else float(config.INDEX_LAG_MIN_DIVERGENCE)

        if abs(divergence) < min_div:
            return

        state.last_signal_time = current_time
        trade_side = "YES" if divergence > 0.0 else "NO"
        div_pct = divergence * 100.0
        logger.info(f"[{asset_symbol}] INDEX LAG SIGNAL! Spot: ${current_spot:.2f} | 60s Avg: ${state.index_lag_tracker.get_average():.2f} | Div: {div_pct:+.3f}% | Side: {trade_side}")
        await self._route_generic_signal_entry(asset_symbol, state, trade_side, f"INDEX_LAG ({div_pct:+.2f}%)", current_time, seconds_left)

    # ==========================================
    # STRATEGY 5: TAKER ORDER FLOW IMBALANCE (OFI)
    # ==========================================
    async def _evaluate_ofi_entry(self, asset_symbol: str, state: AssetState, current_time: float):
        if self.shutting_down or not state.active_contract_id or current_time < state.cooldown_until:
            return
        if state.active_contract_id == getattr(state, "last_traded_event", ""):
            return
        if current_time - getattr(state, "last_signal_time", 0.0) < float(config.SIGNAL_EVAL_THROTTLE_SECS):
            return
        if self.circuit_breaker.is_locked_out():
            return
            
        seconds_left = state.expiration_time - current_time if state.expiration_time else 720.0
        if seconds_left < config.STRATEGY_5_MIN_SECONDS_LEFT or seconds_left > config.STRATEGY_5_MAX_SECONDS_LEFT:
            return

        current_hour = int(datetime.datetime.now(datetime.timezone.utc).hour)
        if not self.performance_tracker.should_trade(asset_symbol, current_hour):
            return

        target_ratio = float(config.OFI_BUY_SELL_RATIO)
        min_vol = float(config.OFI_MIN_VOLUME_NOTIONAL_ETH) if asset_symbol == "ETH-USD" else float(config.OFI_MIN_VOLUME_NOTIONAL)

        trade_side, persistence_count, is_transition = state.taker_ofi_tracker.update_and_check_persistence(
            current_time, target_ratio, min_vol, 2.5
        )

        if not trade_side or not is_transition:
            return

        buy_vol, sell_vol, ratio = state.taker_ofi_tracker.get_metrics()
        if min(buy_vol, sell_vol) < 2500.0:
            return

        if persistence_count == 1:
            logger.info(f"[{asset_symbol}] TAKER OFI CANDIDATE! ({trade_side} | Ratio: {ratio:.2f}x) Persistence: {persistence_count}/2. Awaiting 2.5s confirmation...")
            return
        macro = self.macro_trend.get(asset_symbol, "FLAT")
        if trade_side == "YES" and macro == "DOWN":
            logger.info(f"[{asset_symbol}] TAKER OFI blocked by 3H Trend Shield: Trade is YES (bullish), but macro trend is DOWN.")
            return
        if trade_side == "NO" and macro == "UP":
            logger.info(f"[{asset_symbol}] TAKER OFI blocked by 3H Trend Shield: Trade is NO (bearish), but macro trend is UP.")
            return

        state.last_signal_time = current_time
        logger.warning(f"[{asset_symbol}] 🎯 CONFIRMED TAKER OFI SIGNAL (2/2 Persistence)! 30s BuyVol: ${buy_vol:,.0f} | SellVol: ${sell_vol:,.0f} | Ratio: {ratio:.2f}x | Side: {trade_side}")
        await self._route_generic_signal_entry(asset_symbol, state, trade_side, f"TAKER_OFI ({ratio:.1f}x)", current_time, seconds_left)

    # ==========================================
    # GENERIC SIGNAL ROUTER
    # ==========================================
    async def _route_generic_signal_entry(self, asset_symbol: str, state: AssetState, trade_side: str, signal_tag: str, current_time: float, seconds_left: float):
        if "DOGE" in asset_symbol or asset_symbol == "DOGE-USD":
            logger.info(f"[{asset_symbol}] Signal entry blocked: DOGE trading is currently DISABLED.")
            return

        if self.last_sync_time == 0.0 or time.time() - self.last_sync_time > config.STALE_BALANCE_TIMEOUT_SEC:
            return

        executing_contract_id = state.active_contract_id
        if executing_contract_id == getattr(state, "last_traded_event", ""):
            return

        slot_acquired = False
        local_mutated = False
        try:
            async with self.trade_cap_lock:
                if self.active_trade_count >= config.MAX_CONCURRENT_TRADES:
                    return
                if executing_contract_id in self.execution_in_flight:
                    return
                self.active_trade_count += 1
                self.execution_in_flight.add(executing_contract_id)
                slot_acquired = True

            best_vals = await self.broker.get_best_bid_ask(executing_contract_id, trade_side)
            if not best_vals:
                return

            if self.circuit_breaker.is_locked_out():
                return

            if getattr(self.broker, "rate_limited_until", 0.0) > time.time():
                return

            current_time = time.time()
            sec_left_now = state.expiration_time - current_time if state.expiration_time else 720.0
            if sec_left_now < 90.0 or sec_left_now > 480.0:
                return

            if executing_contract_id != state.active_contract_id:
                return

            limit_price = self._validate_orderbook_entry(
                asset_symbol, state, trade_side, best_vals,
                signal_tag=signal_tag, is_mean_reversion_post=False
            )
            if limit_price is None:
                return

            slot_res = await self._acquire_execution_slot(state, executing_contract_id, trade_side, limit_price, current_time)
            if not slot_res:
                return
            quantity, total_cost, local_mutated = slot_res

            logger.warning(f"[{asset_symbol}] [{signal_tag}] SIGNAL EXECUTING! Ask: ${best_vals[1]:.2f} | Sniping {quantity} contracts.")
            exec_task = asyncio.create_task(
                self.execute_and_hold_entry(
                    state, executing_contract_id, trade_side, limit_price, quantity, total_cost, sec_left_now
                )
            )
            self._pending_tasks.add(exec_task)
            exec_task.add_done_callback(self._handle_task_done)
            slot_acquired = False
        except Exception as inner_e:
            logger.error(f"[{signal_tag}] Execution fault", exc_info=True)
            if 'exec_task' in locals():
                if not exec_task.done():
                    exec_task.cancel()
            if local_mutated and slot_acquired:
                await self._safe_shield(self._update_local_state(total_cost, -total_cost, state, executing_contract_id, -quantity))
                if getattr(state, "last_traded_event", "") == executing_contract_id:
                    state.last_traded_event = ""
            raise
        finally:
            if slot_acquired:
                await self._safe_shield(self._decrement_trade_cap(executing_contract_id)) 

# ==========================================
# ASYNC QUEUES
# ==========================================
async def coinbase_websocket_consumer(engine: LiveTradingEngine, queue: asyncio.Queue):
    uri = "wss://ws-feed.exchange.coinbase.com" 
    subscribe_message = {
        "type": "subscribe",
        "product_ids": ["BTC-USD", "SOL-USD", "ETH-USD", "DOGE-USD"],
        "channels": ["ticker"]
    }

    attempt = 0
    max_attempts = 30
    while not engine.shutting_down:
        conn_start = time.time()
        reset_done = False
        try:
            if not await is_safe_destination_async(uri):
                logger.critical(f"[SECURITY] Aborting Coinbase WS connection. Destination '{uri}' fails boundary rules.")
                engine.shutting_down = True
                break
            async with websockets.connect(uri, ssl=GLOBAL_SSL_CONTEXT, max_size=1048576, max_queue=256) as ws:
                logger.info("Connected to Coinbase Live Spot Feed.")
                await ws.send(orjson.dumps(subscribe_message).decode('utf-8'))
                
                async for message in ws:
                    if engine.shutting_down: break
                    if not reset_done and time.time() - conn_start > 10.0:
                        attempt = 0  
                        reset_done = True
                    try: 
                        queue.put_nowait((time.time(), message))
                    except asyncio.QueueFull: 
                        engine.purge_memory(queue) 
                        try:
                            queue.put_nowait((time.time(), message))
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
            ingress_time, message = await asyncio.wait_for(queue.get(), timeout=1.0)
            try: 
                now = time.time()
                if now - ingress_time > 3.0:
                    last_drop_log = getattr(engine, "_last_drop_log_tick_time", 0.0)
                    if now - last_drop_log > 5.0:
                        logger.warning(f"[LATENCY GATE] Dropping stale spot tick inside worker (delayed by {now - ingress_time:.2f}s)")
                        engine._last_drop_log_tick_time = now
                    continue
                await engine.process_live_tick(message)
            except Exception as e: logger.error("Tick fault", exc_info=True)
            finally: queue.task_done()

            # Batch drain backlogged ticks during volatility surges to minimize event loop contention
            if queue.qsize() > 50:
                drain_count = 0
                while drain_count < 20 and not queue.empty():
                    try:
                        ig_t, msg = queue.get_nowait()
                        if time.time() - ig_t <= 3.0:
                            try:
                                await engine.process_live_tick(msg)
                            except Exception as tick_err:
                                logger.error(f"Tick processing fault inside batch drain: {tick_err}", exc_info=True)
                        else:
                            logger.warning(f"[LATENCY GATE] Skipped stale tick in batch drain (age: {time.time() - ig_t:.2f}s)")
                        queue.task_done()
                        drain_count += 1
                    except asyncio.QueueEmpty:
                        break
                    except Exception as batch_err:
                        logger.error(f"Batch drain queue exception: {batch_err}", exc_info=True)
                        break
                await asyncio.sleep(0)
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
            if not await is_safe_destination_async(uri):
                logger.critical(f"[SECURITY] Aborting Binance WS connection. Destination '{uri}' fails boundary rules.")
                engine.shutting_down = True
                break
            async with websockets.connect(uri, ssl=GLOBAL_SSL_CONTEXT, max_size=1048576, max_queue=256) as ws:
                logger.info("Connected to Binance Futures Feed.")
                async for message in ws:
                    if engine.shutting_down: break
                    if not reset_done and time.time() - conn_start > 10.0:
                        attempt = 0  
                        reset_done = True
                    try: 
                        queue.put_nowait((time.time(), message))
                    except asyncio.QueueFull: 
                        logger.warning("Binance queue overflow - purging and retrying.")
                        safe_drain_queue(queue)
                        try:
                            queue.put_nowait((time.time(), message))
                        except asyncio.QueueFull:
                            pass
                
                logger.warning("Binance WebSocket closed cleanly. Draining stale queue signals...")
                safe_drain_queue(queue)
                if not reset_done and time.time() - conn_start > 10.0:
                    attempt = 0
                attempt += 1
                if attempt > max_attempts:
                    logger.critical("[FATAL] Binance connection limit reached. Stopping consumer.")
                    engine.shutting_down = True
                    break
                delay = calculate_backoff_delay(attempt)
                await asyncio.sleep(delay)
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
        return

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
            if not await is_safe_destination_async(uri):
                logger.critical(f"[SECURITY] Aborting Kalshi WS connection. Destination '{uri}' fails boundary rules.")
                engine.shutting_down = True
                break
            current_time_ms = str(int(time.time() * 1000))
            sig = engine.broker._generate_signature(current_time_ms, "GET", "/trade-api/ws/v2")
            
            headers = {
                "KALSHI-ACCESS-KEY": engine.broker.key_id,
                "KALSHI-ACCESS-SIGNATURE": sig,
                "KALSHI-ACCESS-TIMESTAMP": current_time_ms
            }
            
            async with websockets.connect(uri, additional_headers=headers, ssl=GLOBAL_SSL_CONTEXT, max_size=1048576, max_queue=256) as ws:
                logger.debug("Connected to Kalshi Private WebSocket Feed.")
                
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
                                        if len(engine.orphan_fills) > 200:
                                            evicted_key = next(iter(engine.orphan_fills))
                                            logger.warning(f"[SECURITY] Evicting orphan fill for {evicted_key}. Potential fill loss.")
                                            engine.orphan_fills.pop(evicted_key)
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

async def bybit_websocket_consumer(engine: LiveTradingEngine, queue: asyncio.Queue):
    uri = "wss://stream.bybit.com/v5/public/linear"
    subscribe_message = {
        "op": "subscribe",
        "args": [
            "allLiquidation.BTCUSDT",
            "allLiquidation.ETHUSDT",
            "allLiquidation.SOLUSDT",
            "allLiquidation.DOGEUSDT",
            "allLiquidation.HYPEUSDT"
        ]
    }
    
    attempt = 0
    max_attempts = 30
    while not engine.shutting_down:
        conn_start = time.time()
        reset_done = False
        try:
            if not await is_safe_destination_async(uri):
                logger.critical(f"[SECURITY] Aborting Bybit WS connection. Destination '{uri}' fails boundary rules.")
                engine.shutting_down = True
                break
            async with websockets.connect(uri, ssl=GLOBAL_SSL_CONTEXT, max_size=1048576, max_queue=256) as ws:
                logger.info("Connected to Bybit Futures Feed.")
                await ws.send(orjson.dumps(subscribe_message).decode('utf-8'))
                async for message in ws:
                    if engine.shutting_down: break
                    if not reset_done and time.time() - conn_start > 10.0:
                        attempt = 0  
                        reset_done = True
                    try:
                        msg = orjson.loads(message)
                        if "data" in msg:
                            data_field = msg["data"]
                            items = data_field if isinstance(data_field, list) else [data_field]
                            for item in items:
                                symbol = item.get("s")
                                if not symbol:
                                    continue
                                bybit_side = item.get("S")
                                if bybit_side == "Buy":
                                    mapped_side = "SELL"
                                elif bybit_side == "Sell":
                                    mapped_side = "BUY"
                                else:
                                    continue
                                
                                normalized = {
                                    "e": "forceOrder",
                                    "o": {
                                        "s": symbol,
                                        "S": mapped_side,
                                        "q": item.get("v"),
                                        "p": item.get("p")
                                    }
                                }
                                try:
                                    queue.put_nowait((time.time(), normalized))
                                except asyncio.QueueFull:
                                    logger.warning("Liquidation queue overflow during Bybit ingestion - purging.")
                                    safe_drain_queue(queue)
                                    try:
                                        queue.put_nowait((time.time(), normalized))
                                    except asyncio.QueueFull:
                                        pass
                    except Exception as parse_err:
                        logger.warning(f"Error parsing Bybit WS frame: {parse_err}")
                        
        except Exception as e:
            if not reset_done and time.time() - conn_start > 10.0:
                attempt = 0
            attempt += 1
            if attempt > max_attempts:
                logger.critical("[FATAL] Bybit connection limit reached. Stopping consumer.")
                engine.shutting_down = True
                break
            delay = calculate_backoff_delay(attempt)
            logger.warning(f"Bybit WS error ({type(e).__name__}). Retrying in {delay:.2f}s...")
            await asyncio.sleep(delay)

async def hyperliquid_websocket_consumer(engine: LiveTradingEngine, queue: asyncio.Queue):
    uri = "wss://api.hyperliquid.xyz/ws"
    subscribe_messages = [
        {"method": "subscribe", "subscription": {"type": "trades", "coin": "BTC"}},
        {"method": "subscribe", "subscription": {"type": "trades", "coin": "ETH"}},
        {"method": "subscribe", "subscription": {"type": "trades", "coin": "SOL"}},
        {"method": "subscribe", "subscription": {"type": "trades", "coin": "DOGE"}},
        {"method": "subscribe", "subscription": {"type": "trades", "coin": "HYPE"}}
    ]
    
    attempt = 0
    max_attempts = 30
    while not engine.shutting_down:
        conn_start = time.time()
        reset_done = False
        try:
            if not await is_safe_destination_async(uri):
                logger.critical(f"[SECURITY] Aborting Hyperliquid WS connection. Destination '{uri}' fails boundary rules.")
                engine.shutting_down = True
                break
            async with websockets.connect(uri, ssl=GLOBAL_SSL_CONTEXT, max_size=1048576, max_queue=256) as ws:
                logger.info("Connected to Hyperliquid Trades Feed.")
                for sub_msg in subscribe_messages:
                    await ws.send(orjson.dumps(sub_msg).decode('utf-8'))
                    
                async for message in ws:
                    if engine.shutting_down: break
                    if not reset_done and time.time() - conn_start > 10.0:
                        attempt = 0  
                        reset_done = True
                    try:
                        msg = orjson.loads(message)
                        if msg.get("channel") == "trades" and "data" in msg:
                            for trade in msg["data"]:
                                if "liquidation" in trade:
                                    coin = trade.get("coin")
                                    if not coin:
                                        continue
                                    symbol = f"{coin}USDT"
                                    hl_side = trade.get("side")
                                    if hl_side == "A":
                                        mapped_side = "SELL"
                                    elif hl_side == "B":
                                        mapped_side = "BUY"
                                    else:
                                        continue
                                    
                                    normalized = {
                                        "e": "forceOrder",
                                        "o": {
                                            "s": symbol,
                                            "S": mapped_side,
                                            "q": trade.get("sz"),
                                            "p": trade.get("px")
                                        }
                                    }
                                    try:
                                        queue.put_nowait((time.time(), normalized))
                                    except asyncio.QueueFull:
                                        logger.warning("Liquidation queue overflow during Hyperliquid ingestion - purging.")
                                        safe_drain_queue(queue)
                                        try:
                                            queue.put_nowait((time.time(), normalized))
                                        except asyncio.QueueFull:
                                            pass
                    except Exception as parse_err:
                        logger.warning(f"Error parsing Hyperliquid WS frame: {parse_err}")
                        
        except Exception as e:
            if not reset_done and time.time() - conn_start > 10.0:
                attempt = 0
            attempt += 1
            if attempt > max_attempts:
                logger.critical("[FATAL] Hyperliquid connection limit reached. Stopping consumer.")
                engine.shutting_down = True
                break
            delay = calculate_backoff_delay(attempt)
            logger.warning(f"Hyperliquid WS error ({type(e).__name__}). Retrying in {delay:.2f}s...")
            await asyncio.sleep(delay)

async def binance_worker_loop(engine: LiveTradingEngine, queue: asyncio.Queue):
    semaphore = asyncio.Semaphore(10)

    async def worker(ingress_time, message):
        try:
            now = time.time()
            if now - ingress_time > 1.5:
                last_drop_log = getattr(engine, "_last_drop_log_time", 0.0)
                if now - last_drop_log > 5.0:
                    logger.warning(f"[LATENCY GATE] Dropping stale liquidation event inside worker (delayed by {now - ingress_time:.2f}s)")
                    engine._last_drop_log_time = now
                return
            await engine.process_binance_liquidation(message)
        finally:
            semaphore.release()

    while not engine.shutting_down:
        try:
            ingress_time, message = await asyncio.wait_for(queue.get(), timeout=1.0)
            try: 
                await semaphore.acquire()
                task = asyncio.create_task(worker(ingress_time, message))
                engine._pending_tasks.add(task)
                task.add_done_callback(engine._handle_task_done)
            except Exception as e: 
                logger.error("Binance liquidation task spawning fault", exc_info=True)
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
        
        if isinstance(response, dict) and 'SecretString' in response:
            response['SecretString'] = "X" * len(resp_json)
        
        del response
        
        resp_dict = orjson.loads(resp_json)
        del resp_json
        
        key_id = resp_dict["KEY_ID"]
        private_key_pem = bytearray(resp_dict["PRIVATE_KEY"], 'utf-8')
        
        pem_len = len(resp_dict["PRIVATE_KEY"])
        kid_len = len(resp_dict["KEY_ID"])
        resp_dict["PRIVATE_KEY"] = "X" * pem_len
        resp_dict["KEY_ID"] = "X" * kid_len
        del resp_dict
        
        private_key = load_pem_private_key(private_key_pem, password=None)
        
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
        gc.collect()
