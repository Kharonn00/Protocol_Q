"""
Kalshi Quantitative Trading Engine (KQTE) - Security & Circuit Breaker

This module provides network security utilities and trading safety controls.
It includes SSRF/DNS rebinding protection, log injection sanitization,
a macroeconomic event circuit breaker, and rate-limited health check endpoints.
"""

import os
import sys
import time
import re
import asyncio
import logging
import urllib.parse
import aiohttp
import orjson
import datetime
import random
import ipaddress
import socket
from functools import lru_cache
from typing import Dict, List, Optional
from aiohttp.abc import AbstractResolver
from aiohttp import web
from pydantic import ValidationError

from engine.config import TRUSTED_INTERNAL_HOSTS, _ANSI_ESCAPE_RE
from engine.models import EconomicEvent, EconomicCalendarResponse

logger = logging.getLogger("KalshiQuantEngine")

# ==========================================
# SECURITY SANITIZATION & CACHING
# ==========================================
def sanitize_log_str(val: str) -> str:
    """Sanitizes strings prior to logging to prevent Log Injection (CWE-117) and ANSI escape injection."""
    val = val.replace('\n', '\\n').replace('\r', '\\r')
    return _ANSI_ESCAPE_RE.sub('', val)

_ZERO_NET = ipaddress.IPv4Network('0.0.0.0/8')

@lru_cache(maxsize=1024)
def is_private_ip(ip_str: str) -> bool:
    """Optimized and cached private IP lookup to prevent connection overhead."""
    try:
        ip_addr = ipaddress.ip_address(ip_str)
        # Unpack IPv4-mapped IPv6 addresses (e.g. ::ffff:127.0.0.1)
        if isinstance(ip_addr, ipaddress.IPv6Address) and ip_addr.ipv4_mapped is not None:
            ip_addr = ip_addr.ipv4_mapped
        if ip_addr.version == 4 and ip_addr in _ZERO_NET:
            return True
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
        if parsed.scheme not in ("https", "wss"):
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
        self.last_success_time: float = 0.0

    def is_locked_out(self) -> bool:
        current_time = time.time()
        locked = False
        active_event_name = ""
        
        # Stale calendar data validation for live/paper environments
        if self.calendar_url and os.environ.get("BOT_ENV", "simulation").lower() in ("live", "paper"):
            if self.last_success_time == 0.0 or (current_time - self.last_success_time > 86400.0):
                locked = True
                active_event_name = "Stale Calendar Data"

        if not locked:
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
                if active_event_name == "Stale Calendar Data":
                    logger.critical(f"[CIRCUIT BREAKER] Macro calendar data is stale (last success: {self.last_success_time}). Blocking all trades for safety.")
                else:
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
            # SEC-08: Relying on streaming chunk limit below to prevent compression bombs (CWE-409)
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
                        clean_event = "".join(c for c in raw_event if (c.isalnum() and c.isascii()) or c in " -()%./")
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
                self.last_success_time = time.time()
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
_HEALTH_MAX_REQUESTS_PER_SEC: int = 10

async def handle_health_check(request: web.Request) -> web.Response:
    """Hardened health check handler with method restriction and rate limiting."""
    # SEC-07a: Reject non-GET methods to prevent body-based resource exhaustion
    if request.method != "GET":
        return web.Response(status=405, text="Method Not Allowed")
    
    # Securely resolve client IP behind reverse proxies (like ALB) using right-to-left traversal of X-Forwarded-For
    x_forwarded_for = request.headers.get("X-Forwarded-For")
    remote_ip = request.remote or "127.0.0.1"
    
    # SEC-07c: Only resolve proxy IPs if the physical connector source is a private space (VPC/ALB)
    if is_private_ip(remote_ip):
        if x_forwarded_for:
            for ip in reversed([ip.strip() for ip in x_forwarded_for.split(",")]):
                if ip and not is_private_ip(ip):
                    remote_ip = ip
                    break
                
    # Bypass rate-limiter for loopback and private/VPC IP space (internal health checks)
    if is_private_ip(remote_ip):
        return web.json_response(
            {"status": "ok", "service": "kalshi-quant-engine"},
            dumps=lambda x: orjson.dumps(x).decode('utf-8')
        )
        
    # SEC-07b: Track rate limit per source IP for external traffic
    app = request.app
    if "health_limiter_ips" not in app:
        app["health_limiter_ips"] = {}
        
    limiters = app["health_limiter_ips"]
    
    # Prevent memory exhaustion by capping tracking cache size (evict oldest instead of clearing all)
    if len(limiters) > 1000:
        oldest_ips = sorted(limiters.keys(), key=lambda ip: limiters[ip]["window_start"])[:200]
        for ip in oldest_ips:
            limiters.pop(ip, None)
        
    now = time.time()
    if remote_ip not in limiters:
        limiters[remote_ip] = {"count": 0, "window_start": now}
        
    limiter = limiters[remote_ip]
    if now - limiter["window_start"] > 1.0:
        limiter["count"] = 0
        limiter["window_start"] = now
        
    limiter["count"] += 1
    if limiter["count"] > _HEALTH_MAX_REQUESTS_PER_SEC:
        return web.Response(status=429, text="Too Many Requests")
        
    return web.json_response(
        {"status": "ok", "service": "kalshi-quant-engine"},
        dumps=lambda x: orjson.dumps(x).decode('utf-8')
    )
