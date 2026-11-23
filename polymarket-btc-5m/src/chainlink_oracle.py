"""
Dedicated Chainlink BTC/USD Oracle Engine for Polymarket 5m Markets.

Fixes real-world production failure modes:
1. Never freezes on a stale price if public RPCs rate-limit (HTTP 429): uses a persistent
   `requests.Session` Round-Robin pool across 8 public RPCs spanning BOTH Arbitrum One
   (`0x6ce185860a4963106506C203335A2910413708e9`, tight L2 deviation threshold) AND
   Polygon Mainnet (`0xc907E116054Ad103354f2D350FD2514433D57F6f`), always selecting the
   Chainlink report with the newest on-chain `updatedAt` timestamp!
2. Strict Stale-Feed Circuit Breaker: if no live poll/stream succeeds within `max_stale_sec` (15s),
   `settlement_reference_price` returns `None` instead of repeating a frozen price.
3. Supports all 4 Chainlink tiers seamlessly even if `websockets` is not yet installed.
"""
import asyncio
from collections import deque
from dataclasses import dataclass
import hashlib
import hmac
import json
import os
import sys
import time
from typing import Any, Callable, Deque, List, Optional, Tuple
import urllib.parse

import requests


CHAINLINK_BTC_USD_FEED_ID = "0x00039d9e45394f473ab1f050a1b963e6b05351e52d71e507509ada0c95ed75b8"
CHAINLINK_DATAENGINE_HOST = "ws.dataengine.chain.link"

POLYBOLT_WS_URL = "wss://ws-live-v2.polymarket.com/ws"
POLY_RTDS_WS_URL = "wss://ws-live-data.polymarket.com"
STREAMING_ORACLE_SOURCES = ("CHAINLINK_DATA_STREAMS", "POLYBOLT_CHAINLINK", "POLY_RTDS_CHAINLINK")
PRIMARY_ORACLE_SOURCES = ("CHAINLINK_DATA_STREAMS", "POLYBOLT_CHAINLINK")
BINANCE_TIME_URLS = ("https://api.binance.com/api/v3/time", "https://data-api.binance.vision/api/v3/time")

POLYGON_CHAINLINK_BTC_USD = "0xc907E116054Ad103354f2D350FD2514433D57F6f"
ARBITRUM_CHAINLINK_BTC_USD = "0x6ce185860a4963106506C203335A2910413708e9"
LATEST_ROUND_DATA_SELECTOR = "0xfeaf968c"

# (chain_label, rpc_url, contract_address) — Round-robin pool prevents HTTP 429 bans!
ONCHAIN_CHAINLINK_TARGETS: List[Tuple[str, str, str]] = [
    ("ARB_CHAINLINK", "https://arbitrum-one-rpc.publicnode.com", ARBITRUM_CHAINLINK_BTC_USD),
    ("POLY_CHAINLINK", "https://polygon-bor-rpc.publicnode.com", POLYGON_CHAINLINK_BTC_USD),
    ("ARB_CHAINLINK", "https://arb1.arbitrum.io/rpc", ARBITRUM_CHAINLINK_BTC_USD),
    ("POLY_CHAINLINK", "https://polygon.drpc.org", POLYGON_CHAINLINK_BTC_USD),
    ("ARB_CHAINLINK", "https://arbitrum.drpc.org", ARBITRUM_CHAINLINK_BTC_USD),
    ("POLY_CHAINLINK", "https://1rpc.io/matic", POLYGON_CHAINLINK_BTC_USD),
    ("ARB_CHAINLINK", "https://1rpc.io/arb", ARBITRUM_CHAINLINK_BTC_USD),
    ("POLY_CHAINLINK", "https://polygon-rpc.com", POLYGON_CHAINLINK_BTC_USD),
]


def has_websockets_installed() -> bool:
    try:
        import websockets  # noqa: F401
        return True
    except ImportError:
        return False


def normalize_btc_usd_price(raw_value: Any) -> Optional[float]:
    try:
        val = float(raw_value)
    except (TypeError, ValueError):
        return None

    if val <= 0:
        return None
    if val > 1e19:
        val /= 1e18
    elif val > 1e9:
        val /= 1e8

    if 1000.0 <= val <= 2_000_000.0:
        return val
    return None


def decode_aggregator_v3_hex(hex_result: str) -> Optional[Tuple[int, float, int]]:
    """
    Decode ABI return payload of `latestRoundData()`:
      (uint80 roundId, int256 answer, uint256 startedAt, uint256 updatedAt, uint80 answeredInRound)
    Returns (round_id, btc_usd_price, updated_at_ts) or None.
    """
    if not hex_result or not hex_result.startswith("0x"):
        return None
    raw = hex_result[2:]
    if len(raw) < 64 * 5:
        return None

    round_id = int(raw[0:64], 16)
    answer_uint = int(raw[64:128], 16)
    if answer_uint >= (1 << 255):
        answer_uint -= (1 << 256)
    updated_at = int(raw[192:256], 16)

    price = normalize_btc_usd_price(answer_uint)
    if price is None or updated_at <= 0:
        return None
    return round_id, price, updated_at


def decode_chainlink_report_v3_hex(full_report_hex: str) -> Optional[Tuple[float, int]]:
    clean = full_report_hex[2:] if full_report_hex.startswith("0x") else full_report_hex
    if len(clean) < 64 * 12:
        return None
    try:
        data = bytes.fromhex(clean)
        blob_offset = int.from_bytes(data[96:128], "big")
        if blob_offset + 32 + 288 > len(data):
            return None
        blob_len = int.from_bytes(data[blob_offset : blob_offset + 32], "big")
        if blob_len < 288:
            return None
        blob = data[blob_offset + 32 : blob_offset + 32 + blob_len]

        obs_ts = int.from_bytes(blob[64:96], "big")
        raw_price = int.from_bytes(blob[192:224], "big", signed=True)
        price = normalize_btc_usd_price(raw_price)
        if price is None:
            return None
        return price, obs_ts
    except Exception:
        return None


def build_data_streams_hmac_headers(
    method: str,
    api_key: str,
    api_secret: str,
    full_path_with_query: str,
    body: bytes = b"",
    timestamp_ms: Optional[int] = None,
) -> dict:
    ts_str = str(timestamp_ms if timestamp_ms is not None else int(time.time() * 1000))
    body_hash_hex = hashlib.sha256(body).hexdigest()
    to_sign = f"{method} {full_path_with_query} {body_hash_hex} {api_key} {ts_str}".encode("utf-8")
    sig = hmac.new(api_secret.encode("utf-8"), to_sign, hashlib.sha256).hexdigest()
    return {
        "Authorization": api_key,
        "X-Authorization-Timestamp": ts_str,
        "X-Authorization-Signature-SHA256": sig,
    }


def _parse_ts_sec(raw_ts: Any) -> Optional[float]:
    """Parse epoch timestamp in milliseconds or seconds into UTC seconds."""
    if raw_ts is None:
        return None
    try:
        val = float(raw_ts)
    except (TypeError, ValueError):
        return None
    if val > 1e11:
        return val / 1000.0
    if val > 1e9:
        return val
    return None


@dataclass
class ChainlinkOracleState:
    price: Optional[float] = None
    twap_60s: Optional[float] = None
    official_twap_60s: Optional[float] = None
    official_twap_ts: float = 0.0
    official_twap_recv_ts: float = 0.0
    official_twap_source: str = ""
    oracle_ts: float = 0.0          # On-chain `updatedAt` or stream observation timestamp
    local_recv_ts: float = 0.0      # Wall-clock time of last verified live response
    source: str = "UNINITIALIZED"
    round_id: Optional[int] = None
    onchain_display_price: Optional[float] = None
    onchain_display_source: str = ""
    onchain_display_ts: float = 0.0
    clock_offset_sec: float = 0.0

    def is_alive(self, max_silence_sec: float = 4.5, max_oracle_age_sec: Optional[float] = None) -> bool:
        """True only for fresh streaming Chainlink sources (on-chain RPCs never feed live orders)."""
        if self.price is None or self.local_recv_ts <= 0:
            return False
        if self.source not in STREAMING_ORACLE_SOURCES:
            return False
        if (time.time() - self.local_recv_ts) > max_silence_sec:
            return False
        eff_oracle_age = (
            max_oracle_age_sec
            if max_oracle_age_sec is not None
            else (5.0 if self.source == "POLY_RTDS_CHAINLINK" else 4.0)
        )
        now_utc = time.time() + self.clock_offset_sec
        if self.oracle_ts > 0.0 and (now_utc - self.oracle_ts) > eff_oracle_age:
            return False
        return True

    @property
    def settlement_reference_price(self) -> Optional[float]:
        """Return Chainlink price ONLY if the oracle connection is alive (never return frozen data!)."""
        if not self.is_alive():
            return None
        return self.price


class ChainlinkOracleFeed:
    """
    Multi-tier Chainlink BTC/USD live oracle manager with Round-Robin RPC anti-ban protection.
    """

    def __init__(
        self,
        on_tick_callback: Optional[Callable[[], None]] = None,
        on_price_callback: Optional[Callable[[float, float], None]] = None,
    ) -> None:
        self.state = ChainlinkOracleState()
        self.on_tick_callback: Optional[Callable[[], None]] = on_tick_callback
        self.on_price_callback: Optional[Callable[[float, float], None]] = on_price_callback
        self._twap_samples: Deque[Tuple[float, float]] = deque(maxlen=600)
        self._utc_history: Deque[Tuple[float, float]] = deque(maxlen=360)  # (oracle_utc_ts, price)
        self._utc_twap_history: Deque[Tuple[float, float]] = deque(maxlen=360)
        self._official_twap_history: Deque[Tuple[float, float]] = deque(maxlen=900)
        self.clock_offset_sec: float = 0.0
        self.raw_clock_offset_ms: float = 0.0
        self._precise_clock_synced_ts: float = 0.0
        self._clock_samples: Deque[float] = deque(maxlen=15)
        self._consecutive_clock_warn_count: int = 0
        self.rtds_reconnects: int = 0
        self.polybolt_reconnects: int = 0
        self._stop_event = asyncio.Event()
        self._http = requests.Session()
        self._rr_idx = 0

    @property
    def effective_clock_offset_ms(self) -> float:
        """Residual clock skew remaining after `clock_offset_sec` correction (in milliseconds)."""
        return round(self.raw_clock_offset_ms - self.clock_offset_sec * 1000.0, 1)

    def update_clock_offset_precise(self, offset_sec: float) -> float:
        """Apply RTT-compensated millisecond server clock offset with a +/-50ms deadband."""
        if abs(offset_sec) > 86400.0:
            return self.raw_clock_offset_ms
        self.raw_clock_offset_ms = round(offset_sec * 1000.0, 1)
        if abs(self.raw_clock_offset_ms) > 250.0:
            self._consecutive_clock_warn_count += 1
        else:
            self._consecutive_clock_warn_count = 0
        self.clock_offset_sec = offset_sec if abs(offset_sec) >= 0.05 else 0.0
        self.state.clock_offset_sec = self.clock_offset_sec
        self._precise_clock_synced_ts = time.time()
        return self.raw_clock_offset_ms

    def sync_clock_precise(
        self,
        session: Optional[requests.Session] = None,
        samples: int = 3,
        max_rtt_sec: float = 0.6,
    ) -> Optional[float]:
        """Measure UTC offset against Binance `/api/v3/time` (ms resolution) rejecting congested RTT probes."""
        http = session or self._http
        offs: List[float] = []
        n_probes = max(1, int(samples))
        for idx in range(n_probes):
            url = BINANCE_TIME_URLS[idx % len(BINANCE_TIME_URLS)]
            try:
                t0 = time.time()
                resp = http.get(url, timeout=2.0)
                t1 = time.time()
                if resp.status_code != 200:
                    continue
                rtt = max(0.0, t1 - t0)
                if rtt > max_rtt_sec:
                    continue
                srv_ms = float(resp.json()["serverTime"])
                offs.append((srv_ms / 1000.0) - (t0 + 0.5 * rtt))
            except Exception:
                continue
        if not offs:
            return None
        offs.sort()
        return self.update_clock_offset_precise(offs[len(offs) // 2])

    def _is_primary_streaming_healthy(self, now_ts: float, max_age_sec: float = 3.0) -> bool:
        """Return True when primary high-frequency Chainlink stream ticked recently."""
        return (
            self.state.source in PRIMARY_ORACLE_SOURCES
            and self.state.local_recv_ts > 0.0
            and (now_ts - self.state.local_recv_ts) < max_age_sec
        )

    def _rtds_zombie_timeout_sec(self, now_ts: float) -> float:
        """Relax standby RTDS silence threshold to 15s while primary stream is healthy."""
        return 15.0 if self._is_primary_streaming_healthy(now_ts) else 3.5

    @staticmethod
    def _rtds_reconnect_delay_sec(consec_failures: int, primary_healthy: bool) -> float:
        """Compute bounded exponential backoff for RTDS reconnects."""
        step = max(0, int(consec_failures) - 1)
        if primary_healthy:
            return min(15.0, round(2.0 * (2 ** min(step, 3)), 2))
        return min(5.0, round(1.0 * (1.5 ** min(step, 4)), 2))

    def update_clock_offset(self, server_utc_ts: float, local_ts: Optional[float] = None) -> None:
        """
        Align local PC clock (`time.time()`) with authoritative real-time server UTC timestamps.
        Uses a +/-1.0s deadband for coarse 1-second HTTP Date / stream event headers without
        overwriting a recent millisecond-precise Binance time sync!
        """
        if self._precise_clock_synced_ts > 0.0 and (time.time() - self._precise_clock_synced_ts) < 900.0:
            return
        now_local = local_ts if local_ts is not None else time.time()
        diff = server_utc_ts - now_local
        if abs(diff) > 86400.0:
            return
        self._clock_samples.append(diff)
        ordered = sorted(self._clock_samples)
        med = ordered[len(ordered) // 2]
        self.clock_offset_sec = med if abs(med) >= 1.0 else 0.0
        self.state.clock_offset_sec = self.clock_offset_sec
        self.raw_clock_offset_ms = round(med * 1000.0, 1)

    def utc_now(self) -> float:
        return time.time() + self.clock_offset_sec

    def get_price_near_utc(self, target_utc_ts: float, max_diff_sec: float = 6.0) -> Optional[float]:
        """Return the Chainlink price whose authoritative `oracle_ts` is closest to `target_utc_ts`."""
        best_p: Optional[float] = None
        best_dt = max_diff_sec
        for ots, p in self._utc_history:
            dt = abs(ots - target_utc_ts)
            if dt <= best_dt:
                best_dt = dt
                best_p = p
        return best_p

    def get_twap_near_utc(self, target_utc_ts: float, max_diff_sec: float = 6.0) -> Optional[float]:
        """Return the Chainlink 60s TWAP whose authoritative `oracle_ts` is closest to `target_utc_ts`."""
        best_p: Optional[float] = None
        best_dt = max_diff_sec
        for ots, p in self._utc_twap_history:
            dt = abs(ots - target_utc_ts)
            if dt <= best_dt:
                best_dt = dt
                best_p = p
        return best_p

    def on_official_twap_tick(
        self,
        price: float,
        oracle_ts: float,
        source: str = "OFFICIAL_TWAP60",
    ) -> None:
        """Record an official Chainlink 60s TWAP tick with its authoritative observation timestamp."""
        if price <= 0.0 or oracle_ts <= 0.0:
            return
        ots = float(oracle_ts)
        val = float(price)
        self._official_twap_history.append((ots, val))
        if ots >= self.state.official_twap_ts:
            self.state.official_twap_60s = val
            self.state.official_twap_ts = ots
            self.state.official_twap_recv_ts = time.time()
            self.state.official_twap_source = source
            self.state.twap_60s = val

    def get_twap_at_or_after_utc(self, window_ts: float, max_after_sec: float = 5.0) -> Optional[float]:
        """Return the earliest official Chainlink 60s TWAP tick at or after `window_ts` within `max_after_sec`."""
        upper = window_ts + max(0.0, float(max_after_sec))
        best_ots: Optional[float] = None
        best_p: Optional[float] = None
        for ots, p in self._official_twap_history:
            if window_ts <= ots <= upper:
                if best_ots is None or ots < best_ots:
                    best_ots = ots
                    best_p = p
        return best_p

    def on_chainlink_tick(
        self,
        price: float,
        source: str,
        oracle_ts: Optional[float] = None,
        round_id: Optional[int] = None,
        explicit_twap_60s: Optional[float] = None,
    ) -> None:
        now = time.time()
        eff_oracle_ts = oracle_ts if oracle_ts is not None else self.utc_now()

        # On-chain aggregator rounds lag real-time streams and never trigger or price live orders.
        is_streaming = source in STREAMING_ORACLE_SOURCES
        if not is_streaming:
            if self.state.onchain_display_ts <= 0.0 or eff_oracle_ts >= self.state.onchain_display_ts:
                self.state.onchain_display_price = price
                self.state.onchain_display_source = source
                self.state.onchain_display_ts = eff_oracle_ts
            return

        # Do not overwrite a newer streaming Chainlink tick with an older lagging one!
        if self.state.oracle_ts > 0 and eff_oracle_ts < self.state.oracle_ts - 10.0:
            return

        is_monotonic_spot = eff_oracle_ts >= self.state.oracle_ts
        self.state.price = price
        self.state.source = source
        self.state.oracle_ts = max(self.state.oracle_ts, eff_oracle_ts)
        self.state.local_recv_ts = now
        if round_id is not None:
            self.state.round_id = round_id

        self._utc_history.append((eff_oracle_ts, price))
        if self.on_price_callback is not None and is_monotonic_spot:
            try:
                self.on_price_callback(eff_oracle_ts, price)
            except Exception as exc:
                sys.stderr.write(f"[ORACLE VOL WARN] {exc}\n")
        self._twap_samples.append((now, price))
        cutoff = now - 60.0
        while len(self._twap_samples) > 2 and self._twap_samples[0][0] < cutoff:
            self._twap_samples.popleft()

        has_fresh_official = (
            self.state.official_twap_60s is not None
            and self.state.official_twap_recv_ts > 0.0
            and (now - self.state.official_twap_recv_ts) < 10.0
        )
        if explicit_twap_60s is not None:
            self.state.twap_60s = explicit_twap_60s
        elif has_fresh_official:
            self.state.twap_60s = self.state.official_twap_60s
        else:
            self.state.twap_60s = self._compute_local_twap(now)
        if self.state.twap_60s and self.state.twap_60s > 0:
            self._utc_twap_history.append((eff_oracle_ts, float(self.state.twap_60s)))
        if self.on_tick_callback is not None:
            try:
                self.on_tick_callback()
            except Exception as exc:
                sys.stderr.write(f"[ORACLE WAKE WARN] {exc}\n")

    def _compute_local_twap(self, now: float) -> float:
        pts = list(self._twap_samples)
        if not pts:
            return 0.0
        if len(pts) == 1:
            return pts[0][1]

        weighted_sum = 0.0
        total_dt = 0.0
        start_cut = now - 60.0
        for i in range(len(pts)):
            t0 = max(start_cut, pts[i][0])
            t1 = pts[i + 1][0] if (i + 1 < len(pts)) else now
            dt = max(0.0, t1 - t0)
            weighted_sum += pts[i][1] * dt
            total_dt += dt

        return (weighted_sum / total_dt) if total_dt > 1e-6 else pts[-1][1]

    def _poll_round_robin_onchain(self) -> Optional[Tuple[str, int, float, int]]:
        """
        Query 2 distinct Chainlink targets (1 Arbitrum + 1 Polygon) per cycle in round-robin order
        and return the freshest Chainlink `updatedAt` round.
        """
        targets = list(ONCHAIN_CHAINLINK_TARGETS)
        custom_rpc = os.getenv("POLYGON_RPC_URL")
        if custom_rpc:
            targets.insert(0, ("POLY_CUSTOM_RPC", custom_rpc, POLYGON_CHAINLINK_BTC_USD))

        best: Optional[Tuple[str, int, float, int]] = None
        # Probe up to 3 endpoints starting at current round-robin cursor
        for _ in range(3):
            label, url, contract = targets[self._rr_idx % len(targets)]
            self._rr_idx = (self._rr_idx + 1) % len(targets)
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_call",
                "params": [{"to": contract, "data": LATEST_ROUND_DATA_SELECTOR}, "latest"],
            }
            try:
                resp = self._http.post(url, json=payload, timeout=2.5)
                if resp.status_code != 200:
                    continue
                decoded = decode_aggregator_v3_hex(resp.json().get("result") or "")
                if decoded is None:
                    continue
                round_id, price, updated_at = decoded
                # Reject stale cached RPC nodes (> 2 hours old)
                if (time.time() - updated_at) > 7200:
                    continue
                if best is None or updated_at >= best[3]:
                    best = (label, round_id, price, updated_at)
                # Once we got a fresh Arbitrum + Polygon pair or very recent update (<5s), stop probing
                if (time.time() - updated_at) < 5.0:
                    break
            except Exception:
                continue
        return best

    def stop(self) -> None:
        self._stop_event.set()

    async def start(self) -> None:
        tasks = [self._onchain_round_robin_loop()]
        if has_websockets_installed():
            tasks.append(self._rtds_chainlink_ws_loop(slot=0))
            tasks.append(self._rtds_chainlink_ws_loop(slot=1))
            cl_key = os.getenv("CHAINLINK_STREAMS_API_KEY")
            cl_secret = os.getenv("CHAINLINK_STREAMS_API_SECRET")
            if cl_key and cl_secret:
                tasks.append(self._chainlink_data_streams_ws_loop(cl_key, cl_secret))

            poly_key = os.getenv("POLY_API_KEY")
            poly_secret = os.getenv("POLY_API_SECRET")
            poly_pass = os.getenv("POLY_PASSPHRASE")
            if poly_key and poly_secret and poly_pass:
                tasks.append(self._polybolt_v2_ws_loop(poly_key, poly_secret, poly_pass))

        await asyncio.gather(*tasks, return_exceptions=True)

    async def _chainlink_data_streams_ws_loop(self, api_key: str, api_secret: str) -> None:
        import websockets

        query = urllib.parse.urlencode({"feedIDs": CHAINLINK_BTC_USD_FEED_ID})
        full_path = f"/api/v1/ws?{query}"
        url = f"wss://{CHAINLINK_DATAENGINE_HOST}{full_path}"

        while not self._stop_event.is_set():
            try:
                headers = build_data_streams_hmac_headers("GET", api_key, api_secret, full_path)
                async with websockets.connect(url, additional_headers=headers, ping_interval=20) as ws:
                    async for raw in ws:
                        if self._stop_event.is_set():
                            break
                        msg = json.loads(raw)
                        report_hex = (msg.get("report") or {}).get("fullReport") or msg.get("fullReport")
                        if report_hex:
                            decoded = decode_chainlink_report_v3_hex(report_hex)
                            if decoded:
                                price, obs_ts = decoded
                                self.on_chainlink_tick(price, "CHAINLINK_DATA_STREAMS", oracle_ts=float(obs_ts))
            except Exception:
                await asyncio.sleep(2.5)

    def _ingest_spot_snapshot(
        self,
        items: list[Any],
        source: str,
        normalizer: Callable[[Any], Optional[float]],
    ) -> bool:
        """Chronologically replay streaming Chainlink spot snapshots for 1s volatility warmup."""
        parsed: list[Tuple[float, float]] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            p_item = normalizer(item.get("value") or item.get("price"))
            ots_item = _parse_ts_sec(item.get("timestamp"))
            if p_item is not None and p_item > 0.0 and ots_item is not None and ots_item > 0.0:
                parsed.append((ots_item, p_item))
        if not parsed:
            return False
        parsed.sort(key=lambda pair: pair[0])
        for ots_item, p_item in parsed[:-1]:
            self._utc_history.append((ots_item, p_item))
            if self.on_price_callback is not None and ots_item >= self.state.oracle_ts:
                try:
                    self.on_price_callback(ots_item, p_item)
                except Exception as exc:
                    sys.stderr.write(f"[ORACLE VOL WARN] {exc}\n")
        last_ots, last_px = parsed[-1]
        self.on_chainlink_tick(last_px, source, oracle_ts=last_ots)
        return True

    def _handle_polybolt_message(self, msg: dict[str, Any]) -> None:
        """Decode PolyBolt spot or official 60s TWAP snapshots and ticks."""
        ch = msg.get("channel", "")
        data = msg.get("payload") or msg.get("data") or {}
        if not isinstance(data, dict):
            return
        if isinstance(data.get("data"), list):
            if ch == "price.crypto.twap":
                for item in data["data"]:
                    if not isinstance(item, dict):
                        continue
                    p_item = normalize_btc_usd_price(item.get("value") or item.get("price"))
                    ots_item = _parse_ts_sec(item.get("timestamp"))
                    if p_item is not None and ots_item is not None:
                        self.on_official_twap_tick(p_item, ots_item, "POLYBOLT_TWAP60")
                return
            if ch == "price.crypto" and data["data"]:
                self._ingest_spot_snapshot(data["data"], "POLYBOLT_CHAINLINK", normalize_btc_usd_price)
            return
        norm_p = normalize_btc_usd_price(data.get("price") or data.get("value"))
        if norm_p is None:
            return
        ots = _parse_ts_sec(data.get("timestamp") or msg.get("ts"))
        if ch == "price.crypto.twap":
            self.on_official_twap_tick(norm_p, ots if ots is not None else self.utc_now(), "POLYBOLT_TWAP60")
        elif ch in ("price.crypto", ""):
            self.on_chainlink_tick(norm_p, "POLYBOLT_CHAINLINK", oracle_ts=ots)

    def _handle_rtds_twap_payload(self, payload: dict[str, Any]) -> bool:
        """Decode RTDS `crypto_prices_twap_sixty` snapshots and ticks; return True when valid."""
        sym = str(payload.get("symbol", "")).lower().replace("-", "/")
        if sym and "btc" not in sym:
            return False
        if isinstance(payload.get("data"), list):
            any_tick = False
            for item in payload["data"]:
                if not isinstance(item, dict):
                    continue
                tw_p = normalize_btc_usd_price(item.get("value") or item.get("price"))
                tw_ots = _parse_ts_sec(item.get("timestamp"))
                if tw_p is not None and tw_ots is not None:
                    self.on_official_twap_tick(tw_p, tw_ots, "RTDS_TWAP60")
                    any_tick = True
            return any_tick
        tw_p = normalize_btc_usd_price(payload.get("value") or payload.get("price"))
        if tw_p is None:
            return False
        tw_ots = _parse_ts_sec(payload.get("timestamp")) or self.utc_now()
        self.on_official_twap_tick(tw_p, tw_ots, "RTDS_TWAP60")
        return True

    async def _polybolt_v2_ws_loop(self, api_key: str, secret: str, passphrase: str) -> None:
        import websockets

        while not self._stop_event.is_set():
            try:
                async with websockets.connect(
                    POLYBOLT_WS_URL,
                    ping_interval=15,
                    ping_timeout=10,
                    open_timeout=8,
                    close_timeout=2,
                ) as ws:
                    await ws.send(
                        json.dumps(
                            {
                                "op": "auth",
                                "rid": "auth-cl-1",
                                "auth": {"apiKey": api_key, "secret": secret, "passphrase": passphrase},
                            }
                        )
                    )
                    await ws.send(
                        json.dumps(
                            {
                                "op": "subscribe",
                                "subscriptions": [
                                    {"channel": "price.crypto", "filter": {"symbol": "btcusd"}},
                                    {"channel": "price.crypto.twap", "filter": {"symbol": "btcusd", "window_seconds": 60}},
                                ],
                            }
                        )
                    )
                    while not self._stop_event.is_set():
                        raw = await asyncio.wait_for(ws.recv(), timeout=12.0)
                        self._handle_polybolt_message(json.loads(raw))
                if not self._stop_event.is_set():
                    self.polybolt_reconnects += 1
                    await asyncio.sleep(1.5)
            except Exception:
                if not self._stop_event.is_set():
                    self.polybolt_reconnects += 1
                await asyncio.sleep(2.5)

    async def _rtds_chainlink_ws_loop(self, slot: int = 0) -> None:
        import websockets

        if slot > 0:
            await asyncio.sleep(1.5 * slot)

        # Subscribe strictly to BTC/USD Chainlink Spot + official 60s TWAP streams
        sub_msg = {
            "action": "subscribe",
            "subscriptions": [
                {"topic": "crypto_prices_chainlink", "type": "*", "filters": '{"symbol":"btc/usd"}'},
                {"topic": "crypto_prices_twap_sixty", "type": "*", "filters": '{"symbol":"btc/usd"}'},
            ],
        }

        rtds_consec_fails = 0
        while not self._stop_event.is_set():
            try:
                async with websockets.connect(
                    POLY_RTDS_WS_URL,
                    ping_interval=15,
                    ping_timeout=10,
                    open_timeout=8,
                    close_timeout=2,
                ) as ws:
                    await ws.send(json.dumps(sub_msg))
                    last_ping = time.time()
                    last_btc_tick_ts = last_ping
                    last_resub_ts = last_ping

                    while not self._stop_event.is_set():
                        now_loop = time.time()
                        # Break out of silent/zombie sessions (e.g. RTDS drops topic push while still replying PONG)
                        if now_loop - last_btc_tick_ts >= self._rtds_zombie_timeout_sec(now_loop):
                            break
                        if now_loop - last_btc_tick_ts >= 2.0 and now_loop - last_resub_ts >= 2.0:
                            await ws.send(json.dumps(sub_msg))
                            last_resub_ts = now_loop
                        if now_loop - last_ping >= 5.0:
                            await ws.send("PING")
                            last_ping = now_loop
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=1.5)
                        except asyncio.TimeoutError:
                            continue

                        if raw in ("PONG", "pong", ""):
                            continue
                        msg = json.loads(raw)
                        topic = msg.get("topic")
                        if topic == "crypto_prices_twap_sixty":
                            if self._handle_rtds_twap_payload(msg.get("payload") or {}):
                                rtds_consec_fails = 0
                            continue
                        if topic != "crypto_prices_chainlink":
                            continue
                        payload = msg.get("payload") or {}
                        sym = str(payload.get("symbol", "")).lower().replace("-", "/")
                        if "btc" not in sym:
                            continue

                        last_btc_tick_ts = time.time()
                        rtds_consec_fails = 0
                        if (
                            self.state.source in ("CHAINLINK_DATA_STREAMS", "POLYBOLT_CHAINLINK")
                            and (time.time() - self.state.local_recv_ts) < 2.0
                        ):
                            continue

                        if isinstance(payload.get("data"), list):
                            if self._ingest_spot_snapshot(
                                payload["data"], "POLY_RTDS_CHAINLINK", normalize_btc_usd_price
                            ):
                                last_btc_tick_ts = time.time()
                            continue

                        price = normalize_btc_usd_price(payload.get("value") or payload.get("price"))
                        if price is not None:
                            last_btc_tick_ts = time.time()
                            ts_raw = payload.get("timestamp")
                            # Note: RTDS `timestamp` is Chainlink's report observation time (often 2-5s old),
                            # NOT packet send time — use it for `oracle_ts`, never for wall-clock sync!
                            obs_ts = (float(ts_raw) / 1000.0) if (ts_raw and float(ts_raw) > 1e11) else self.utc_now()
                            round_id = payload.get("round_id")
                            self.on_chainlink_tick(
                                price=price,
                                source="POLY_RTDS_CHAINLINK",
                                oracle_ts=obs_ts,
                                round_id=int(round_id) if round_id else None,
                            )
                if not self._stop_event.is_set():
                    self.rtds_reconnects += 1
                    rtds_consec_fails += 1
                    await asyncio.sleep(
                        self._rtds_reconnect_delay_sec(
                            rtds_consec_fails,
                            self._is_primary_streaming_healthy(time.time()),
                        )
                    )
            except Exception:
                if not self._stop_event.is_set():
                    self.rtds_reconnects += 1
                    rtds_consec_fails += 1
                    await asyncio.sleep(
                        self._rtds_reconnect_delay_sec(
                            rtds_consec_fails,
                            self._is_primary_streaming_healthy(time.time()),
                        )
                    )

    async def _onchain_round_robin_loop(self) -> None:
        """
        Polls Arbitrum + Polygon Chainlink contracts across 8 RPCs in round-robin rotation
        strictly for display/diagnostic comparison (NEVER feeds live entry/exit signals!).
        """
        loop = asyncio.get_running_loop()
        while not self._stop_event.is_set():
            if not self.state.is_alive(max_silence_sec=4.5):
                res = await loop.run_in_executor(None, self._poll_round_robin_onchain)
                if res is not None:
                    label, round_id, price, updated_at = res
                    self.on_chainlink_tick(
                        price=price,
                        source=label,
                        oracle_ts=float(updated_at),
                        round_id=round_id,
                    )
            await asyncio.sleep(15.0)
