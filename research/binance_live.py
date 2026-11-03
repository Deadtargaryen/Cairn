#!/usr/bin/env python3
"""Guarded Binance Spot live-data and execution primitives.

The module implements the documented Binance Spot REST and market-data
contracts without storing credentials in source code. Public observation is
available by default. Order placement requires all explicit safety gates:

* ``BINANCE_ORDER_MODE=live``
* ``BINANCE_ENABLE_LIVE_ORDERS=true``
* ``BINANCE_LIVE_CONFIRMATION=I_UNDERSTAND_LIVE_TRADING``
* a JSON gate file whose decision is ``ALLOW_LIVE_TRADING``
* no kill-switch file

The default mode is observation-only. A failed or stale market stream never
authorizes an order. HTTP 5xx responses for signed writes are treated as
unknown execution status and are never blindly retried.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import hashlib
import hmac
import json
import logging
import os
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from pathlib import Path
from typing import Any, Awaitable, Callable, Iterable, Mapping

try:  # Load project .env before reading configuration variables.
    from . import config as _config
except ImportError:  # pragma: no cover - direct script execution
    import config as _config  # type: ignore


LOGGER = logging.getLogger("binance_live")
UTC = dt.timezone.utc
REST_DEFAULT = "https://api.binance.com"
MARKET_WS_DEFAULT = "wss://stream.binance.com:9443/stream"
WS_API_DEFAULT = "wss://ws-api.binance.com:443/ws-api/v3"
LIVE_CONFIRMATION = "I_UNDERSTAND_LIVE_TRADING"


class BinanceError(RuntimeError):
    """An error returned by Binance or the network."""

    def __init__(self, message: str, *, status: int | None = None, code: int | None = None, payload: Any = None):
        super().__init__(message)
        self.status = status
        self.code = code
        self.payload = payload


class ExecutionStatusUnknown(BinanceError):
    """A signed write may have reached the matching engine; reconcile first."""


class SafetyBlocked(RuntimeError):
    """An explicit live-order safety gate is not satisfied."""


class BookResyncRequired(RuntimeError):
    """A depth sequence gap requires a fresh snapshot."""


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _decimal(value: Any, field: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(f"invalid decimal {field}: {value!r}") from exc
    if not result.is_finite():
        raise ValueError(f"non-finite decimal {field}: {value!r}")
    return result


def _floor_step(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        return value
    return (value / step).to_integral_value(rounding=ROUND_DOWN) * step


def _decimal_text(value: Decimal) -> str:
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _now_ms() -> int:
    return int(time.time() * 1000)


def _hmac_hex(secret: str, params: Mapping[str, Any]) -> str:
    # Binance signs the exact query-string representation sent with the
    # request. Preserve insertion order here; sorting would produce a valid
    # HMAC for a different payload than the one emitted on the wire.
    payload = urllib.parse.urlencode(
        [(key, str(value)) for key, value in params.items() if value is not None]
    )
    return hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()


def _encoded_params(params: Mapping[str, Any], *, signature_last: bool = False) -> str:
    """Encode parameters in the same order used for signing."""
    items = [(key, str(value)) for key, value in params.items() if value is not None]
    if signature_last:
        signature = [(key, value) for key, value in items if key == "signature"]
        items = [(key, value) for key, value in items if key != "signature"] + signature
    return urllib.parse.urlencode(items)


@dataclass(frozen=True)
class BinanceConfig:
    api_key: str | None = None
    api_secret: str | None = None
    rest_base_url: str = REST_DEFAULT
    market_ws_url: str = MARKET_WS_DEFAULT
    ws_api_url: str = WS_API_DEFAULT
    recv_window_ms: int = 5_000
    request_timeout_s: float = 10.0
    max_market_age_ms: int = 5_000
    kill_switch_path: Path = Path("research/runtime/KILL_SWITCH")
    live_gate_path: Path = Path("research/runtime/LIVE_GATE.json")
    order_mode: str = "observe"
    enable_live_orders: bool = False
    live_confirmation: str = ""
    require_withdrawals_disabled: bool = True

    @classmethod
    def from_env(cls) -> "BinanceConfig":
        return cls(
            api_key=os.environ.get("BINANCE_API_KEY"),
            api_secret=os.environ.get("BINANCE_API_SECRET"),
            rest_base_url=os.environ.get("BINANCE_REST_BASE_URL", REST_DEFAULT).rstrip("/"),
            market_ws_url=os.environ.get("BINANCE_MARKET_WS_URL", MARKET_WS_DEFAULT),
            ws_api_url=os.environ.get("BINANCE_WS_API_URL", WS_API_DEFAULT),
            recv_window_ms=int(os.environ.get("BINANCE_RECV_WINDOW_MS", "5000")),
            request_timeout_s=float(os.environ.get("BINANCE_REQUEST_TIMEOUT_S", "10")),
            max_market_age_ms=int(os.environ.get("BINANCE_MAX_MARKET_AGE_MS", "5000")),
            kill_switch_path=Path(os.environ.get("BINANCE_KILL_SWITCH_PATH", "research/runtime/KILL_SWITCH")),
            live_gate_path=Path(os.environ.get("BINANCE_LIVE_GATE_PATH", "research/runtime/LIVE_GATE.json")),
            order_mode=os.environ.get("BINANCE_ORDER_MODE", "observe").lower(),
            enable_live_orders=_env_bool("BINANCE_ENABLE_LIVE_ORDERS"),
            live_confirmation=os.environ.get("BINANCE_LIVE_CONFIRMATION", ""),
            require_withdrawals_disabled=not _env_bool("BINANCE_ALLOW_WITHDRAWALS"),
        )

    @property
    def has_credentials(self) -> bool:
        return bool(self.api_key and self.api_secret)


@dataclass(frozen=True)
class LiveSafety:
    config: BinanceConfig

    def kill_switch_active(self) -> bool:
        return self.config.kill_switch_path.exists()

    def gate_decision(self) -> str | None:
        try:
            payload = json.loads(self.config.live_gate_path.read_text(encoding="utf-8"))
        except (FileNotFoundError, OSError, json.JSONDecodeError):
            return None
        return payload.get("decision")

    def assert_authenticated(self) -> None:
        if not self.config.has_credentials:
            raise SafetyBlocked("BINANCE_API_KEY and BINANCE_API_SECRET are required")

    def assert_order_allowed(self) -> None:
        self.assert_authenticated()
        if self.config.order_mode != "live":
            raise SafetyBlocked("BINANCE_ORDER_MODE is not live")
        if not self.config.enable_live_orders:
            raise SafetyBlocked("BINANCE_ENABLE_LIVE_ORDERS is not true")
        if self.config.live_confirmation != LIVE_CONFIRMATION:
            raise SafetyBlocked("live confirmation phrase is missing or invalid")
        if self.kill_switch_active():
            raise SafetyBlocked(f"kill switch is active: {self.config.kill_switch_path}")
        if self.gate_decision() != "ALLOW_LIVE_TRADING":
            raise SafetyBlocked("live gate file does not authorize ALLOW_LIVE_TRADING")

    def assert_market_fresh(self, state: "MarketState") -> None:
        # A trade or ticker event can continue arriving while depth updates
        # are stalled. Live execution depends on a current sequence-checked
        # book, so freshness is measured from the latest depth event.
        if (
            state.stale
            or state.order_book.stale
            or not state.order_book.stream_synced
            or state.order_book.last_event_ms is None
        ):
            raise SafetyBlocked("market state is stale or not initialized")
        age = _now_ms() - state.order_book.last_event_ms
        if age > self.config.max_market_age_ms:
            raise SafetyBlocked(f"market state is {age}ms old")


@dataclass
class SymbolFilters:
    symbol: str
    base_asset: str
    quote_asset: str
    status: str
    order_types: tuple[str, ...]
    tick_size: Decimal = Decimal("0")
    min_price: Decimal = Decimal("0")
    max_price: Decimal = Decimal("0")
    step_size: Decimal = Decimal("0")
    min_qty: Decimal = Decimal("0")
    max_qty: Decimal = Decimal("0")
    market_step_size: Decimal = Decimal("0")
    market_min_qty: Decimal = Decimal("0")
    market_max_qty: Decimal = Decimal("0")
    min_notional: Decimal = Decimal("0")
    max_notional: Decimal = Decimal("0")
    apply_min_to_market: bool = False
    apply_max_to_market: bool = False

    @classmethod
    def from_exchange_info(cls, payload: Mapping[str, Any]) -> "SymbolFilters":
        filters = {item.get("filterType"): item for item in payload.get("filters", [])}
        price = filters.get("PRICE_FILTER", {})
        lot = filters.get("LOT_SIZE", {})
        market_lot = filters.get("MARKET_LOT_SIZE", {})
        min_notional = filters.get("MIN_NOTIONAL", {})
        notional = filters.get("NOTIONAL", {})
        return cls(
            symbol=str(payload["symbol"]),
            base_asset=str(payload.get("baseAsset", "")),
            quote_asset=str(payload.get("quoteAsset", "")),
            status=str(payload.get("status", "")),
            order_types=tuple(payload.get("orderTypes", [])),
            tick_size=_decimal(price.get("tickSize", "0"), "tickSize"),
            min_price=_decimal(price.get("minPrice", "0"), "minPrice"),
            max_price=_decimal(price.get("maxPrice", "0"), "maxPrice"),
            step_size=_decimal(lot.get("stepSize", "0"), "stepSize"),
            min_qty=_decimal(lot.get("minQty", "0"), "minQty"),
            max_qty=_decimal(lot.get("maxQty", "0"), "maxQty"),
            market_step_size=_decimal(market_lot.get("stepSize", "0"), "marketStepSize"),
            market_min_qty=_decimal(market_lot.get("minQty", "0"), "marketMinQty"),
            market_max_qty=_decimal(market_lot.get("maxQty", "0"), "marketMaxQty"),
            min_notional=_decimal(notional.get("minNotional", min_notional.get("minNotional", "0")), "minNotional"),
            max_notional=_decimal(notional.get("maxNotional", "0"), "maxNotional"),
            apply_min_to_market=bool(notional.get("applyMinToMarket", min_notional.get("applyToMarket", False))),
            apply_max_to_market=bool(notional.get("applyMaxToMarket", False)),
        )

    def normalize_price(self, price: Decimal) -> Decimal:
        return _floor_step(price, self.tick_size)

    def normalize_quantity(self, quantity: Decimal, market: bool = False) -> Decimal:
        return _floor_step(quantity, self.market_step_size if market else self.step_size)

    def validate(
        self,
        *,
        side: str,
        order_type: str,
        quantity: Decimal | None = None,
        quote_order_qty: Decimal | None = None,
        price: Decimal | None = None,
        reference_price: Decimal | None = None,
    ) -> None:
        side = side.upper()
        order_type = order_type.upper()
        if side not in {"BUY", "SELL"}:
            raise SafetyBlocked(f"unsupported order side: {side}")
        if quantity is not None and quantity <= 0:
            raise SafetyBlocked("quantity must be positive")
        if quote_order_qty is not None and quote_order_qty <= 0:
            raise SafetyBlocked("quote order quantity must be positive")
        if quantity is None and quote_order_qty is None:
            raise SafetyBlocked("quantity or quote order quantity is required")
        if quantity is not None and quote_order_qty is not None:
            raise SafetyBlocked("quantity and quote order quantity are mutually exclusive")
        if self.status != "TRADING":
            raise SafetyBlocked(f"{self.symbol} is not trading: {self.status}")
        if order_type not in self.order_types:
            raise SafetyBlocked(f"{order_type} is not allowed for {self.symbol}")
        market = order_type == "MARKET"
        if quantity is not None:
            if self.step_size > 0 and quantity % self.step_size != 0:
                raise SafetyBlocked("quantity does not satisfy exchange step size")
            if self.min_qty > 0 and quantity < self.min_qty:
                raise SafetyBlocked("quantity is below exchange minimum")
            if self.max_qty > 0 and quantity > self.max_qty:
                raise SafetyBlocked("quantity exceeds exchange maximum")
            if market:
                if self.market_step_size > 0 and quantity % self.market_step_size != 0:
                    raise SafetyBlocked("quantity does not satisfy market order step size")
                if self.market_min_qty > 0 and quantity < self.market_min_qty:
                    raise SafetyBlocked("quantity is below market order minimum")
                if self.market_max_qty > 0 and quantity > self.market_max_qty:
                    raise SafetyBlocked("quantity exceeds market order maximum")
        if price is not None:
            if self.tick_size > 0 and price % self.tick_size != 0:
                raise SafetyBlocked("price does not satisfy exchange tick size")
            if self.min_price > 0 and price < self.min_price:
                raise SafetyBlocked("price is below exchange minimum")
            if self.max_price > 0 and price > self.max_price:
                raise SafetyBlocked("price exceeds exchange maximum")
        if quote_order_qty is not None:
            if not market:
                raise SafetyBlocked("quote order quantity is supported only for market orders")
            if self.min_notional > 0 and self.apply_min_to_market and quote_order_qty < self.min_notional:
                raise SafetyBlocked("quote order quantity is below exchange minimum notional")
            if self.max_notional > 0 and self.apply_max_to_market and quote_order_qty > self.max_notional:
                raise SafetyBlocked("quote order quantity exceeds exchange maximum notional")
            return

        notional_price = price if price is not None else reference_price
        if notional_price is not None:
            notional = notional_price * quantity
            if self.min_notional > 0 and (not market or self.apply_min_to_market) and notional < self.min_notional:
                raise SafetyBlocked("order notional is below exchange minimum")
            if self.max_notional > 0 and (not market or self.apply_max_to_market) and notional > self.max_notional:
                raise SafetyBlocked("order notional exceeds exchange maximum")
        elif market and (self.apply_min_to_market or self.apply_max_to_market):
            raise SafetyBlocked("market order requires a current reference price for notional validation")


@dataclass(frozen=True)
class OrderIntent:
    symbol: str
    side: str
    order_type: str
    quantity: Decimal | None = None
    price: Decimal | None = None
    time_in_force: str | None = "GTC"
    client_order_id: str | None = None
    quote_order_qty: Decimal | None = None


class BinanceRestClient:
    """Small dependency-light Binance Spot REST client."""

    def __init__(self, config: BinanceConfig | None = None, *, opener: Any = None):
        self.config = config or BinanceConfig.from_env()
        self.safety = LiveSafety(self.config)
        self._opener = opener or urllib.request.urlopen
        self._server_offset_ms = 0
        self._filters: dict[str, SymbolFilters] = {}

    def _url(self, path: str) -> str:
        return f"{self.config.rest_base_url}{path}"

    def _signed_params(self, params: Mapping[str, Any] | None) -> dict[str, str]:
        self.safety.assert_authenticated()
        values = {key: str(value) for key, value in (params or {}).items() if value is not None}
        values.setdefault("recvWindow", str(self.config.recv_window_ms))
        values["timestamp"] = str(_now_ms() + self._server_offset_ms)
        values["signature"] = _hmac_hex(self.config.api_secret, values)
        return values

    def request(
        self,
        method: str,
        path: str,
        params: Mapping[str, Any] | None = None,
        *,
        signed: bool = False,
        write: bool = False,
    ) -> Any:
        values = self._signed_params(params) if signed else {
            key: str(value) for key, value in (params or {}).items() if value is not None
        }
        query = _encoded_params(values, signature_last=signed)
        url = self._url(path)
        if method.upper() in {"GET", "DELETE"} and query:
            url = f"{url}?{query}"
            body = None
        else:
            body = query.encode("utf-8") if query else None
        headers = {"Accept": "application/json", "User-Agent": "crypto-research-binance-live/1.0"}
        if self.config.api_key:
            headers["X-MBX-APIKEY"] = self.config.api_key
        request = urllib.request.Request(url, data=body, headers=headers, method=method.upper())
        try:
            with self._opener(request, timeout=self.config.request_timeout_s) as response:
                payload = json.loads(response.read().decode("utf-8"))
                if response.status >= 400:
                    raise BinanceError(
                        f"Binance HTTP {response.status}", status=response.status, payload=payload,
                    )
                return payload
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                payload = raw
            if write and exc.code >= 500:
                raise ExecutionStatusUnknown(
                    "signed write returned 5xx; reconcile order status before retrying",
                    status=exc.code,
                    payload=payload,
                ) from exc
            code = payload.get("code") if isinstance(payload, dict) else None
            message = payload.get("msg", raw) if isinstance(payload, dict) else raw
            raise BinanceError(message, status=exc.code, code=code, payload=payload) from exc
        except (TimeoutError, urllib.error.URLError) as exc:
            if write:
                raise ExecutionStatusUnknown(
                    "signed write transport status is unknown; reconcile before retrying"
                ) from exc
            raise BinanceError(f"Binance transport error: {exc}") from exc

    def server_time(self) -> int:
        payload = self.request("GET", "/api/v3/time")
        server = int(payload["serverTime"])
        self._server_offset_ms = server - _now_ms()
        return server

    def ping(self) -> dict[str, Any]:
        return self.request("GET", "/api/v3/ping")

    def exchange_info(self, symbol: str = "BTCUSDT") -> SymbolFilters:
        payload = self.request("GET", "/api/v3/exchangeInfo", {"symbol": symbol.upper()})
        symbols = payload.get("symbols", [])
        if len(symbols) != 1:
            raise BinanceError(f"expected one symbol in exchangeInfo, got {len(symbols)}")
        filters = SymbolFilters.from_exchange_info(symbols[0])
        self._filters[filters.symbol] = filters
        return filters

    def filters(self, symbol: str) -> SymbolFilters:
        return self._filters.get(symbol.upper()) or self.exchange_info(symbol)

    def depth(self, symbol: str = "BTCUSDT", limit: int = 5000) -> dict[str, Any]:
        return self.request("GET", "/api/v3/depth", {"symbol": symbol.upper(), "limit": limit})

    def book_ticker(self, symbol: str = "BTCUSDT") -> dict[str, Any]:
        return self.request("GET", "/api/v3/ticker/bookTicker", {"symbol": symbol.upper()})

    def trades(self, symbol: str = "BTCUSDT", limit: int = 500) -> list[dict[str, Any]]:
        return self.request("GET", "/api/v3/trades", {"symbol": symbol.upper(), "limit": limit})

    def aggregate_trades(self, symbol: str = "BTCUSDT", limit: int = 500) -> list[dict[str, Any]]:
        return self.request("GET", "/api/v3/aggTrades", {"symbol": symbol.upper(), "limit": limit})

    def klines(self, symbol: str = "BTCUSDT", interval: str = "1m", limit: int = 100) -> list[list[Any]]:
        return self.request("GET", "/api/v3/klines", {"symbol": symbol.upper(), "interval": interval, "limit": limit})

    def ticker_24h(self, symbol: str = "BTCUSDT") -> dict[str, Any]:
        return self.request("GET", "/api/v3/ticker/24hr", {"symbol": symbol.upper()})

    def account(self) -> dict[str, Any]:
        self.server_time()
        return self.request("GET", "/api/v3/account", signed=True)

    def assert_account_safety(self) -> dict[str, Any]:
        """Verify account permissions immediately before a live write."""
        account = self.account()
        if account.get("accountType") not in {None, "SPOT"}:
            raise SafetyBlocked(f"account is not a spot account: {account.get('accountType')}")
        if account.get("canTrade") is not True:
            raise SafetyBlocked("Binance account does not report canTrade=true")
        if self.config.require_withdrawals_disabled:
            # Binance account payloads may omit permission fields depending on
            # account/API version. Missing is fail-closed; explicitly enabled
            # withdrawal permission is never accepted for this runtime.
            if account.get("canWithdraw") is not False:
                raise SafetyBlocked("account does not explicitly report withdrawals disabled")
        permissions = account.get("permissions") or []
        if permissions and "SPOT" not in permissions:
            raise SafetyBlocked("account does not report SPOT permission")
        return account

    def open_orders(self, symbol: str | None = None) -> list[dict[str, Any]]:
        self.server_time()
        return self.request("GET", "/api/v3/openOrders", {"symbol": symbol.upper() if symbol else None}, signed=True)

    def get_order(self, symbol: str, *, order_id: int | None = None, client_order_id: str | None = None) -> dict[str, Any]:
        self.server_time()
        return self.request(
            "GET", "/api/v3/order", {"symbol": symbol.upper(), "orderId": order_id, "origClientOrderId": client_order_id}, signed=True
        )

    def wait_for_terminal_order(
        self,
        symbol: str,
        *,
        order_id: int | None = None,
        client_order_id: str | None = None,
        timeout_s: float = 10.0,
        poll_s: float = 0.25,
    ) -> dict[str, Any]:
        """Resolve an accepted order without ever submitting it again.

        A successful POST only means Binance accepted the request.  The
        matching-engine status is authoritative, especially after a network
        timeout or a partial fill.
        """
        terminal = {"FILLED", "CANCELED", "REJECTED", "EXPIRED", "EXPIRED_IN_MATCH"}
        deadline = time.monotonic() + max(0.1, timeout_s)
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            try:
                order = self.get_order(
                    symbol,
                    order_id=order_id,
                    client_order_id=client_order_id,
                )
                status = str(order.get("status", ""))
                if status in terminal:
                    return order
            except BinanceError as exc:
                # An order can take a short moment to become queryable. Keep
                # polling, but never reinterpret a missing order as a fill.
                last_error = exc
            time.sleep(max(0.05, poll_s))
        detail = "order did not reach a terminal status before timeout"
        if last_error is not None:
            detail = f"{detail}: {last_error}"
        raise ExecutionStatusUnknown(detail)

    def all_orders(self, symbol: str, limit: int = 1000) -> list[dict[str, Any]]:
        self.server_time()
        return self.request("GET", "/api/v3/allOrders", {"symbol": symbol.upper(), "limit": limit}, signed=True)

    def my_trades(self, symbol: str, limit: int = 1000) -> list[dict[str, Any]]:
        self.server_time()
        return self.request("GET", "/api/v3/myTrades", {"symbol": symbol.upper(), "limit": limit}, signed=True)

    def order_trades(self, symbol: str, order_id: int, limit: int = 1000) -> list[dict[str, Any]]:
        """Return fills for one order for authoritative fee/PnL reconciliation."""
        self.server_time()
        return self.request(
            "GET",
            "/api/v3/myTrades",
            {"symbol": symbol.upper(), "orderId": order_id, "limit": limit},
            signed=True,
        )

    def reconcile_order_fills(self, symbol: str, order: Mapping[str, Any]) -> dict[str, Any]:
        """Aggregate exchange fills and commissions without inventing prices."""
        order_id = order.get("orderId")
        if order_id is None:
            raise BinanceError("cannot reconcile an order without orderId", payload=order)
        fills = self.order_trades(symbol, int(order_id))
        executed_qty = sum(_decimal(fill.get("qty", "0"), "trade quantity") for fill in fills)
        quote_qty = sum(
            _decimal(fill.get("price", "0"), "trade price")
            * _decimal(fill.get("qty", "0"), "trade quantity")
            for fill in fills
        )
        commissions: dict[str, Decimal] = {}
        for fill in fills:
            asset = str(fill.get("commissionAsset", ""))
            if asset:
                commissions[asset] = commissions.get(asset, Decimal("0")) + _decimal(
                    fill.get("commission", "0"), "commission"
                )
        average_price = quote_qty / executed_qty if executed_qty else None
        filters = self.filters(symbol)
        quote_fee = commissions.get(filters.quote_asset, Decimal("0"))
        base_fee = commissions.get(filters.base_asset, Decimal("0"))
        if average_price is not None:
            quote_fee += base_fee * average_price
        unresolved: dict[str, Decimal] = {}
        for asset, commission in commissions.items():
            if asset in {filters.quote_asset, filters.base_asset}:
                continue
            if asset == "BNB":
                try:
                    conversion = self.book_ticker(f"BNB{filters.quote_asset}")
                    quote_fee += commission * _decimal(conversion["bidPrice"], "commission conversion price")
                    continue
                except (BinanceError, KeyError, ValueError):
                    pass
            unresolved[asset] = commission
        return {
            "order_id": int(order_id),
            "status": order.get("status"),
            "side": order.get("side"),
            "executed_qty": executed_qty,
            "quote_qty": quote_qty,
            "average_price": average_price,
            "commissions": commissions,
            "estimated_quote_fee": quote_fee,
            "unresolved_commissions": unresolved,
            "fills": fills,
        }

    def commission_rates(self, symbol: str = "BTCUSDT") -> dict[str, Any]:
        self.server_time()
        return self.request("GET", "/api/v3/account/commission", {"symbol": symbol.upper()}, signed=True)

    def test_order(self, intent: OrderIntent) -> dict[str, Any]:
        self.safety.assert_authenticated()
        self.server_time()
        filters = self.filters(intent.symbol)
        reference_price = self._market_reference_price(intent) if intent.order_type.upper() == "MARKET" else None
        filters.validate(
            side=intent.side,
            order_type=intent.order_type,
            quantity=intent.quantity,
            quote_order_qty=intent.quote_order_qty,
            price=intent.price,
            reference_price=reference_price,
        )
        return self.request("POST", "/api/v3/order/test", self._order_params(intent), signed=True, write=False)

    def place_order(self, intent: OrderIntent, *, state: "MarketState | None" = None) -> dict[str, Any]:
        self.safety.assert_order_allowed()
        self.assert_account_safety()
        if state is None:
            raise SafetyBlocked("live order placement requires initialized live market state")
        if state.symbol != intent.symbol.upper():
            raise SafetyBlocked("market state symbol does not match order intent")
        self.safety.assert_market_fresh(state)
        filters = self.filters(intent.symbol)
        reference_price = self._state_reference_price(intent, state)
        filters.validate(
            side=intent.side,
            order_type=intent.order_type,
            quantity=intent.quantity,
            quote_order_qty=intent.quote_order_qty,
            price=intent.price,
            reference_price=reference_price,
        )
        client_order_id = intent.client_order_id or f"cq-{_now_ms()}-{uuid.uuid4().hex[:12]}"
        live_intent = OrderIntent(
            intent.symbol,
            intent.side,
            intent.order_type,
            intent.quantity,
            intent.price,
            intent.time_in_force,
            client_order_id,
            intent.quote_order_qty,
        )
        try:
            return self.request("POST", "/api/v3/order", self._order_params(live_intent), signed=True, write=True)
        except ExecutionStatusUnknown as exc:
            try:
                reconciled = self.wait_for_terminal_order(
                    live_intent.symbol,
                    client_order_id=client_order_id,
                    timeout_s=self.config.request_timeout_s,
                )
            except BinanceError:
                exc.payload = {"clientOrderId": client_order_id, "reconciled": False}
                raise
            return {"reconciledAfterUnknownStatus": True, **reconciled}

    def cancel_order(self, symbol: str, *, order_id: int | None = None, client_order_id: str | None = None) -> dict[str, Any]:
        self.safety.assert_authenticated()
        self.server_time()
        return self.request(
            "DELETE", "/api/v3/order", {"symbol": symbol.upper(), "orderId": order_id, "origClientOrderId": client_order_id}, signed=True, write=True
        )

    def cancel_all(self, symbol: str) -> list[dict[str, Any]]:
        self.safety.assert_authenticated()
        self.server_time()
        return self.request("DELETE", "/api/v3/openOrders", {"symbol": symbol.upper()}, signed=True, write=True)

    def _market_reference_price(self, intent: OrderIntent) -> Decimal:
        ticker = self.book_ticker(intent.symbol)
        field = "askPrice" if intent.side.upper() == "BUY" else "bidPrice"
        return _decimal(ticker[field], field)

    @staticmethod
    def _state_reference_price(intent: OrderIntent, state: "MarketState") -> Decimal | None:
        if intent.order_type.upper() != "MARKET":
            return None
        quote = state.order_book.best_ask if intent.side.upper() == "BUY" else state.order_book.best_bid
        if quote is None:
            raise SafetyBlocked("market order requires a populated local order book")
        return quote[0]

    @staticmethod
    def _order_params(intent: OrderIntent) -> dict[str, Any]:
        params: dict[str, Any] = {
            "symbol": intent.symbol.upper(),
            "side": intent.side.upper(),
            "type": intent.order_type.upper(),
        }
        if intent.quantity is not None:
            params["quantity"] = _decimal_text(intent.quantity)
        if intent.quote_order_qty is not None:
            params["quoteOrderQty"] = _decimal_text(intent.quote_order_qty)
        if intent.price is not None:
            params["price"] = _decimal_text(intent.price)
        if intent.time_in_force is not None and intent.order_type.upper() == "LIMIT":
            params["timeInForce"] = intent.time_in_force
        if intent.client_order_id:
            params["newClientOrderId"] = intent.client_order_id
        params["newOrderRespType"] = "FULL"
        return params


class BinanceUserDataStream:
    """Authenticated user-event stream using Binance signature subscription."""

    def __init__(self, config: BinanceConfig | None = None):
        self.config = config or BinanceConfig.from_env()
        self.safety = LiveSafety(self.config)
        self._stop = asyncio.Event()

    def stop(self) -> None:
        self._stop.set()

    def _subscription_request(self) -> dict[str, Any]:
        self.safety.assert_authenticated()
        params = {
            "apiKey": self.config.api_key,
            "timestamp": _now_ms(),
            "recvWindow": self.config.recv_window_ms,
        }
        # Binance HMAC signatures are hexadecimal. Base64 is used by the
        # WebSocket API only for RSA/Ed25519 signatures, not HMAC.
        params["signature"] = _hmac_hex(self.config.api_secret, params)
        return {"id": str(uuid.uuid4()), "method": "userDataStream.subscribe.signature", "params": params}

    async def run(
        self,
        callback: Callable[[Mapping[str, Any]], Awaitable[None] | None],
        *,
        reconnect_delay_s: float = 1.0,
    ) -> None:
        try:
            import websockets
        except ImportError as exc:
            raise RuntimeError("websockets package is required for user data streams") from exc
        self.safety.assert_authenticated()
        delay = reconnect_delay_s
        while not self._stop.is_set():
            try:
                await self._run_connection(websockets, callback)
                delay = reconnect_delay_s
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                result = callback({"event": "user_stream_error", "error": str(exc)})
                if asyncio.iscoroutine(result):
                    await result
                await asyncio.sleep(delay)
                delay = min(60.0, delay * 2)

    async def _run_connection(self, websockets: Any, callback: Callable[..., Any]) -> None:
        async with websockets.connect(
            self.config.ws_api_url,
            ping_interval=20,
            ping_timeout=60,
            close_timeout=5,
        ) as socket:
            await socket.send(json.dumps(self._subscription_request()))
            response = json.loads(await socket.recv())
            if response.get("status") != 200:
                raise BinanceError("user data subscription rejected", payload=response)
            while not self._stop.is_set():
                payload = json.loads(await socket.recv())
                result = callback(payload)
                if asyncio.iscoroutine(result):
                    await result



@dataclass
class LocalOrderBook:
    symbol: str
    bids: dict[Decimal, Decimal] = field(default_factory=dict)
    asks: dict[Decimal, Decimal] = field(default_factory=dict)
    last_update_id: int | None = None
    stale: bool = True
    stream_synced: bool = False
    last_event_ms: int | None = None

    def load_snapshot(self, snapshot: Mapping[str, Any]) -> None:
        self.bids = {}
        self.asks = {}
        for price, quantity in snapshot.get("bids", []):
            parsed_quantity = _decimal(quantity, "bid quantity")
            if parsed_quantity > 0:
                self.bids[_decimal(price, "bid price")] = parsed_quantity
        for price, quantity in snapshot.get("asks", []):
            parsed_quantity = _decimal(quantity, "ask quantity")
            if parsed_quantity > 0:
                self.asks[_decimal(price, "ask price")] = parsed_quantity
        self.last_update_id = int(snapshot["lastUpdateId"])
        # A REST snapshot is displayable, but it is not a stream-synchronized
        # book. A live order must wait for at least one contiguous depth event.
        self.last_event_ms = None
        self.stale = True
        self.stream_synced = False

    def bootstrap_events(self, events: Iterable[Mapping[str, Any]]) -> None:
        if self.last_update_id is None:
            raise BookResyncRequired("snapshot must be loaded before depth events")
        buffered = list(events)
        first = next((event for event in buffered if int(event["u"]) > self.last_update_id), None)
        if first is None:
            raise BookResyncRequired("no depth event advanced the snapshot")
        if int(first["U"]) > self.last_update_id + 1:
            raise BookResyncRequired("snapshot is older than buffered depth sequence")
        for event in buffered:
            if int(event["u"]) <= self.last_update_id:
                continue
            self.apply_event(event)
        self.stale = False
        self.stream_synced = True

    def apply_event(self, event: Mapping[str, Any]) -> None:
        if self.last_update_id is None:
            raise BookResyncRequired("order book has no snapshot")
        first_id = int(event["U"])
        final_id = int(event["u"])
        if final_id <= self.last_update_id:
            return
        if first_id > self.last_update_id + 1:
            self.stale = True
            raise BookResyncRequired(
                f"depth sequence gap: local={self.last_update_id}, event=[{first_id},{final_id}]"
            )
        self._apply_levels(self.bids, event.get("b", []))
        self._apply_levels(self.asks, event.get("a", []))
        self.last_update_id = final_id
        self.last_event_ms = int(event.get("E", _now_ms()))
        self.stale = False
        self.stream_synced = True

    @staticmethod
    def _apply_levels(book: dict[Decimal, Decimal], levels: Iterable[Iterable[Any]]) -> None:
        for raw_price, raw_quantity in levels:
            price = _decimal(raw_price, "depth price")
            quantity = _decimal(raw_quantity, "depth quantity")
            if quantity == 0:
                book.pop(price, None)
            else:
                book[price] = quantity

    @property
    def best_bid(self) -> tuple[Decimal, Decimal] | None:
        return max(self.bids.items()) if self.bids else None

    @property
    def best_ask(self) -> tuple[Decimal, Decimal] | None:
        return min(self.asks.items()) if self.asks else None

    def summary(self) -> dict[str, Any]:
        bid = self.best_bid
        ask = self.best_ask
        if bid is None or ask is None:
            return {"symbol": self.symbol, "stale": True, "last_update_id": self.last_update_id}
        return {
            "symbol": self.symbol,
            "stale": self.stale,
            "last_update_id": self.last_update_id,
            "stream_synced": self.stream_synced,
            "last_event_ms": self.last_event_ms,
            "best_bid": _decimal_text(bid[0]),
            "best_bid_qty": _decimal_text(bid[1]),
            "best_ask": _decimal_text(ask[0]),
            "best_ask_qty": _decimal_text(ask[1]),
            "mid": _decimal_text((bid[0] + ask[0]) / 2),
            "spread": _decimal_text(ask[0] - bid[0]),
        }


@dataclass
class MarketState:
    symbol: str
    order_book: LocalOrderBook
    last_trade: dict[str, Any] | None = None
    book_ticker: dict[str, Any] | None = None
    last_kline: dict[str, Any] | None = None
    last_event_ms: int | None = None
    stale: bool = True

    def update(self, event: Mapping[str, Any]) -> None:
        event_type = event.get("e")
        event_ms = int(event.get("E", _now_ms()))
        if event_type == "depthUpdate":
            self.order_book.apply_event(event)
        elif event_type == "aggTrade":
            self.last_trade = {
                "price": event.get("p"),
                "quantity": event.get("q"),
                "trade_id": event.get("a"),
                "event_ms": event_ms,
                "trade_ms": event.get("T"),
                "buyer_maker": event.get("m"),
            }
        elif event_type == "trade":
            self.last_trade = {
                "price": event.get("p"),
                "quantity": event.get("q"),
                "trade_id": event.get("t"),
                "event_ms": event_ms,
                "trade_ms": event.get("T"),
                "buyer_maker": event.get("m"),
            }
        elif event_type == "kline":
            self.last_kline = event.get("k", event)
        elif event_type == "bookTicker":
            self.book_ticker = {
                "update_id": event.get("u"),
                "bid": event.get("b"),
                "bid_quantity": event.get("B"),
                "ask": event.get("a"),
                "ask_quantity": event.get("A"),
                "event_ms": event_ms,
            }
        elif event_type == "24hrTicker":
            self.last_trade = {"price": event.get("c"), "event_ms": event_ms}
        self.last_event_ms = max(self.last_event_ms or 0, event_ms)
        self.stale = self.order_book.stale

    def freshness_ms(self) -> int | None:
        event_ms = self.order_book.last_event_ms or self.last_event_ms
        return None if event_ms is None else max(0, _now_ms() - event_ms)

    def snapshot(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "stale": self.stale,
            "freshness_ms": self.freshness_ms(),
            "order_book": self.order_book.summary(),
            "last_trade": self.last_trade,
            "book_ticker": self.book_ticker,
            "last_kline": self.last_kline,
        }


def normalize_stream_payload(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    data = payload.get("data", payload)
    if not isinstance(data, Mapping):
        raise ValueError("unexpected Binance stream payload")
    normalized = dict(data)
    stream = str(payload.get("stream", ""))
    if "e" not in normalized and stream.endswith("@bookTicker"):
        normalized["e"] = "bookTicker"
        normalized["E"] = _now_ms()
    return normalized


class BinanceMarketStream:
    """Reconnectable public market stream with order-book resynchronization."""

    def __init__(self, rest: BinanceRestClient, symbol: str = "BTCUSDT", intervals: tuple[str, ...] = ("1m",)):
        self.rest = rest
        self.symbol = symbol.upper()
        self.intervals = intervals
        self.state = MarketState(self.symbol, LocalOrderBook(self.symbol))
        self._stop = asyncio.Event()

    def stop(self) -> None:
        self._stop.set()

    def stream_url(self) -> str:
        streams = [
            f"{self.symbol.lower()}@depth@100ms",
            f"{self.symbol.lower()}@aggTrade",
            f"{self.symbol.lower()}@bookTicker",
        ]
        streams.extend(f"{self.symbol.lower()}@kline_{interval}" for interval in self.intervals)
        separator = "&" if "?" in self.rest.config.market_ws_url else "?"
        return f"{self.rest.config.market_ws_url}{separator}streams={'/'.join(streams)}"

    async def run(
        self,
        callback: Callable[[dict[str, Any]], Awaitable[None] | None],
        *,
        reconnect_delay_s: float = 1.0,
        max_connection_age_s: float = 23 * 60 * 60,
    ) -> None:
        try:
            import websockets
        except ImportError as exc:
            raise RuntimeError("websockets package is required for live market streams") from exc
        delay = reconnect_delay_s
        while not self._stop.is_set():
            try:
                await self._run_connection(websockets, callback, max_connection_age_s)
                delay = reconnect_delay_s
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.state.stale = True
                await self._emit(callback, {"type": "stream_error", "error": str(exc), "state": self.state.snapshot()})
                await asyncio.sleep(delay)
                delay = min(60.0, delay * 2)

    async def _run_connection(self, websockets: Any, callback: Callable[..., Any], max_age_s: float) -> None:
        queue: asyncio.Queue[Mapping[str, Any]] = asyncio.Queue(maxsize=10_000)
        self.state.stale = True
        self.state.order_book = LocalOrderBook(self.symbol)
        async with websockets.connect(
            self.stream_url(),
            ping_interval=20,
            ping_timeout=60,
            close_timeout=5,
            max_queue=10_000,
        ) as socket:
            async def reader() -> None:
                async for raw in socket:
                    payload = json.loads(raw)
                    await queue.put(normalize_stream_payload(payload))

            reader_task = asyncio.create_task(reader())
            started = time.monotonic()
            buffered_depth: list[Mapping[str, Any]] = []
            try:
                snapshot = await asyncio.to_thread(self.rest.depth, self.symbol, 5000)
                self.state.order_book.load_snapshot(snapshot)
                # Buffer until a depth event advances the snapshot. Other
                # market events can still update the display while the book
                # is synchronizing, but they never make it order-ready.
                sync_deadline = time.monotonic() + 10.0
                while not buffered_depth:
                    remaining = sync_deadline - time.monotonic()
                    if remaining <= 0:
                        raise BookResyncRequired("timed out waiting for depth synchronization")
                    event = await asyncio.wait_for(queue.get(), timeout=remaining)
                    if event.get("e") == "depthUpdate":
                        if int(event["u"]) > self.state.order_book.last_update_id:
                            buffered_depth.append(event)
                    else:
                        await self._process_event(event, callback)
                while not queue.empty():
                    event = queue.get_nowait()
                    if event.get("e") == "depthUpdate":
                        buffered_depth.append(event)
                    else:
                        await self._process_event(event, callback)
                self.state.order_book.bootstrap_events(buffered_depth)
                self.state.stale = self.state.order_book.stale
                await self._emit(callback, {"type": "book_ready", "state": self.state.snapshot()})
                while not self._stop.is_set() and time.monotonic() - started < max_age_s:
                    try:
                        event = await asyncio.wait_for(queue.get(), timeout=1.0)
                    except asyncio.TimeoutError:
                        continue
                    await self._process_event(event, callback)
            finally:
                reader_task.cancel()
                await asyncio.gather(reader_task, return_exceptions=True)

    async def _process_event(self, event: Mapping[str, Any], callback: Callable[..., Any]) -> None:
        event_type = event.get("e")
        if event_type == "depthUpdate" and self.state.order_book.last_update_id is None:
            return
        self.state.update(event)
        await self._emit(callback, {"type": "market_event", "event": dict(event), "state": self.state.snapshot()})

    @staticmethod
    async def _emit(callback: Callable[..., Any], value: dict[str, Any]) -> None:
        result = callback(value)
        if asyncio.iscoroutine(result):
            await result


class KillSwitch:
    def __init__(self, path: Path):
        self.path = path

    def engage(self, reason: str = "manual") -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_name(f".{self.path.name}.part")
        temporary.write_text(json.dumps({"engaged_at": dt.datetime.now(UTC).isoformat(), "reason": reason}) + "\n", encoding="utf-8")
        os.replace(temporary, self.path)

    def active(self) -> bool:
        return self.path.exists()


class AccountReconciler:
    """Read-only account/order reconciliation helper."""

    def __init__(self, rest: BinanceRestClient, symbol: str = "BTCUSDT"):
        self.rest = rest
        self.symbol = symbol.upper()

    def snapshot(self) -> dict[str, Any]:
        account = self.rest.account()
        orders = self.rest.open_orders(self.symbol)
        return {
            "retrieved_at": dt.datetime.now(UTC).isoformat(),
            "account_type": account.get("accountType"),
            "can_trade": account.get("canTrade"),
            "can_withdraw": account.get("canWithdraw"),
            "can_deposit": account.get("canDeposit"),
            "permissions": account.get("permissions") or [],
            "balances": [
                balance for balance in account.get("balances", []) if balance.get("asset") in {"BTC", "USDT"}
            ],
            "open_orders": orders,
        }


def _safe_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


async def observe(config: BinanceConfig, symbol: str, duration_s: float | None = None) -> None:
    rest = BinanceRestClient(config)
    rest.ping()
    rest.server_time()
    stream = BinanceMarketStream(rest, symbol=symbol)
    if duration_s is not None:
        async def stopper() -> None:
            await asyncio.sleep(duration_s)
            stream.stop()
        asyncio.create_task(stopper())

    async def callback(message: dict[str, Any]) -> None:
        if message.get("type") in {"book_ready", "stream_error"}:
            print(_safe_json(message), flush=True)
        elif message.get("type") == "market_event":
            event = message["event"]
            if event.get("e") in {"bookTicker", "aggTrade", "kline"}:
                print(_safe_json({"event": event.get("e"), "state": message["state"]}), flush=True)

    await stream.run(callback)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("public-check", help="call public ping/time/exchangeInfo/book endpoints")
    observe_parser = sub.add_parser("observe", help="connect to live public market streams")
    observe_parser.add_argument("--symbol", default="BTCUSDT")
    observe_parser.add_argument("--duration", type=float)
    account_parser = sub.add_parser("account-check", help="read account and open orders; never places orders")
    account_parser.add_argument("--symbol", default="BTCUSDT")
    test_parser = sub.add_parser("test-order", help="signed Binance order validation; no matching-engine order")
    test_parser.add_argument("--symbol", default="BTCUSDT")
    test_parser.add_argument("--side", choices=("BUY", "SELL"), required=True)
    test_parser.add_argument("--quantity", required=True)
    test_parser.add_argument("--price")
    test_parser.add_argument("--type", dest="order_type", choices=("LIMIT", "MARKET", "LIMIT_MAKER"), default="LIMIT")
    place_parser = sub.add_parser("place-order", help="guarded live order path; disabled unless all gates pass")
    place_parser.add_argument("--symbol", default="BTCUSDT")
    place_parser.add_argument("--side", choices=("BUY", "SELL"), required=True)
    place_parser.add_argument("--quantity", required=True)
    place_parser.add_argument("--price")
    place_parser.add_argument("--type", dest="order_type", choices=("LIMIT", "MARKET", "LIMIT_MAKER"), default="LIMIT")
    kill_parser = sub.add_parser("kill", help="engage the independent kill switch")
    kill_parser.add_argument("--reason", default="manual")
    return parser


def main() -> int:
    logging.basicConfig(level=os.environ.get("BINANCE_LOG_LEVEL", "INFO"))
    args = build_parser().parse_args()
    config = BinanceConfig.from_env()
    rest = BinanceRestClient(config)
    try:
        if args.command == "public-check":
            payload = {
                "ping": rest.ping(),
                "server_time": rest.server_time(),
                "exchange": rest.exchange_info("BTCUSDT").__dict__,
                "book_ticker": rest.book_ticker("BTCUSDT"),
            }
            print(_safe_json(payload))
            return 0
        if args.command == "observe":
            asyncio.run(observe(config, args.symbol, args.duration))
            return 0
        if args.command == "account-check":
            rest.safety.assert_authenticated()
            print(_safe_json(AccountReconciler(rest, args.symbol).snapshot()))
            return 0
        if args.command in {"test-order", "place-order"}:
            quantity = _decimal(args.quantity, "quantity")
            price = _decimal(args.price, "price") if args.price else None
            intent = OrderIntent(args.symbol, args.side, args.order_type, quantity, price)
            if args.command == "test-order":
                print(_safe_json(rest.test_order(intent)))
            else:
                print(_safe_json(rest.place_order(intent)))
            return 0
        if args.command == "kill":
            KillSwitch(config.kill_switch_path).engage(args.reason)
            print(_safe_json({"kill_switch": "engaged", "path": str(config.kill_switch_path)}))
            return 0
    except (BinanceError, SafetyBlocked, BookResyncRequired, ValueError) as exc:
        print(_safe_json({"error": type(exc).__name__, "message": str(exc)}))
        return 1
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
