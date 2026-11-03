#!/usr/bin/env python3
"""Local, safety-first dashboard for the crypto research system.

The browser is only a control surface. It never receives API secrets and it
cannot bypass the Binance connector's server-side safety checks. The default
mode is public observation. A real order requires the connector's explicit
environment gates, a fresh sequence-checked market state, account preflight,
and a separate request confirmation.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import ipaddress
import json
import os
import contextlib
import subprocess
import sys
import time
import traceback
import uuid
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Awaitable, Callable, Mapping

try:  # Load project .env before importing configuration consumers.
    from . import config as _config
except ImportError:  # pragma: no cover - direct script execution
    import config as _config  # type: ignore

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

try:  # Support both ``uvicorn research.dashboard_app:app`` and direct runs.
    from .binance_live import (
        AccountReconciler,
        BinanceConfig,
        BinanceError,
        BinanceMarketStream,
        BinanceRestClient,
        BinanceUserDataStream,
        BookResyncRequired,
        ExecutionStatusUnknown,
        KillSwitch,
        MarketState,
        OrderIntent,
        SafetyBlocked,
        _decimal,
        _decimal_text,
        _now_ms,
    )
    from .live_engine import (
        EngineConfig,
        LiveTradingEngine,
        NoTradeReasoner,
        DeterministicReasoner,
        OpenAICompatibleReasoner,
        PaperExecutor,
        BinanceLiveExecutor,
    )
except ImportError:  # pragma: no cover - used when executing this file directly.
    from binance_live import (  # type: ignore
        AccountReconciler,
        BinanceConfig,
        BinanceError,
        BinanceMarketStream,
        BinanceRestClient,
        BinanceUserDataStream,
        BookResyncRequired,
        ExecutionStatusUnknown,
        KillSwitch,
        MarketState,
        OrderIntent,
        SafetyBlocked,
        _decimal,
        _decimal_text,
        _now_ms,
    )
    from live_engine import (  # type: ignore
        EngineConfig,
        LiveTradingEngine,
        NoTradeReasoner,
        DeterministicReasoner,
        OpenAICompatibleReasoner,
        PaperExecutor,
        BinanceLiveExecutor,
    )


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DASHBOARD_ROOT = PROJECT_ROOT / "research" / "dashboard"
RUNTIME_ROOT = PROJECT_ROOT / "research" / "runtime"
RESEARCH_ROOT = PROJECT_ROOT / "research"


def _resolve_project_path(path: Path) -> Path:
    return path if path.is_absolute() else PROJECT_ROOT / path


def dashboard_config() -> BinanceConfig:
    source = BinanceConfig.from_env()
    return replace(
        source,
        kill_switch_path=_resolve_project_path(source.kill_switch_path),
        live_gate_path=_resolve_project_path(source.live_gate_path),
    )


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def safe_json(value: Any) -> Any:
    """Return JSON-safe values without ever serializing credentials."""
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dt.datetime):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): safe_json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [safe_json(item) for item in value]
    if isinstance(value, set):
        return sorted(safe_json(item) for item in value)
    if hasattr(value, "__dict__") and not isinstance(value, (str, bytes)):
        return safe_json(value.__dict__)
    return value


def read_json(path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return None


@dataclass
class ActionRecord:
    action_id: str
    name: str
    status: str = "queued"
    started_at: str | None = None
    finished_at: str | None = None
    result: Any = None
    error: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "action_id": self.action_id,
            "name": self.name,
            "status": self.status,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "result": safe_json(self.result),
            "error": self.error,
        }


@dataclass
class DashboardRuntime:
    config: BinanceConfig = field(default_factory=dashboard_config)
    rest: BinanceRestClient | None = None
    stream: BinanceMarketStream | None = None
    stream_task: asyncio.Task[Any] | None = None
    user_stream_task: asyncio.Task[Any] | None = None
    engine: LiveTradingEngine | None = None
    engine_task: asyncio.Task[Any] | None = None
    latest_public: dict[str, Any] | None = None
    latest_market_message: dict[str, Any] | None = None
    latest_account: dict[str, Any] | None = None
    user_events: list[dict[str, Any]] = field(default_factory=list)
    account_baseline_usdt: float | None = None
    account_last_usdt: float | None = None
    last_error: str | None = None
    actions: dict[str, ActionRecord] = field(default_factory=dict)
    action_order: list[str] = field(default_factory=list)
    sockets: set[WebSocket] = field(default_factory=set)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    def __post_init__(self) -> None:
        self.rest = BinanceRestClient(self.config)
        RUNTIME_ROOT.mkdir(parents=True, exist_ok=True)

    @property
    def safety(self):
        assert self.rest is not None
        return self.rest.safety

    def stream_state(self) -> dict[str, Any] | None:
        return self.stream.state.snapshot() if self.stream is not None else None

    def engine_state(self) -> dict[str, Any] | None:
        return self.engine.state_snapshot() if self.engine is not None else None

    def research_snapshot(self) -> dict[str, Any]:
        files = {
            "deployment_gate": RESEARCH_ROOT / "experiments" / "deployment-gate-final-v2.json",
            "baseline": RESEARCH_ROOT / "experiments" / "baseline-final-v2" / "baseline_report.json",
            "walk_forward": RESEARCH_ROOT / "experiments" / "walk-forward-final-v2.json",
            "signals": RESEARCH_ROOT / "experiments" / "signal-diagnostics-final-v3.json",
            "warehouse": RESEARCH_ROOT / "warehouse.sqlite",
        }
        result: dict[str, Any] = {}
        for name, path in files.items():
            item: dict[str, Any] = {"present": path.exists(), "path": str(path)}
            if path.is_file():
                item["modified_at"] = dt.datetime.fromtimestamp(path.stat().st_mtime, tz=dt.timezone.utc).isoformat()
                item["size_bytes"] = path.stat().st_size
            if path.suffix == ".json":
                payload = read_json(path)
                if payload is not None:
                    item["decision"] = payload.get("decision")
                    item["deployment_status"] = payload.get("deployment_status")
                    if name == "deployment_gate":
                        item["blocker_count"] = len(payload.get("blockers", []))
                    if name == "baseline":
                        item["report_count"] = len(payload.get("reports", []))
                        item["trade_count"] = payload.get("trade_count")
                    if name == "walk_forward":
                        item["fold_count"] = len(payload.get("reports", []))
                    if name == "signals":
                        item["result_count"] = len(payload.get("conditional_results", []))
            result[name] = item
        return result

    def safety_snapshot(self) -> dict[str, Any]:
        gate = self.safety.gate_decision()
        kill = self.safety.kill_switch_active()
        return {
            "credentials_configured": self.config.has_credentials,
            "order_mode": self.config.order_mode,
            "live_orders_enabled": self.config.enable_live_orders,
            "confirmation_configured": self.config.live_confirmation == "I_UNDERSTAND_LIVE_TRADING",
            "gate_decision": gate,
            "kill_switch_active": kill,
            "withdrawal_permission_policy": "must be disabled before live writes" if self.config.require_withdrawals_disabled else "not enforced",
            "market_state_ready": bool(
                self.stream is not None
                and self.stream.state.last_event_ms is not None
                and not self.stream.state.stale
                and not self.stream.state.order_book.stale
            ),
            "live_order_path_ready": bool(
                self.config.has_credentials
                and self.config.order_mode == "live"
                and self.config.enable_live_orders
                and self.config.live_confirmation == "I_UNDERSTAND_LIVE_TRADING"
                and gate == "ALLOW_LIVE_TRADING"
                and not kill
            ),
            "user_stream_connected": bool(
                self.user_stream_task is not None and not self.user_stream_task.done()
            ),
            "engine_running": bool(self.engine_task is not None and not self.engine_task.done()),
            "engine_mode": self.engine.config.mode if self.engine is not None else EngineConfig.from_env().mode,
        }

    def account_summary(self) -> dict[str, Any] | None:
        if self.latest_account is None:
            return None
        balances = self.latest_account.get("balances", [])
        by_asset = {str(item.get("asset")): item for item in balances}
        usdt = float(by_asset.get("USDT", {}).get("free", 0) or 0) + float(by_asset.get("USDT", {}).get("locked", 0) or 0)
        btc = float(by_asset.get("BTC", {}).get("free", 0) or 0) + float(by_asset.get("BTC", {}).get("locked", 0) or 0)
        ask = None
        if self.stream is not None and self.stream.state.order_book.best_ask is not None:
            ask = float(self.stream.state.order_book.best_ask[0])
        elif self.latest_public and self.latest_public.get("book_ticker", {}).get("askPrice"):
            ask = float(self.latest_public["book_ticker"]["askPrice"])
        total = usdt + btc * ask if ask is not None else None
        if total is not None:
            if self.account_baseline_usdt is None:
                self.account_baseline_usdt = total
            self.account_last_usdt = total
        return {
            "usdt_total": usdt,
            "btc_total": btc,
            "btc_mark_price": ask,
            "estimated_total_usdt": total,
            "session_change_usdt": (total - self.account_baseline_usdt) if total is not None and self.account_baseline_usdt is not None else None,
            "session_change_is_profit": False,
            "note": "This is an account-value estimate; it is not realized profit and excludes unobserved costs.",
        }

    async def snapshot(self) -> dict[str, Any]:
        async with self.lock:
            actions = [self.actions[action_id].as_dict() for action_id in self.action_order[-12:]]
            return {
                "time": utc_now(),
                "system": {
                    "name": "Pocket Quant Control Room",
                    "mode": (
                        f"engine-{self.engine.config.mode.lower()}"
                        if self.engine_task is not None and not self.engine_task.done() and self.engine is not None
                        else "live-observation" if self.stream_task and not self.stream_task.done() else "standby"
                    ),
                    "exchange": "Binance Spot",
                    "symbol": "BTCUSDT",
                    "dashboard_local_only": True,
                },
                "safety": self.safety_snapshot(),
                "market": self.stream_state(),
                "engine": self.engine_state(),
                "public": self.latest_public,
                "account": self.latest_account,
                "account_summary": self.account_summary(),
                "user_events": self.user_events[-10:],
                "last_market_message": self.latest_market_message,
                "last_error": self.last_error,
                "research": self.research_snapshot(),
                "actions": actions,
            }

    async def broadcast(self) -> None:
        message = await self.snapshot()
        stale: list[WebSocket] = []
        for socket in list(self.sockets):
            try:
                await socket.send_json(message)
            except Exception:
                stale.append(socket)
        for socket in stale:
            self.sockets.discard(socket)

    async def record_action(self, name: str, operation: Callable[[], Awaitable[Any]]) -> ActionRecord:
        record = ActionRecord(action_id=uuid.uuid4().hex[:16], name=name)
        async with self.lock:
            self.actions[record.action_id] = record
            self.action_order.append(record.action_id)
            if len(self.action_order) > 100:
                old = self.action_order.pop(0)
                self.actions.pop(old, None)
        asyncio.create_task(
            self._run_action(record, operation),
            name=f"dashboard-action-{record.action_id}",
        )
        await self.broadcast()
        return record

    async def _run_action(self, record: ActionRecord, operation: Callable[[], Awaitable[Any]]) -> None:
        record.status = "running"
        record.started_at = utc_now()
        await self.broadcast()
        try:
            record.result = await operation()
            record.status = "completed"
        except asyncio.CancelledError:
            record.status = "cancelled"
            record.error = "action cancelled"
            raise
        except Exception as exc:  # Surface a safe message to the UI; keep trace server-side.
            record.status = "failed"
            record.error = f"{type(exc).__name__}: {exc}"
            self.last_error = record.error
            traceback.print_exc()
        finally:
            record.finished_at = utc_now()
            await self.broadcast()

    async def start_safe_mode(self) -> dict[str, Any]:
        """Start observation only; this operation can never place an order."""
        public = await self.public_check()
        stream = await self.start_stream(("1m",))
        return {
            "mode": "observation_only",
            "public": public,
            "stream": stream,
            "real_orders": False,
            "reason": "The research deployment gate does not authorize live orders.",
        }

    async def public_check(self) -> dict[str, Any]:
        if self.rest is None:
            raise RuntimeError("REST client is not initialized")
        ping = await asyncio.to_thread(self.rest.ping)
        server_time = await asyncio.to_thread(self.rest.server_time)
        filters = await asyncio.to_thread(self.rest.exchange_info, "BTCUSDT")
        book = await asyncio.to_thread(self.rest.book_ticker, "BTCUSDT")
        result = {
            "ok": True,
            "checked_at": utc_now(),
            "ping": ping,
            "server_time": server_time,
            "symbol": filters.symbol,
            "status": filters.status,
            "base_asset": filters.base_asset,
            "quote_asset": filters.quote_asset,
            "order_types": list(filters.order_types),
            "tick_size": _decimal_text(filters.tick_size),
            "step_size": _decimal_text(filters.step_size),
            "min_qty": _decimal_text(filters.min_qty),
            "min_notional": _decimal_text(filters.min_notional),
            "book_ticker": book,
        }
        self.latest_public = result
        return result

    async def start_stream(self, intervals: tuple[str, ...] = ("1m",)) -> dict[str, Any]:
        if self.stream_task is not None and not self.stream_task.done():
            return {"started": False, "reason": "already_running", "state": self.stream_state()}
        if self.rest is None:
            raise RuntimeError("REST client is not initialized")
        self.stream = BinanceMarketStream(self.rest, symbol="BTCUSDT", intervals=intervals)

        async def callback(message: dict[str, Any]) -> None:
            event = message.get("event")
            # Do not flood the dashboard with every trade. The connector keeps
            # the complete in-memory state; the UI receives a compact latest view.
            if event is None or event.get("e") in {"bookTicker", "kline", "depthUpdate"}:
                self.latest_market_message = {
                    "type": message.get("type"),
                    "event": event.get("e") if isinstance(event, Mapping) else None,
                    "received_at": utc_now(),
                }
                await self.broadcast()

        async def runner() -> None:
            assert self.stream is not None
            await self.stream.run(callback)

        self.stream_task = asyncio.create_task(runner(), name="binance-market-stream")
        return {"started": True, "stream_url": self.stream.stream_url(), "state": self.stream_state()}

    async def start_engine(self, mode: str | None = None, duration_minutes: int | None = None) -> dict[str, Any]:
        if self.engine_task is not None and not self.engine_task.done():
            return {"started": False, "reason": "engine_already_running", "state": self.engine_state()}
        if self.rest is None:
            raise RuntimeError("REST client is not initialized")
        engine_config = EngineConfig.from_env()
        engine_config = __import__("dataclasses").replace(
            engine_config,
            journal_path=_resolve_project_path(engine_config.journal_path),
            state_path=_resolve_project_path(engine_config.state_path),
            knowledge_base_path=_resolve_project_path(engine_config.knowledge_base_path),
            assumptions_path=_resolve_project_path(engine_config.assumptions_path),
        )
        if mode is not None:
            requested_mode = str(mode).upper()
            if requested_mode not in {"PAPER", "LIVE"}:
                raise SafetyBlocked("engine mode must be PAPER or LIVE")
            if requested_mode == "LIVE" and engine_config.mode != "LIVE":
                raise SafetyBlocked("LIVE must be enabled in server environment; browser cannot switch into LIVE")
            engine_config = __import__("dataclasses").replace(engine_config, mode=requested_mode)
        binance_config = self.config
        if engine_config.mode == "LIVE":
            # The engine performs the final Binance safety assertion immediately
            # before each write. Do a preflight here so the UI reports a clear
            # failure before a session is started.
            self.safety.assert_order_allowed()
            await asyncio.to_thread(self.rest.assert_account_safety)
        reasoner: Any = DeterministicReasoner(engine_config) if engine_config.mode == "PAPER" else NoTradeReasoner()
        if engine_config.llm_enabled:
            context: dict[str, Any] = {}
            for path in (_resolve_project_path(engine_config.knowledge_base_path), _resolve_project_path(engine_config.assumptions_path)):
                context[str(path)] = read_json(path) or {"status": "unavailable"}
            reasoner = OpenAICompatibleReasoner(engine_config, context)
        executor: Any = PaperExecutor(engine_config)
        if engine_config.mode == "LIVE":
            executor = BinanceLiveExecutor(engine_config, self.rest)
        # The engine owns this stream. The standalone market-watch action must
        # not create a second Binance connection for the same symbol.
        if self.stream_task is not None and not self.stream_task.done():
            await self.stop_stream()
        self.stream = BinanceMarketStream(self.rest, symbol=engine_config.symbol, intervals=("1m",))
        self.engine = LiveTradingEngine(engine_config, rest=self.rest, stream=self.stream, reasoner=reasoner, executor=executor)

        async def runner() -> None:
            assert self.engine is not None
            try:
                await self.engine.run(duration_minutes=duration_minutes)
            except Exception as exc:
                self.last_error = f"engine stopped: {type(exc).__name__}: {exc}"
                if self.engine is not None:
                    self.engine.last_error = self.last_error
                    self.engine.risk.halt("engine runtime failure")
                    self.engine.running = False
                    self.engine.persist_state()
            finally:
                await self.broadcast()

        self.engine_task = asyncio.create_task(runner(), name="live-trading-engine")
        return {"started": True, "mode": engine_config.mode, "state": self.engine_state()}

    async def stop_engine(self, reason: str = "dashboard stop") -> dict[str, Any]:
        if self.engine is not None:
            if self.engine.position is not None:
                try:
                    if self.engine.features.last_features is not None and self.engine.stream is not None:
                        await self.engine._close_position(
                            self.engine.features.last_features,
                            self.engine.stream.state,
                            "operator_stop",
                        )
                except Exception as exc:
                    self.last_error = f"failed to close position on stop: {type(exc).__name__}: {exc}"
            self.engine.request_stop(reason)
        if self.stream is not None:
            self.stream.stop()
        task = self.engine_task
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self.engine_task = None
        if self.engine is not None:
            self.engine.running = False
            self.engine.persist_state()
        return {"stopped": True, "engine": self.engine_state()}

    async def engine_status(self) -> dict[str, Any]:
        state = self.engine_state()
        return {
            "running": bool(self.engine_task is not None and not self.engine_task.done()),
            "mode": state.get("mode") if state else EngineConfig.from_env().mode,
            "state": state,
        }

    async def stop_stream(self) -> dict[str, Any]:
        if self.stream is not None:
            self.stream.stop()
        task = self.stream_task
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self.stream_task = None
        if self.stream is not None:
            self.stream.state.stale = True
        await self.broadcast()
        return {"stopped": True}

    async def account_check(self) -> dict[str, Any]:
        if self.rest is None:
            raise RuntimeError("REST client is not initialized")
        self.safety.assert_authenticated()
        result = await asyncio.to_thread(AccountReconciler(self.rest, "BTCUSDT").snapshot)
        self.latest_account = result
        return result

    async def start_user_stream(self) -> dict[str, Any]:
        if self.user_stream_task is not None and not self.user_stream_task.done():
            return {"started": False, "reason": "already_running"}
        if self.rest is None:
            raise RuntimeError("REST client is not initialized")
        self.safety.assert_authenticated()
        user_stream = BinanceUserDataStream(self.config)

        async def callback(event: Mapping[str, Any]) -> None:
            safe_event = {
                "event": event.get("e", event.get("event")),
                "received_at": utc_now(),
                "symbol": event.get("s"),
                "order_status": event.get("X"),
                "execution_type": event.get("x"),
                "side": event.get("S"),
                "order_id": event.get("i"),
                "error": event.get("error"),
            }
            self.user_events.append(safe_event)
            self.user_events = self.user_events[-50:]
            await self.broadcast()

        async def runner() -> None:
            await user_stream.run(callback)

        self.user_stream_task = asyncio.create_task(runner(), name="binance-user-stream")
        return {"started": True, "mode": "authenticated_user_events"}

    def parse_order_intent(self, body: Mapping[str, Any]) -> OrderIntent:
        symbol = str(body.get("symbol", "BTCUSDT")).upper()
        if symbol != "BTCUSDT":
            raise SafetyBlocked("dashboard currently allows BTCUSDT only")
        side = str(body.get("side", "BUY")).upper()
        order_type = str(body.get("order_type", body.get("type", "MARKET"))).upper()
        raw_quantity = body.get("quantity")
        raw_quote_quantity = body.get("quote_order_qty", body.get("quoteOrderQty"))
        quantity = _decimal(raw_quantity, "quantity") if raw_quantity not in (None, "") else None
        quote_quantity = _decimal(raw_quote_quantity, "quote_order_qty") if raw_quote_quantity not in (None, "") else None
        price = _decimal(body["price"], "price") if body.get("price") not in (None, "") else None
        return OrderIntent(
            symbol=symbol,
            side=side,
            order_type=order_type,
            quantity=quantity,
            price=price,
            time_in_force=str(body.get("time_in_force", "GTC")) if body.get("time_in_force") else "GTC",
            client_order_id=str(body["client_order_id"]) if body.get("client_order_id") else None,
            quote_order_qty=quote_quantity,
        )

    async def test_order(self, body: Mapping[str, Any]) -> dict[str, Any]:
        if self.rest is None:
            raise RuntimeError("REST client is not initialized")
        intent = self.parse_order_intent(body)
        result = await asyncio.to_thread(self.rest.test_order, intent)
        return {"sent_to_matching_engine": False, "result": result}

    async def place_order(self, body: Mapping[str, Any]) -> dict[str, Any]:
        if str(body.get("confirmation", "")) != "I_UNDERSTAND_THIS_CAN_USE_REAL_MONEY":
            raise SafetyBlocked("dashboard live-order confirmation is missing")
        if self.rest is None:
            raise RuntimeError("REST client is not initialized")
        if self.stream is None:
            raise SafetyBlocked("start the live market connection before an order")
        intent = self.parse_order_intent(body)
        result = await asyncio.to_thread(self.rest.place_order, intent, state=self.stream.state)
        return {"sent_to_matching_engine": True, "result": result}

    async def engage_kill_switch(self, cancel_orders: bool = True) -> dict[str, Any]:
        kill = KillSwitch(self.config.kill_switch_path)
        kill.engage("dashboard emergency stop")
        await self.stop_engine("emergency kill switch")
        await self.stop_stream()
        cancelled: Any = None
        cancel_error: str | None = None
        if cancel_orders and self.rest is not None and self.config.has_credentials:
            try:
                cancelled = await asyncio.to_thread(self.rest.cancel_all, "BTCUSDT")
            except Exception as exc:
                cancel_error = f"{type(exc).__name__}: {exc}"
        return {
            "kill_switch_active": True,
            "new_orders_blocked": True,
            "market_stream_stopped": True,
            "cancel_attempted": cancel_orders and self.config.has_credentials,
            "cancelled_orders": cancelled,
            "cancel_error": cancel_error,
            "note": "The stop file is sticky. Remove it manually only after an independent review.",
        }

    async def run_research_tests(self) -> dict[str, Any]:
        command = [
            sys.executable,
            "-m",
            "unittest",
            "discover",
            "-s",
            str(RESEARCH_ROOT),
            "-p",
            "test_*.py",
        ]
        completed = await asyncio.to_thread(
            subprocess.run,
            command,
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=180,
            check=False,
        )
        output = (completed.stdout + "\n" + completed.stderr).strip()
        return {
            "ok": completed.returncode == 0,
            "return_code": completed.returncode,
            "output_tail": output[-6000:],
            "command": command,
        }


runtime = DashboardRuntime()
app = FastAPI(title="Pocket Quant Control Room", version="1.0.0")
app.mount("/assets", StaticFiles(directory=DASHBOARD_ROOT), name="assets")


@app.middleware("http")
async def local_only(request: Request, call_next: Callable[..., Awaitable[Any]]):
    """Keep the control surface local unless an operator changes the code."""
    client_host = request.client.host if request.client else None
    try:
        local = client_host is not None and ipaddress.ip_address(client_host).is_loopback
    except ValueError:
        local = False
    if not local:
        return __import__("fastapi").responses.JSONResponse(
            {"error": "dashboard is local-only"}, status_code=403
        )
    return await call_next(request)


@app.on_event("shutdown")
async def shutdown_runtime() -> None:
    await runtime.stop_engine("dashboard shutdown")
    await runtime.stop_stream()
    if runtime.user_stream_task is not None and not runtime.user_stream_task.done():
        runtime.user_stream_task.cancel()
        await asyncio.gather(runtime.user_stream_task, return_exceptions=True)


@app.get("/", response_class=FileResponse)
async def index() -> FileResponse:
    return FileResponse(DASHBOARD_ROOT / "index.html")


@app.get("/api/state")
async def state() -> dict[str, Any]:
    return await runtime.snapshot()


@app.get("/api/tutorial")
async def tutorial() -> dict[str, Any]:
    return {
        "title": "How to use the little trading robot",
        "steps": [
            {"number": 1, "title": "Look", "text": "Press Watch Market. The robot reads Binance prices and the order book. It does not spend money."},
            {"number": 2, "title": "Check", "text": "Press Safety Check. The robot checks its files, rules, and research tests."},
            {"number": 3, "title": "Learn", "text": "Read the big cards. A green card means a check worked. A yellow card means we still need more evidence."},
            {"number": 4, "title": "Stop", "text": "The red Stop Everything button stops new work and leaves a stop file that must be reviewed by a person."},
            {"number": 5, "title": "Real money", "text": "Real orders stay locked until the research gate, account permissions, market data, and human approval all agree."},
        ],
    }


@app.get("/api/action/{action_id}")
async def action_status(action_id: str) -> dict[str, Any]:
    record = runtime.actions.get(action_id)
    if record is None:
        raise HTTPException(status_code=404, detail="action not found")
    return record.as_dict()


@app.post("/api/actions/public-check")
async def public_check() -> dict[str, Any]:
    record = await runtime.record_action("Check Binance market", runtime.public_check)
    return record.as_dict()


@app.post("/api/actions/connect-market")
async def connect_market() -> dict[str, Any]:
    async def operation() -> dict[str, Any]:
        public = await runtime.public_check()
        stream = await runtime.start_stream(("1m",))
        return {"public": public, "stream": stream}

    record = await runtime.record_action("Connect to Binance market", operation)
    return record.as_dict()


@app.post("/api/actions/start-safe-mode")
async def start_safe_mode() -> dict[str, Any]:
    record = await runtime.record_action("Start safe observation mode", lambda: runtime.start_engine("PAPER"))
    return record.as_dict()


@app.post("/api/actions/start-engine")
async def start_engine(request: Request) -> dict[str, Any]:
    body = await request.json() if request.headers.get("content-length") else {}
    mode = str(body.get("mode", "PAPER")).upper() if isinstance(body, Mapping) else "PAPER"
    duration = body.get("duration_minutes") if isinstance(body, Mapping) else None
    duration_value = int(duration) if duration not in (None, "") else None
    record = await runtime.record_action(
        f"Start {mode} engine",
        lambda: runtime.start_engine(mode, duration_value),
    )
    return record.as_dict()


@app.post("/api/actions/stop-engine")
async def stop_engine() -> dict[str, Any]:
    record = await runtime.record_action("Stop trading engine", runtime.stop_engine)
    return record.as_dict()


@app.get("/api/engine")
async def engine_status() -> dict[str, Any]:
    return await runtime.engine_status()


@app.post("/api/actions/stop-market")
async def stop_market() -> dict[str, Any]:
    record = await runtime.record_action("Stop market connection", runtime.stop_stream)
    return record.as_dict()


@app.post("/api/actions/account-check")
async def account_check() -> dict[str, Any]:
    record = await runtime.record_action("Check account", runtime.account_check)
    return record.as_dict()


@app.post("/api/actions/start-account-events")
async def start_account_events() -> dict[str, Any]:
    record = await runtime.record_action("Start account event watch", runtime.start_user_stream)
    return record.as_dict()


@app.post("/api/actions/test-order")
async def test_order(request: Request) -> dict[str, Any]:
    body = await request.json()
    record = await runtime.record_action("Test order without real money", lambda: runtime.test_order(body))
    return record.as_dict()


@app.post("/api/actions/place-order")
async def place_order(request: Request) -> dict[str, Any]:
    body = await request.json()
    record = await runtime.record_action("Guarded live order", lambda: runtime.place_order(body))
    return record.as_dict()


@app.post("/api/actions/kill-switch")
async def kill_switch(request: Request) -> dict[str, Any]:
    # Engage the file synchronously before creating an action record. If the
    # process is interrupted immediately after this handler begins, every
    # subsequent live-order gate still observes the stop.
    KillSwitch(runtime.config.kill_switch_path).engage("dashboard emergency stop")
    body = await request.json() if request.headers.get("content-length") else {}
    cancel_orders = bool(body.get("cancel_orders", True)) if isinstance(body, Mapping) else True
    record = await runtime.record_action("Emergency stop", lambda: runtime.engage_kill_switch(cancel_orders))
    return record.as_dict()


@app.post("/api/actions/run-research-tests")
async def run_research_tests() -> dict[str, Any]:
    record = await runtime.record_action("Run research checks", runtime.run_research_tests)
    return record.as_dict()


@app.websocket("/ws")
async def dashboard_socket(websocket: WebSocket) -> None:
    await websocket.accept()
    runtime.sockets.add(websocket)
    try:
        await websocket.send_json(await runtime.snapshot())
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        runtime.sockets.discard(websocket)


if __name__ == "__main__":  # pragma: no cover
    import uvicorn

    uvicorn.run("research.dashboard_app:app", host="127.0.0.1", port=int(os.environ.get("DASHBOARD_PORT", "8787")), reload=False)
