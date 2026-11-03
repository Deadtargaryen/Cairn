#!/usr/bin/env python3
"""Small, fail-closed real-time Binance trading engine.

The engine consumes the already-normalized market state from
``binance_live.BinanceMarketStream``. Deterministic code aggregates trades,
depth, price, volume, spread, and volatility into a compact feature snapshot;
an optional GPT-compatible reasoning adapter receives that snapshot and must
return a strict decision contract. Missing/invalid reasoning means
``NO_TRADE``. The paper executor uses the same decision and risk pipeline as
the live executor, while only the latter can call Binance order endpoints.

The default mode is PAPER and the default strategy is observation-only. Live
orders require the independent Binance safety gates in ``binance_live`` plus
engine-level limits and a fresh synchronized order book.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import hashlib
import json
import math
import os
import statistics
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any, Awaitable, Callable, Deque, Iterable, Mapping, Protocol

try:  # Load project .env before reading configuration variables.
    from . import config as _config
except ImportError:  # pragma: no cover - direct script execution
    import config as _config  # type: ignore

try:
    from .binance_live import (
        BinanceConfig,
        BinanceMarketStream,
        BinanceRestClient,
        BinanceError,
        ExecutionStatusUnknown,
        MarketState,
        OrderIntent,
        SafetyBlocked,
        _decimal,
        _decimal_text,
        _now_ms,
    )
except ImportError:  # pragma: no cover
    from binance_live import (  # type: ignore
        BinanceConfig,
        BinanceMarketStream,
        BinanceRestClient,
        BinanceError,
        ExecutionStatusUnknown,
        MarketState,
        OrderIntent,
        SafetyBlocked,
        _decimal,
        _decimal_text,
        _now_ms,
    )


UTC = dt.timezone.utc
ENGINE_VERSION = "live-engine-v1"
DECISION_CONFIRMATION = "I_UNDERSTAND_THIS_CAN_USE_REAL_MONEY"


def utc_now() -> str:
    return dt.datetime.now(UTC).isoformat()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


@dataclass(frozen=True)
class EngineConfig:
    mode: str = "PAPER"
    symbol: str = "BTCUSDT"
    session_minutes: int = 720
    decision_cooldown_ms: int = 5_000
    minimum_trade_interval_ms: int = 30_000
    max_market_age_ms: int = 5_000
    max_spread_bps: float = 20.0
    max_realized_vol: float = 0.01
    max_position_quote: float = 2.0
    max_total_exposure_quote: float = 2.0
    max_daily_loss_quote: float = 0.50
    max_consecutive_losses: int = 3
    minimum_expected_net_profit_quote: float = 0.01
    minimum_confidence: float = 0.70
    fee_rate: float = 0.001
    half_spread_bps: float = 5.0
    slippage_bps: float = 5.0
    impact_bps: float = 2.0
    live_order_timeout_s: float = 10.0
    paper_fee_rate: float = 0.001
    paper_slippage_bps: float = 5.0
    paper_latency_ms: int = 250
    paper_initial_quote: float = 3.0
    paper_initial_base: float = 0.0
    max_order_retries: int = 0
    journal_path: Path = Path("research/runtime/trade_journal.jsonl")
    state_path: Path = Path("research/runtime/engine_state.json")
    runtime_root: Path = Path("research/runtime")
    llm_enabled: bool = False
    llm_base_url: str = ""
    llm_model: str = ""
    llm_api_key_env: str = "OPENAI_API_KEY"
    knowledge_base_path: Path = Path("research/knowledge_base_references.json")
    assumptions_path: Path = Path("research/research_assumptions.json")

    @classmethod
    def from_env(cls) -> "EngineConfig":
        return cls(
            mode=os.environ.get("ENGINE_MODE", "PAPER").upper(),
            symbol=os.environ.get("ENGINE_SYMBOL", "BTCUSDT").upper(),
            session_minutes=int(os.environ.get("ENGINE_SESSION_MINUTES", "720")),
            decision_cooldown_ms=int(os.environ.get("ENGINE_DECISION_COOLDOWN_MS", "5000")),
            minimum_trade_interval_ms=int(os.environ.get("ENGINE_MINIMUM_TRADE_INTERVAL_MS", "30000")),
            max_market_age_ms=int(os.environ.get("ENGINE_MAX_MARKET_AGE_MS", "5000")),
            max_spread_bps=float(os.environ.get("ENGINE_MAX_SPREAD_BPS", "20")),
            max_realized_vol=float(os.environ.get("ENGINE_MAX_REALIZED_VOL", "0.01")),
            max_position_quote=float(os.environ.get("ENGINE_MAX_POSITION_QUOTE", "2")),
            max_total_exposure_quote=float(os.environ.get("ENGINE_MAX_TOTAL_EXPOSURE_QUOTE", "2")),
            max_daily_loss_quote=float(os.environ.get("ENGINE_MAX_DAILY_LOSS_QUOTE", "0.50")),
            max_consecutive_losses=int(os.environ.get("ENGINE_MAX_CONSECUTIVE_LOSSES", "3")),
            minimum_expected_net_profit_quote=float(os.environ.get("ENGINE_MIN_EXPECTED_NET_PROFIT", "0.01")),
            minimum_confidence=float(os.environ.get("ENGINE_MIN_CONFIDENCE", "0.70")),
            fee_rate=float(os.environ.get("ENGINE_FEE_RATE", "0.001")),
            half_spread_bps=float(os.environ.get("ENGINE_HALF_SPREAD_BPS", "5")),
            slippage_bps=float(os.environ.get("ENGINE_SLIPPAGE_BPS", "5")),
            impact_bps=float(os.environ.get("ENGINE_IMPACT_BPS", "2")),
            live_order_timeout_s=float(os.environ.get("ENGINE_LIVE_ORDER_TIMEOUT_S", "10")),
            paper_fee_rate=float(os.environ.get("ENGINE_PAPER_FEE_RATE", "0.001")),
            paper_slippage_bps=float(os.environ.get("ENGINE_PAPER_SLIPPAGE_BPS", "5")),
            paper_latency_ms=int(os.environ.get("ENGINE_PAPER_LATENCY_MS", "250")),
            paper_initial_quote=float(os.environ.get("ENGINE_PAPER_INITIAL_QUOTE", "3")),
            paper_initial_base=float(os.environ.get("ENGINE_PAPER_INITIAL_BASE", "0")),
            max_order_retries=int(os.environ.get("ENGINE_MAX_ORDER_RETRIES", "0")),
            journal_path=Path(os.environ.get("ENGINE_JOURNAL_PATH", "research/runtime/trade_journal.jsonl")),
            state_path=Path(os.environ.get("ENGINE_STATE_PATH", "research/runtime/engine_state.json")),
            runtime_root=Path(os.environ.get("ENGINE_RUNTIME_ROOT", "research/runtime")),
            llm_enabled=os.environ.get("ENGINE_LLM_ENABLED", "false").lower() in {"1", "true", "yes", "on"},
            llm_base_url=os.environ.get("ENGINE_LLM_BASE_URL", ""),
            llm_model=os.environ.get("ENGINE_LLM_MODEL", ""),
            llm_api_key_env=os.environ.get("ENGINE_LLM_API_KEY_ENV", "OPENAI_API_KEY"),
            knowledge_base_path=Path(os.environ.get("ENGINE_KNOWLEDGE_BASE_PATH", "research/knowledge_base_references.json")),
            assumptions_path=Path(os.environ.get("ENGINE_ASSUMPTIONS_PATH", "research/research_assumptions.json")),
        )

    def validate(self) -> None:
        if self.mode not in {"PAPER", "LIVE"}:
            raise SafetyBlocked(f"ENGINE_MODE must be PAPER or LIVE, got {self.mode}")
        if self.session_minutes <= 0:
            raise SafetyBlocked("engine session duration must be positive")
        if self.max_position_quote <= 0 or self.max_total_exposure_quote <= 0:
            raise SafetyBlocked("position and exposure limits must be positive")
        if self.max_position_quote > self.max_total_exposure_quote:
            raise SafetyBlocked("position limit exceeds total exposure limit")
        if not 0 <= self.minimum_confidence <= 1:
            raise SafetyBlocked("minimum confidence must be between zero and one")
        if self.max_daily_loss_quote <= 0 or self.max_consecutive_losses <= 0:
            raise SafetyBlocked("loss limits must be positive")
        if self.max_order_retries < 0:
            raise SafetyBlocked("maximum order retries cannot be negative")
        if self.mode == "LIVE" and self.paper_initial_quote < 0:
            raise SafetyBlocked("invalid paper balance configuration")


@dataclass(frozen=True)
class MarketFeatures:
    timestamp_ms: int
    symbol: str
    last_price: float | None
    best_bid: float | None
    best_ask: float | None
    mid_price: float | None
    spread_bps: float | None
    bid_depth: float
    ask_depth: float
    depth_imbalance: float | None
    trade_count: int
    buy_volume: float
    sell_volume: float
    trade_imbalance: float | None
    return_1s: float | None
    return_5s: float | None
    realized_vol: float | None
    volume_rate: float | None
    regime: str
    data_quality: str
    data_age_ms: int | None = None

    def as_dict(self) -> dict[str, Any]:
        return self.__dict__.copy()


@dataclass(frozen=True)
class Opportunity:
    opportunity_id: str
    timestamp_ms: int
    strategy: str
    direction: str
    strength: float
    horizon_seconds: int
    expected_gross_return: float
    expected_net_profit_quote: float
    expected_adverse_move: float
    expected_favorable_move: float
    invalidation: str
    features: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return self.__dict__.copy()


@dataclass(frozen=True)
class Decision:
    action: str
    symbol: str
    confidence: float
    entry: float | None
    stop: float | None
    target: float | None
    position_size_quote: float
    expected_net_profit: float
    expected_risk: float
    reason: str
    strategy: str
    max_hold_seconds: int
    invalidation: str
    source: str = "deterministic_fallback"

    @classmethod
    def no_trade(cls, symbol: str, reason: str, strategy: str = "none") -> "Decision":
        return cls("HOLD", symbol, 0.0, None, None, None, 0.0, 0.0, 0.0, reason, strategy, 0, reason)

    def as_dict(self) -> dict[str, Any]:
        return self.__dict__.copy()

    def validate_for_spot(self, symbol: str) -> None:
        if self.symbol != symbol:
            raise ValueError("decision symbol does not match engine symbol")
        if self.action not in {"BUY", "SELL", "HOLD"}:
            raise ValueError("decision action is invalid")
        if not 0 <= self.confidence <= 1:
            raise ValueError("decision confidence is outside [0, 1]")
        for value in (self.position_size_quote, self.expected_net_profit, self.expected_risk):
            if not math.isfinite(value) or value < 0:
                raise ValueError("decision contains invalid numeric values")
        if self.action == "SELL":
            raise ValueError("spot engine does not allow naked short decisions")
        if self.action == "BUY" and self.position_size_quote <= 0:
            raise ValueError("BUY decision has no position size")


class Reasoner(Protocol):
    async def decide(self, market: MarketFeatures, opportunity: Opportunity | None) -> Decision:
        ...


class NoTradeReasoner:
    async def decide(self, market: MarketFeatures, opportunity: Opportunity | None) -> Decision:
        return Decision.no_trade(
            market.symbol,
            "No external reasoning adapter is enabled; fail-closed observation mode.",
            opportunity.strategy if opportunity else "none",
        )


class DeterministicReasoner:
    """A small, auditable fallback reasoner for PAPER mode.

    It only converts an already detected opportunity into a bounded decision;
    it does not invent an opportunity and is never permitted to authorize a
    live order by itself.
    """

    def __init__(self, config: EngineConfig):
        self.config = config

    async def decide(self, market: MarketFeatures, opportunity: Opportunity | None) -> Decision:
        if opportunity is None:
            return Decision.no_trade(market.symbol, "No validated short-horizon opportunity.")
        if opportunity.direction != "BUY":
            return Decision.no_trade(market.symbol, "Spot mode cannot open a short position.", opportunity.strategy)
        if opportunity.expected_net_profit_quote < self.config.minimum_expected_net_profit_quote:
            return Decision.no_trade(market.symbol, "Expected net profit is below the configured cost hurdle.", opportunity.strategy)
        entry = market.best_ask or market.mid_price
        if entry is None or entry <= 0:
            return Decision.no_trade(market.symbol, "No executable ask is available.", opportunity.strategy)
        adverse = max(opportunity.expected_adverse_move, 0.0005)
        favorable = max(opportunity.expected_favorable_move, adverse * 1.25)
        stop = entry * (1.0 - adverse)
        target = entry * (1.0 + favorable)
        confidence = min(0.99, max(self.config.minimum_confidence, 0.5 + opportunity.strength / 2.0))
        return Decision(
            action="BUY",
            symbol=market.symbol,
            confidence=confidence,
            entry=entry,
            stop=stop,
            target=target,
            position_size_quote=min(self.config.max_position_quote, self.config.max_total_exposure_quote),
            expected_net_profit=opportunity.expected_net_profit_quote,
            expected_risk=self.config.max_position_quote * adverse,
            reason=f"Deterministic confirmation: {opportunity.strategy}; flow/depth agreement.",
            strategy=opportunity.strategy,
            max_hold_seconds=opportunity.horizon_seconds,
            invalidation=opportunity.invalidation,
            source="deterministic",
        )


class OpenAICompatibleReasoner:
    """Minimal JSON-only adapter; credentials are read only at call time."""

    def __init__(self, config: EngineConfig, knowledge_context: dict[str, Any]):
        self.config = config
        self.knowledge_context = knowledge_context

    async def decide(self, market: MarketFeatures, opportunity: Opportunity | None) -> Decision:
        if not self.config.llm_enabled or not self.config.llm_base_url or not self.config.llm_model:
            return await NoTradeReasoner().decide(market, opportunity)
        api_key = os.environ.get(self.config.llm_api_key_env)
        if not api_key:
            return Decision.no_trade(market.symbol, "LLM API key is not configured; fail closed.")
        try:
            import urllib.request

            prompt = {
                "role": "system",
                "content": (
                    "You are the reasoning layer of a risk-controlled crypto engine. "
                    "Return JSON only with action BUY, SELL, or HOLD. Never invent data. "
                    "HOLD when expected net value is not clearly positive or data is stale. "
                    "The deterministic engine independently enforces all risk limits."
                ),
            }
            user = {
                "role": "user",
                "content": json.dumps(
                    {
                        "market": market.as_dict(),
                        "opportunity": opportunity.as_dict() if opportunity else None,
                        "knowledge_base_context": self.knowledge_context,
                        "required_fields": [
                            "action",
                            "confidence",
                            "entry",
                            "stop",
                            "target",
                            "position_size_quote",
                            "expected_net_profit",
                            "expected_risk",
                            "reason",
                            "strategy",
                            "max_hold_seconds",
                            "invalidation",
                        ],
                    },
                    sort_keys=True,
                ),
            }
            payload = json.dumps({"model": self.config.llm_model, "messages": [prompt, user], "temperature": 0}).encode()
            request = urllib.request.Request(
                self.config.llm_base_url.rstrip("/") + "/chat/completions",
                data=payload,
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                method="POST",
            )
            response = await asyncio.to_thread(urllib.request.urlopen, request, timeout=8)
            body = json.loads(response.read().decode())
            content = body["choices"][0]["message"]["content"]
            parsed = json.loads(content)
            return decision_from_mapping(parsed, market.symbol, "gpt")
        except Exception as exc:
            return Decision.no_trade(market.symbol, f"LLM response invalid or unavailable: {type(exc).__name__}")


def decision_from_mapping(value: Mapping[str, Any], symbol: str, source: str) -> Decision:
    action = str(value.get("action", "HOLD")).upper()
    if action not in {"BUY", "SELL", "HOLD"}:
        raise ValueError("decision action must be BUY, SELL, or HOLD")
    confidence = float(value.get("confidence", 0.0))
    size = float(value.get("position_size_quote", 0.0))
    expected = float(value.get("expected_net_profit", 0.0))
    risk = float(value.get("expected_risk", 0.0))
    if not all(math.isfinite(x) for x in (confidence, size, expected, risk)):
        raise ValueError("decision contains non-finite numeric values")
    if not 0 <= confidence <= 1 or size < 0 or expected < 0 or risk < 0:
        raise ValueError("decision numeric bounds are invalid")
    if action == "HOLD":
        size = 0.0
    if action == "BUY" and size > 0 and (value.get("stop") is None or value.get("target") is None):
        raise ValueError("BUY decisions require stop and target levels")
    return Decision(
        action=action,
        symbol=symbol,
        confidence=confidence,
        entry=float(value["entry"]) if value.get("entry") is not None else None,
        stop=float(value["stop"]) if value.get("stop") is not None else None,
        target=float(value["target"]) if value.get("target") is not None else None,
        position_size_quote=size,
        expected_net_profit=expected,
        expected_risk=risk,
        reason=str(value.get("reason", "No reason supplied"))[:1000],
        strategy=str(value.get("strategy", "unknown"))[:100],
        max_hold_seconds=max(0, int(value.get("max_hold_seconds", 0))),
        invalidation=str(value.get("invalidation", "unspecified"))[:500],
        source=source,
    )


class FeatureEngine:
    """Deterministic bounded-memory market feature aggregator."""

    def __init__(self, symbol: str, max_events: int = 2_000):
        self.symbol = symbol
        self.trades: Deque[tuple[int, float, float, bool]] = deque(maxlen=max_events)
        self.prices: Deque[tuple[int, float]] = deque(maxlen=max_events)
        self.last_features: MarketFeatures | None = None

    def ingest_event(self, event: Mapping[str, Any], state: MarketState) -> MarketFeatures:
        event_type = event.get("e")
        event_ms = int(event.get("E", _now_ms()))
        if event_type in {"aggTrade", "trade"}:
            price = float(event.get("p", 0))
            quantity = float(event.get("q", 0))
            if price > 0 and quantity >= 0:
                self.trades.append((event_ms, price, quantity, bool(event.get("m", False))))
                self.prices.append((event_ms, price))
        elif event_type == "kline":
            kline = event.get("k", event)
            close = float(kline.get("c", 0)) if kline.get("c") is not None else 0
            if close > 0:
                self.prices.append((event_ms, close))
        return self.features(state, event_ms)

    def features(self, state: MarketState, now_ms: int | None = None, max_market_age_ms: int = 5_000) -> MarketFeatures:
        now = now_ms or _now_ms()
        book = state.order_book
        bid = float(book.best_bid[0]) if book.best_bid else None
        ask = float(book.best_ask[0]) if book.best_ask else None
        mid = (bid + ask) / 2 if bid is not None and ask is not None else None
        spread_bps = ((ask - bid) / mid * 10_000) if bid is not None and ask is not None and mid else None
        bid_depth = float(sum(float(qty) for _, qty in sorted(book.bids.items(), reverse=True)[:10]))
        ask_depth = float(sum(float(qty) for _, qty in sorted(book.asks.items())[:10]))
        total_depth = bid_depth + ask_depth
        imbalance = (bid_depth - ask_depth) / total_depth if total_depth else None
        recent = [item for item in self.trades if now - item[0] <= 5_000]
        buy_volume = sum(quantity for _, _, quantity, buyer_maker in recent if not buyer_maker)
        sell_volume = sum(quantity for _, _, quantity, buyer_maker in recent if buyer_maker)
        total_volume = buy_volume + sell_volume
        trade_imbalance = (buy_volume - sell_volume) / total_volume if total_volume else None
        def price_at_or_before(age_ms: int) -> float | None:
            target = now - age_ms
            for timestamp, price in reversed(self.prices):
                if timestamp <= target:
                    return price
            return None
        current_price = float(self.trades[-1][1]) if self.trades else (mid or None)
        p1 = price_at_or_before(1_000)
        p5 = price_at_or_before(5_000)
        return_1s = current_price / p1 - 1 if current_price and p1 else None
        return_5s = current_price / p5 - 1 if current_price and p5 else None
        recent_prices = [price for timestamp, price in self.prices if now - timestamp <= 5_000]
        log_returns = [math.log(b / a) for a, b in zip(recent_prices, recent_prices[1:]) if a > 0 and b > 0]
        realized_vol = statistics.pstdev(log_returns) if len(log_returns) > 1 else None
        data_age = state.freshness_ms()
        quality = "ok"
        if data_age is None or data_age > max_market_age_ms or book.stale or not book.stream_synced:
            quality = "stale_or_unsynchronized"
        if spread_bps is not None and spread_bps > 100:
            quality = "abnormal_spread"
        regime = self._regime(return_5s, realized_vol, trade_imbalance)
        features = MarketFeatures(
            timestamp_ms=now,
            symbol=self.symbol,
            last_price=current_price,
            best_bid=bid,
            best_ask=ask,
            mid_price=mid,
            spread_bps=spread_bps,
            bid_depth=bid_depth,
            ask_depth=ask_depth,
            depth_imbalance=imbalance,
            trade_count=len(recent),
            buy_volume=buy_volume,
            sell_volume=sell_volume,
            trade_imbalance=trade_imbalance,
            return_1s=return_1s,
            return_5s=return_5s,
            realized_vol=realized_vol,
            volume_rate=total_volume / 5 if total_volume else None,
            regime=regime,
            data_quality=quality,
            data_age_ms=data_age,
        )
        self.last_features = features
        return features

    @staticmethod
    def _regime(return_5s: float | None, realized_vol: float | None, imbalance: float | None) -> str:
        if return_5s is None:
            return "UNKNOWN"
        if return_5s <= -0.01:
            return "PANIC"
        if return_5s >= 0.01:
            return "EUPHORIA"
        if realized_vol is not None and realized_vol >= 0.003:
            return "HIGH_VOLATILITY"
        if imbalance is not None and abs(imbalance) >= 0.6:
            return "ORDERBOOK_IMBALANCED"
        if abs(return_5s) < 0.001:
            return "SIDEWAYS"
        return "TRENDING_UP" if return_5s > 0 else "TRENDING_DOWN"


class OpportunityDetector:
    """Fixed, explainable short-horizon candidates; no forced trades."""

    def __init__(self, config: EngineConfig):
        self.config = config

    def detect(self, features: MarketFeatures) -> Opportunity | None:
        if features.data_quality != "ok":
            return None
        if features.spread_bps is None or features.spread_bps > self.config.max_spread_bps:
            return None
        if features.last_price is None or features.best_ask is None or features.best_bid is None:
            return None
        if features.realized_vol is not None and features.realized_vol > self.config.max_realized_vol:
            return None
        candidates: list[Opportunity] = []
        if features.return_5s is not None and features.trade_imbalance is not None:
            if features.return_5s >= 0.001 and features.trade_imbalance >= 0.25 and (features.depth_imbalance or 0) >= 0:
                candidates.append(self._candidate(features, "momentum_flow", "BUY", min(1.0, features.return_5s * 100), 0.0015))
            # Binance Spot does not permit a naked short entry. Reversal is
            # represented as a future long setup only after selling pressure
            # has exhausted; an exit is handled by the position manager.
            if features.return_5s <= -0.001 and features.trade_imbalance >= 0.25 and (features.depth_imbalance or 0) >= 0:
                candidates.append(self._candidate(features, "reversal_flow", "BUY", min(1.0, abs(features.return_5s) * 100), 0.001))
        if features.depth_imbalance is not None and abs(features.depth_imbalance) >= 0.55 and features.trade_imbalance is not None:
            direction = "BUY" if features.depth_imbalance > 0 and features.trade_imbalance > 0 else "HOLD"
            if direction != "HOLD":
                candidates.append(self._candidate(features, "depth_pressure", direction, abs(features.depth_imbalance), 0.001))
        if not candidates:
            return None
        return max(candidates, key=lambda candidate: candidate.strength)

    def _candidate(self, features: MarketFeatures, strategy: str, direction: str, strength: float, gross_return: float) -> Opportunity:
        round_trip_proxy = 2 * (
            self.config.fee_rate
            + (features.spread_bps or 0) / 20_000
            + self.config.slippage_bps / 10_000
            + self.config.impact_bps / 10_000
        )
        expected_net = (gross_return - round_trip_proxy) * self.config.max_position_quote
        return Opportunity(
            opportunity_id=uuid.uuid4().hex[:16],
            timestamp_ms=features.timestamp_ms,
            strategy=strategy,
            direction=direction,
            strength=strength,
            horizon_seconds=30,
            expected_gross_return=gross_return,
            expected_net_profit_quote=expected_net,
            expected_adverse_move=round_trip_proxy,
            expected_favorable_move=gross_return,
            invalidation="spread or data quality exceeds configured limit",
            features=features.as_dict(),
        )


@dataclass
class RiskState:
    daily_loss_quote: float = 0.0
    consecutive_losses: int = 0
    open_exposure_quote: float = 0.0
    halted: bool = False
    halt_reason: str | None = None
    utc_day: str = field(default_factory=lambda: dt.datetime.now(UTC).date().isoformat())


class RiskManager:
    def __init__(self, config: EngineConfig):
        self.config = config
        self.state = RiskState()

    def halt(self, reason: str) -> None:
        self.state.halted = True
        self.state.halt_reason = reason

    def reset_if_new_utc_day(self, now: dt.datetime | None = None) -> None:
        current = (now or dt.datetime.now(UTC)).astimezone(UTC).date().isoformat()
        if current != self.state.utc_day:
            self.state.daily_loss_quote = 0.0
            self.state.consecutive_losses = 0
            self.state.utc_day = current
            if self.state.halt_reason in {"maximum daily loss reached", "maximum consecutive losses reached"}:
                self.state.halted = False
                self.state.halt_reason = None

    def validate(
        self,
        decision: Decision,
        market: MarketFeatures,
        *,
        live: bool,
        has_position: bool = False,
    ) -> tuple[bool, str]:
        self.reset_if_new_utc_day()
        if self.state.halted:
            return False, f"risk halted: {self.state.halt_reason}"
        if decision.symbol != self.config.symbol:
            return False, "symbol mismatch"
        if decision.action not in {"BUY", "SELL", "HOLD"}:
            return False, "invalid action"
        if decision.action == "HOLD":
            return True, "hold"
        if market.data_quality != "ok":
            return False, "market data is stale or unsynchronized"
        if market.spread_bps is None or market.spread_bps > self.config.max_spread_bps:
            return False, "spread limit exceeded"
        if market.realized_vol is not None and market.realized_vol > self.config.max_realized_vol:
            return False, "volatility protection"
        if decision.action == "BUY" and has_position:
            return False, "spot position already open"
        if decision.action == "SELL" and not has_position:
            return False, "spot cannot open a naked short"
        if decision.position_size_quote <= 0 or decision.position_size_quote > self.config.max_position_quote:
            return False, "position size limit"
        if self.state.open_exposure_quote + decision.position_size_quote > self.config.max_total_exposure_quote:
            return False, "total exposure limit"
        if decision.expected_net_profit < self.config.minimum_expected_net_profit_quote:
            return False, "expected net profit below hurdle"
        if decision.confidence < self.config.minimum_confidence:
            return False, "confidence below threshold"
        if live and self.config.mode != "LIVE":
            return False, "engine is not in LIVE mode"
        return True, "approved"

    def validate_engine_config(self) -> None:
        if self.config.mode not in {"PAPER", "LIVE"}:
            raise SafetyBlocked(f"ENGINE_MODE must be PAPER or LIVE, got {self.config.mode}")
        if self.config.session_minutes <= 0:
            raise SafetyBlocked("session duration must be positive")
        if self.config.max_position_quote <= 0 or self.config.max_total_exposure_quote <= 0:
            raise SafetyBlocked("position and exposure limits must be positive")
        if self.config.max_total_exposure_quote < self.config.max_position_quote:
            raise SafetyBlocked("total exposure limit cannot be below position limit")
        if not 0 <= self.config.minimum_confidence <= 1:
            raise SafetyBlocked("minimum confidence must be between zero and one")

    def register_result(self, pnl_quote: float) -> None:
        if pnl_quote < 0:
            self.state.daily_loss_quote += -pnl_quote
            self.state.consecutive_losses += 1
        else:
            self.state.consecutive_losses = 0
        if self.state.daily_loss_quote >= self.config.max_daily_loss_quote:
            self.halt("maximum daily loss reached")
        elif self.state.consecutive_losses >= self.config.max_consecutive_losses:
            self.halt("maximum consecutive losses reached")


@dataclass
class OpenPosition:
    position_id: str
    side: str
    quantity: float
    entry_price: float
    entry_time_ms: int
    expected_exit_ms: int
    stop: float | None
    target: float | None
    entry_cost: float
    mode: str


class Executor(Protocol):
    async def enter(
        self,
        decision: Decision,
        market: MarketFeatures,
        state: MarketState | None = None,
    ) -> OpenPosition | None:
        ...

    async def exit(
        self,
        position: OpenPosition,
        market: MarketFeatures,
        reason: str,
        state: MarketState | None = None,
    ) -> dict[str, Any]:
        ...


class PaperExecutor:
    def __init__(self, config: EngineConfig):
        self.config = config
        self.quote_balance = float(config.paper_initial_quote)
        self.base_balance = float(config.paper_initial_base)
        self.total_fees = 0.0

    async def enter(
        self,
        decision: Decision,
        market: MarketFeatures,
        state: MarketState | None = None,
    ) -> OpenPosition | None:
        if market.best_ask is None or market.best_bid is None:
            return None
        raw_price = market.best_ask if decision.action == "BUY" else market.best_bid
        slip = self.config.paper_slippage_bps / 10_000
        price = raw_price * (1 + slip if decision.action == "BUY" else 1 - slip)
        requested_quote = min(float(decision.position_size_quote), self.quote_balance)
        if requested_quote <= 0:
            return None
        quantity = requested_quote / price
        fee = requested_quote * self.config.paper_fee_rate
        if requested_quote + fee > self.quote_balance:
            requested_quote = self.quote_balance / (1 + self.config.paper_fee_rate)
            quantity = requested_quote / price
            fee = requested_quote * self.config.paper_fee_rate
        self.quote_balance -= requested_quote + fee
        self.base_balance += quantity
        self.total_fees += fee
        return OpenPosition(
            position_id=uuid.uuid4().hex[:16],
            side=decision.action,
            quantity=quantity,
            entry_price=price,
            entry_time_ms=market.timestamp_ms,
            expected_exit_ms=market.timestamp_ms + max(1, decision.max_hold_seconds) * 1000,
            stop=decision.stop,
            target=decision.target,
            entry_cost=fee,
            mode="PAPER",
        )

    async def exit(
        self,
        position: OpenPosition,
        market: MarketFeatures,
        reason: str,
        state: MarketState | None = None,
    ) -> dict[str, Any]:
        raw_price = market.best_bid if position.side == "BUY" else market.best_ask
        if raw_price is None:
            return {"status": "deferred", "reason": "no opposing quote"}
        slip = self.config.paper_slippage_bps / 10_000
        exit_price = raw_price * (1 - slip if position.side == "BUY" else 1 + slip)
        gross = (exit_price - position.entry_price) * position.quantity * (1 if position.side == "BUY" else -1)
        exit_fee = exit_price * position.quantity * self.config.paper_fee_rate
        net = gross - position.entry_cost - exit_fee
        self.base_balance = max(0.0, self.base_balance - position.quantity)
        proceeds = exit_price * position.quantity
        self.quote_balance += proceeds - exit_fee
        self.total_fees += exit_fee
        return {
            "status": "closed",
            "position_id": position.position_id,
            "exit_price": exit_price,
            "gross_pnl": gross,
            "fees": position.entry_cost + exit_fee,
            "net_pnl": net,
            "reason": reason,
            "mode": "PAPER",
        }

    def portfolio_value(self, mark_price: float | None) -> float | None:
        if mark_price is None:
            return None
        return self.quote_balance + self.base_balance * mark_price

    def balances(self) -> dict[str, float]:
        return {"USDT": self.quote_balance, "BTC": self.base_balance}


class BinanceLiveExecutor:
    def __init__(self, config: EngineConfig, rest: BinanceRestClient):
        self.config = config
        self.rest = rest

    async def enter(
        self,
        decision: Decision,
        market: MarketFeatures,
        state: MarketState | None = None,
    ) -> OpenPosition | None:
        if state is None:
            raise SafetyBlocked("live entry requires the synchronized MarketState")
        return await self.enter_with_state(decision, market, state)

    async def enter_with_state(
        self,
        decision: Decision,
        market: MarketFeatures,
        state: MarketState,
    ) -> OpenPosition | None:
        if decision.position_size_quote <= 0:
            return None
        intent = OrderIntent(
            symbol=self.config.symbol,
            side=decision.action,
            order_type="MARKET",
            quote_order_qty=Decimal(str(decision.position_size_quote)),
        )
        response = await asyncio.to_thread(self.rest.place_order, intent, state=state)
        status = str(response.get("status", ""))
        if status not in {"NEW", "PARTIALLY_FILLED", "FILLED"}:
            raise BinanceError(f"unexpected entry order status: {status}", payload=response)
        terminal = await asyncio.to_thread(
            self.rest.wait_for_terminal_order,
            self.config.symbol,
            order_id=int(response["orderId"]) if response.get("orderId") is not None else None,
            client_order_id=response.get("clientOrderId"),
            timeout_s=self.config.live_order_timeout_s,
        ) if status not in {"FILLED", "CANCELED", "REJECTED", "EXPIRED", "EXPIRED_IN_MATCH"} else response
        if str(terminal.get("status", "")) not in {"FILLED", "PARTIALLY_FILLED"}:
            raise BinanceError("entry did not produce a verified fill", payload=terminal)
        executed_qty = float(terminal.get("executedQty", 0) or 0)
        if executed_qty <= 0:
            raise BinanceError("entry terminal response has zero executed quantity", payload=terminal)
        cumulative_quote = float(terminal.get("cummulativeQuoteQty", 0) or 0)
        if cumulative_quote <= 0:
            fills = terminal.get("fills") or response.get("fills") or []
            cumulative_quote = sum(float(fill.get("price", 0)) * float(fill.get("qty", 0)) for fill in fills)
        entry_price = cumulative_quote / executed_qty if cumulative_quote > 0 else float(market.best_ask or market.best_bid or 0)
        reconciliation = await asyncio.to_thread(
            self.rest.reconcile_order_fills,
            self.config.symbol,
            terminal,
        )
        if reconciliation["unresolved_commissions"]:
            raise BinanceError(
                "entry commissions could not be converted to quote currency",
                payload=reconciliation,
            )
        entry_fee = float(reconciliation["estimated_quote_fee"])
        return OpenPosition(
            position_id=str(response.get("orderId", uuid.uuid4().hex[:16])),
            side=decision.action,
            quantity=executed_qty,
            entry_price=entry_price,
            entry_time_ms=market.timestamp_ms,
            expected_exit_ms=market.timestamp_ms + max(1, decision.max_hold_seconds) * 1000,
            stop=decision.stop,
            target=decision.target,
            entry_cost=entry_fee,
            mode="LIVE",
        )

    async def exit(
        self,
        position: OpenPosition,
        market: MarketFeatures,
        reason: str,
        state: MarketState | None = None,
    ) -> dict[str, Any]:
        if state is None:
            raise SafetyBlocked("live exit requires the synchronized MarketState")
        return await self.exit_with_state(position, market, state, reason)

    async def exit_with_state(
        self,
        position: OpenPosition,
        market: MarketFeatures,
        state: MarketState,
        reason: str,
    ) -> dict[str, Any]:
        side = "SELL" if position.side == "BUY" else "BUY"
        intent = OrderIntent(self.config.symbol, side, "MARKET", quantity=Decimal(str(position.quantity)))
        response = await asyncio.to_thread(self.rest.place_order, intent, state=state)
        status = str(response.get("status", ""))
        if status not in {"NEW", "PARTIALLY_FILLED", "FILLED"}:
            raise BinanceError(f"unexpected exit order status: {status}", payload=response)
        terminal = await asyncio.to_thread(
            self.rest.wait_for_terminal_order,
            self.config.symbol,
            order_id=int(response["orderId"]) if response.get("orderId") is not None else None,
            client_order_id=response.get("clientOrderId"),
            timeout_s=self.config.live_order_timeout_s,
        ) if status not in {"FILLED", "CANCELED", "REJECTED", "EXPIRED", "EXPIRED_IN_MATCH"} else response
        if str(terminal.get("status", "")) not in {"FILLED", "PARTIALLY_FILLED"}:
            raise BinanceError("exit did not produce a verified fill", payload=terminal)
        executed_qty = float(terminal.get("executedQty", 0) or 0)
        cumulative_quote = float(terminal.get("cummulativeQuoteQty", 0) or 0)
        reconciliation = await asyncio.to_thread(
            self.rest.reconcile_order_fills,
            self.config.symbol,
            terminal,
        )
        if reconciliation["unresolved_commissions"]:
            raise BinanceError(
                "exit commissions could not be converted to quote currency",
                payload=reconciliation,
            )
        executed_qty = float(reconciliation["executed_qty"])
        average_price = float(reconciliation["average_price"]) if reconciliation["average_price"] is not None else None
        entry_value = position.entry_price * executed_qty
        exit_value = float(reconciliation["quote_qty"])
        side_sign = 1.0 if position.side == "BUY" else -1.0
        gross_pnl = (exit_value - entry_value) * side_sign
        fees = position.entry_cost + float(reconciliation["estimated_quote_fee"])
        net_pnl = gross_pnl - fees
        return {
            "status": "closed" if str(terminal.get("status")) == "FILLED" else "partially_filled",
            "position_id": position.position_id,
            "reason": reason,
            "mode": "LIVE",
            "exchange": terminal,
            "executed_qty": executed_qty,
            "average_price": average_price,
            "gross_pnl": gross_pnl,
            "fees": fees,
            "net_pnl": net_pnl,
            "pnl_status": "reconciled_from_myTrades",
            "commission_assets": {key: str(value) for key, value in reconciliation["commissions"].items()},
            "requested_qty": position.quantity,
            "residual_qty": max(0.0, position.quantity - executed_qty),
        }


class TradeJournal:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def append(self, record: Mapping[str, Any]) -> None:
        payload = {"journal_id": uuid.uuid4().hex[:16], "recorded_at": utc_now(), **dict(record)}
        with self.path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(payload, sort_keys=True, default=str) + "\n")


class LiveTradingEngine:
    def __init__(
        self,
        config: EngineConfig | None = None,
        *,
        rest: BinanceRestClient | None = None,
        stream: BinanceMarketStream | None = None,
        reasoner: Reasoner | None = None,
        executor: Executor | None = None,
    ):
        self.config = config or EngineConfig.from_env()
        self.rest = rest or BinanceRestClient(BinanceConfig.from_env())
        self.stream = stream
        self.features = FeatureEngine(self.config.symbol)
        self.detector = OpportunityDetector(self.config)
        self.risk = RiskManager(self.config)
        self.reasoner = reasoner or NoTradeReasoner()
        self.executor = executor or PaperExecutor(self.config)
        self.journal = TradeJournal(self.config.journal_path)
        self.position: OpenPosition | None = None
        self.started_at_ms: int | None = None
        self.last_decision_ms: int = 0
        self.last_trade_ms: int = 0
        self.last_opportunity: Opportunity | None = None
        self.last_decision: Decision = Decision.no_trade(self.config.symbol, "not started")
        self.last_execution: dict[str, Any] | None = None
        self.last_error: str | None = None
        self.trade_count = 0
        self.win_count = 0
        self.loss_count = 0
        self.realized_pnl_quote = 0.0
        self.running = False
        self.stop_requested = False
        self.order_in_flight = False
        self.config.validate()

    def state_snapshot(self) -> dict[str, Any]:
        mark_price = self.features.last_features.mid_price if self.features.last_features else None
        paper_value = self.executor.portfolio_value(mark_price) if isinstance(self.executor, PaperExecutor) else None
        return {
            "engine_version": ENGINE_VERSION,
            "mode": self.config.mode,
            "symbol": self.config.symbol,
            "running": self.running,
            "started_at_ms": self.started_at_ms,
            "session_minutes": self.config.session_minutes,
            "features": self.features.last_features.as_dict() if self.features.last_features else None,
            "opportunity": self.last_opportunity.as_dict() if self.last_opportunity else None,
            "decision": self.last_decision.as_dict(),
            "position": self.position.__dict__ if self.position else None,
            "last_execution": self.last_execution,
            "risk": self.risk.state.__dict__,
            "statistics": {
                "trade_count": self.trade_count,
                "wins": self.win_count,
                "losses": self.loss_count,
                "realized_pnl_quote": self.realized_pnl_quote,
                "fees_included": True,
                "paper_quote_balance": getattr(self.executor, "quote_balance", None),
                "paper_base_balance": getattr(self.executor, "base_balance", None),
                "paper_fees": getattr(self.executor, "total_fees", None),
                "paper_portfolio_value": paper_value,
            },
            "last_error": self.last_error,
            "last_trade_ms": self.last_trade_ms,
            "order_in_flight": self.order_in_flight,
            "deployment_status": "research_only" if self.config.mode != "LIVE" else "live-gated",
        }

    def persist_state(self) -> None:
        self.config.state_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.config.state_path.with_name(f".{self.config.state_path.name}.part")
        temporary.write_text(json.dumps(self.state_snapshot(), indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
        os.replace(temporary, self.config.state_path)

    async def process_event(self, event: Mapping[str, Any], state: MarketState) -> dict[str, Any]:
        if self.rest.safety.kill_switch_active():
            self.request_stop("kill switch active")
            self.persist_state()
            return self.state_snapshot()
        features = self.features.ingest_event(event, state)
        now = features.timestamp_ms
        if self.started_at_ms is None:
            self.started_at_ms = now
        self.running = True
        if self.risk.state.halted:
            self.persist_state()
            return self.state_snapshot()
        if self.position is not None and not self.order_in_flight:
            exit_reason = self._exit_reason(features, now)
            if exit_reason:
                await self._close_position(features, state, exit_reason)
        if now - self.last_decision_ms >= self.config.decision_cooldown_ms and self.position is None and not self.order_in_flight:
            self.last_decision_ms = now
            opportunity = self.detector.detect(features)
            self.last_opportunity = opportunity
            decision = await self.reasoner.decide(features, opportunity)
            self.last_decision = decision
            try:
                decision.validate_for_spot(self.config.symbol)
            except ValueError as exc:
                self.last_error = str(exc)
                self.journal.append({"type": "decision_rejected", "reason": str(exc), "decision": decision.as_dict(), "features": features.as_dict()})
                self.persist_state()
                return self.state_snapshot()
            approved, reason = self.risk.validate(
                decision,
                features,
                live=self.config.mode == "LIVE",
                has_position=self.position is not None,
            )
            self.journal.append({"type": "decision", "market": features.as_dict(), "opportunity": opportunity.as_dict() if opportunity else None, "decision": decision.as_dict(), "approved": approved, "approval_reason": reason, "mode": self.config.mode})
            if approved and decision.action == "BUY" and now - self.last_trade_ms >= self.config.minimum_trade_interval_ms:
                await self._open_position(decision, features, state)
        self.persist_state()
        return self.state_snapshot()

    def request_stop(self, reason: str = "operator stop") -> None:
        self.stop_requested = True
        self.risk.halt(reason)
        if self.stream is not None:
            self.stream.stop()
        self.running = False

    def _exit_reason(self, features: MarketFeatures, now: int) -> str | None:
        assert self.position is not None
        price = features.last_price
        if price is None:
            return None
        if now >= self.position.expected_exit_ms:
            return "time_stop"
        if self.position.stop is not None:
            if self.position.side == "BUY" and price <= self.position.stop:
                return "stop"
            if self.position.side == "SELL" and price >= self.position.stop:
                return "stop"
        if self.position.target is not None:
            if self.position.side == "BUY" and price >= self.position.target:
                return "target"
            if self.position.side == "SELL" and price <= self.position.target:
                return "target"
        if features.data_quality != "ok":
            return "data_quality"
        return None

    async def _open_position(self, decision: Decision, features: MarketFeatures, state: MarketState) -> None:
        if decision.action != "BUY":
            self.last_error = "spot engine rejects naked short decisions"
            self.journal.append({"type": "decision_rejected", "reason": "spot_no_naked_short", "decision": decision.as_dict(), "features": features.as_dict()})
            return
        self.order_in_flight = True
        try:
            position = await self.executor.enter(decision, features, state)
        except Exception as exc:
            self.last_error = f"entry failed: {type(exc).__name__}: {exc}"
            self.risk.halt("entry execution failure")
            self.journal.append({"type": "entry_error", "error": self.last_error, "decision": decision.as_dict(), "features": features.as_dict()})
            return
        finally:
            self.order_in_flight = False
        if position is None:
            self.last_error = "executor did not produce a verified position"
            return
        self.position = position
        self.last_trade_ms = features.timestamp_ms
        self.risk.state.open_exposure_quote = decision.position_size_quote
        self.trade_count += 1
        self.last_execution = {"event": "entry_verified", "position": position.__dict__, "mode": self.config.mode}
        self.journal.append({"type": "entry", "decision": decision.as_dict(), "features": features.as_dict(), "position": position.__dict__})

    async def _close_position(self, features: MarketFeatures, state: MarketState, reason: str) -> None:
        assert self.position is not None
        position = self.position
        self.order_in_flight = True
        try:
            result = await self.executor.exit(position, features, reason, state)
        except Exception as exc:
            self.last_error = f"exit failed: {type(exc).__name__}: {exc}"
            self.risk.halt("exit execution failure; manual reconciliation required")
            self.journal.append({"type": "exit_error", "error": self.last_error, "position": position.__dict__, "reason": reason})
            return
        finally:
            self.order_in_flight = False
        if result.get("status") == "deferred":
            return
        self.last_execution = {"event": "exit_verified", **result}
        residual_qty = float(result.get("residual_qty", 0.0) or 0.0)
        if residual_qty > 0:
            self.risk.halt("partial exit requires manual residual-position reconciliation")
            self.last_error = f"partial exit left residual quantity {residual_qty}"
            self.journal.append({"type": "partial_exit", "position": position.__dict__, "result": result})
            return
        if result.get("net_pnl") is None:
            # Live fills require account trade/commission reconciliation before
            # they can affect risk statistics. Halt rather than inventing PnL.
            self.risk.halt("live fill requires account reconciliation")
            self.last_error = "live fill received without reconciled commission/PnL"
            self.journal.append({"type": "reconciliation_required", "position": position.__dict__, "result": result})
            self.position = None
            self.risk.state.open_exposure_quote = 0.0
            return
        pnl = float(result.get("net_pnl", 0.0))
        self.realized_pnl_quote += pnl
        if pnl >= 0:
            self.win_count += 1
        else:
            self.loss_count += 1
        self.risk.register_result(pnl)
        self.risk.state.open_exposure_quote = 0.0
        self.journal.append({"type": "exit", "features": features.as_dict(), "position": position.__dict__, "result": result, "reason": reason})
        self.position = None

    async def run(self, *, duration_minutes: int | None = None) -> None:
        if self.rest.safety.kill_switch_active():
            raise SafetyBlocked("kill switch is active; engine start is blocked")
        if self.stream is None:
            self.stream = BinanceMarketStream(self.rest, self.config.symbol, ("1m",))
        duration = duration_minutes if duration_minutes is not None else self.config.session_minutes
        stop_at = time.monotonic() + duration * 60
        self.running = True
        self.stop_requested = False

        async def callback(message: dict[str, Any]) -> None:
            if message.get("type") == "stream_error":
                self.last_error = str(message.get("error", "market stream error"))
                self.risk.halt("market stream failure; manual review required")
                self.request_stop("market stream failure")
                self.persist_state()
                return
            if message.get("type") == "market_event":
                try:
                    await self.process_event(message["event"], self.stream.state)
                except Exception as exc:
                    self.last_error = f"{type(exc).__name__}: {exc}"
                    self.risk.halt("engine processing error")
                    self.persist_state()

        async def stop_after_duration() -> None:
            while time.monotonic() < stop_at and self.running and not self.stop_requested:
                await asyncio.sleep(1)
            self.stream.stop()

        stopper = asyncio.create_task(stop_after_duration())
        try:
            await self.stream.run(callback)
        finally:
            self.running = False
            stopper.cancel()
            await asyncio.gather(stopper, return_exceptions=True)
            self.persist_state()


async def run_engine_from_env(duration_minutes: int | None = None) -> None:
    config = EngineConfig.from_env()
    binance_config = BinanceConfig.from_env()
    rest = BinanceRestClient(binance_config)
    knowledge_context: dict[str, Any] = {}
    for path in (config.knowledge_base_path, config.assumptions_path):
        try:
            knowledge_context[str(path)] = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            knowledge_context[str(path)] = {"status": "unavailable"}
    reasoner: Reasoner = (
        OpenAICompatibleReasoner(config, knowledge_context)
        if config.llm_enabled
        else DeterministicReasoner(config) if config.mode == "PAPER" else NoTradeReasoner()
    )
    executor: Executor = PaperExecutor(config)
    if config.mode == "LIVE":
        executor = BinanceLiveExecutor(config, rest)
    engine = LiveTradingEngine(config, rest=rest, reasoner=reasoner, executor=executor)
    await engine.run(duration_minutes=duration_minutes)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run", help="run the configured PAPER or LIVE engine")
    run.add_argument("--duration-minutes", type=int)
    sub.add_parser("state", help="print persisted engine state")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    config = EngineConfig.from_env()
    if args.command == "state":
        try:
            print(config.state_path.read_text(encoding="utf-8"))
            return 0
        except FileNotFoundError:
            print(json.dumps({"status": "not_started", "mode": config.mode}))
            return 0
    try:
        asyncio.run(run_engine_from_env(args.duration_minutes))
        return 0
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        print(json.dumps({"error": type(exc).__name__, "message": str(exc)}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
