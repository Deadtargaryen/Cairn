from __future__ import annotations

import hashlib
import hmac
import json
import tempfile
import unittest
import urllib.error
from decimal import Decimal
from pathlib import Path

import binance_live as live


class FakeResponse:
    def __init__(self, payload: object, status: int = 200):
        self.payload = payload
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self):
        return json.dumps(self.payload).encode("utf-8")


class FakeOpener:
    def __init__(self, payload: object):
        self.payload = payload
        self.requests = []

    def __call__(self, request, timeout):
        self.requests.append(request)
        return FakeResponse(self.payload)


def filters_payload(**overrides):
    payload = {
        "symbol": "BTCUSDT",
        "baseAsset": "BTC",
        "quoteAsset": "USDT",
        "status": "TRADING",
        "orderTypes": ["LIMIT", "MARKET", "LIMIT_MAKER"],
        "filters": [
            {"filterType": "PRICE_FILTER", "minPrice": "0.01", "maxPrice": "1000000", "tickSize": "0.01"},
            {"filterType": "LOT_SIZE", "minQty": "0.00001", "maxQty": "100", "stepSize": "0.00001"},
            {"filterType": "MARKET_LOT_SIZE", "minQty": "0.00001", "maxQty": "100", "stepSize": "0.00001"},
            {"filterType": "NOTIONAL", "minNotional": "10", "maxNotional": "1000000", "applyMinToMarket": True, "applyMaxToMarket": True},
        ],
    }
    payload.update(overrides)
    return payload


class BinanceLiveTests(unittest.TestCase):
    def test_signed_query_matches_hmac_sha256(self):
        config = live.BinanceConfig(api_key="key", api_secret="secret")
        client = live.BinanceRestClient(config)
        client._server_offset_ms = 0
        signed = client._signed_params({"symbol": "BTCUSDT", "recvWindow": 5000})
        unsigned = {key: value for key, value in signed.items() if key != "signature"}
        expected = hmac.new(
            b"secret",
            __import__("urllib.parse", fromlist=["urlencode"]).urlencode(unsigned).encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        self.assertEqual(signed["signature"], expected)

    def test_exchange_filters_validate_and_normalize_orders(self):
        parsed = live.SymbolFilters.from_exchange_info(filters_payload())
        self.assertEqual(parsed.normalize_price(Decimal("50000.019")), Decimal("50000.01"))
        self.assertEqual(parsed.normalize_quantity(Decimal("0.001239")), Decimal("0.00123"))
        parsed.validate(side="BUY", order_type="LIMIT", quantity=Decimal("0.001"), price=Decimal("50000.00"))
        with self.assertRaises(live.SafetyBlocked):
            parsed.validate(side="BUY", order_type="LIMIT", quantity=Decimal("0.00001"), price=Decimal("50000.00"))

    def test_order_book_applies_sequence_and_detects_gap(self):
        book = live.LocalOrderBook("BTCUSDT")
        book.load_snapshot({"lastUpdateId": 10, "bids": [["100", "1"]], "asks": [["101", "2"]]})
        book.bootstrap_events([{"U": 11, "u": 11, "E": 1000, "b": [["100", "2"]], "a": []}])
        self.assertEqual(book.last_update_id, 11)
        self.assertEqual(book.best_bid, (Decimal("100"), Decimal("2")))
        with self.assertRaises(live.BookResyncRequired):
            book.apply_event({"U": 13, "u": 13, "E": 1001, "b": [], "a": []})
        self.assertTrue(book.stale)

    def test_stream_payload_normalizes_book_ticker(self):
        payload = live.normalize_stream_payload({"stream": "btcusdt@bookTicker", "data": {"u": 4, "b": "100", "a": "101"}})
        self.assertEqual(payload["e"], "bookTicker")
        self.assertEqual(payload["a"], "101")

    def test_default_order_path_is_blocked(self):
        config = live.BinanceConfig(api_key="key", api_secret="secret")
        safety = live.LiveSafety(config)
        with self.assertRaises(live.SafetyBlocked):
            safety.assert_order_allowed()

    def test_stale_market_state_blocks_live_order(self):
        config = live.BinanceConfig(
            api_key="key",
            api_secret="secret",
            order_mode="live",
            enable_live_orders=True,
            live_confirmation=live.LIVE_CONFIRMATION,
        )
        with tempfile.TemporaryDirectory() as directory:
            config = live.BinanceConfig(
                **{**config.__dict__, "live_gate_path": Path(directory) / "gate.json", "kill_switch_path": Path(directory) / "kill"}
            )
            config.live_gate_path.write_text(json.dumps({"decision": "ALLOW_LIVE_TRADING"}), encoding="utf-8")
            state = live.MarketState("BTCUSDT", live.LocalOrderBook("BTCUSDT"))
            state.last_event_ms = live._now_ms()
            state.stale = False
            with self.assertRaises(live.SafetyBlocked):
                live.LiveSafety(config).assert_market_fresh(state)

    def test_unknown_write_status_is_not_retried(self):
        config = live.BinanceConfig(api_key="key", api_secret="secret")

        def opener(request, timeout):
            raise urllib.error.HTTPError(request.full_url, 503, "busy", {}, __import__("io").BytesIO(b'{"code":-1008,"msg":"busy"}'))

        client = live.BinanceRestClient(config, opener=opener)
        with self.assertRaises(live.ExecutionStatusUnknown):
            client.request("POST", "/api/v3/order", {"symbol": "BTCUSDT"}, signed=True, write=True)

    def test_user_subscription_signature_uses_api_secret_without_logging_it(self):
        config = live.BinanceConfig(api_key="public-key", api_secret="private-secret")
        request = live.BinanceUserDataStream(config)._subscription_request()
        encoded = json.dumps(request)
        self.assertIn("public-key", encoded)
        self.assertNotIn("private-secret", encoded)
        self.assertEqual(request["method"], "userDataStream.subscribe.signature")
        self.assertRegex(request["params"]["signature"], r"^[0-9a-f]{64}$")

    def test_snapshot_is_ready_until_a_sequence_gap_occurs(self):
        book = live.LocalOrderBook("BTCUSDT")
        book.load_snapshot({"lastUpdateId": 10, "bids": [["100", "1"]], "asks": [["101", "2"]]})
        self.assertTrue(book.stale)
        self.assertFalse(book.stream_synced)
        book.apply_event({"U": 11, "u": 11, "E": 1000, "b": [], "a": []})
        self.assertFalse(book.stale)
        self.assertTrue(book.stream_synced)
        with self.assertRaises(live.BookResyncRequired):
            book.apply_event({"U": 13, "u": 13, "E": 1001, "b": [], "a": []})
        self.assertTrue(book.stale)


if __name__ == "__main__":
    unittest.main()
