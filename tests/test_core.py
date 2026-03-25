"""Unit tests for target-api."""

from __future__ import annotations

import io
import json
import pytest
from singer_sdk.testing import target_sync_test

import backoff._sync as backoff_sync
import requests

from target_api.client import ApiSink
from target_api.sinks import BatchSink, RecordSink
from target_api.target import TargetApi


def _singer_input(
    records: list[dict],
    stream: str = "users",
    key_properties: list[str] | None = None,
) -> io.StringIO:
    schema = {
        "type": "object",
        "properties": {"id": {"type": "integer"}, "name": {"type": "string"}},
    }
    messages = [
        {
            "type": "SCHEMA",
            "stream": stream,
            "schema": schema,
            "key_properties": key_properties or ["id"],
        },
    ]
    for record in records:
        messages.append({"type": "RECORD", "stream": stream, "record": record})
    return io.StringIO("\n".join(json.dumps(m) for m in messages) + "\n")


def test_target_record_flow(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: list[dict] = []

    def _fake_request_api(self, http_method, endpoint=None, params=None, request_data=None, headers=None, verify=True):
        captured.append(
            {
                "method": http_method,
                "endpoint": endpoint,
                "params": params,
                "request_data": request_data,
                "headers": headers,
                "verify": verify,
            }
        )

        class _Resp:
            ok = True

            def json(self):
                return {"id": "rec-1"}

        return _Resp()

    monkeypatch.setattr(RecordSink, "request_api", _fake_request_api, raising=True)

    target = TargetApi(config={"url": "https://example.com/{stream}"})
    input_buf = _singer_input([{"id": 1, "name": "Ada"}])

    target_sync_test(target, input_buf, finalize=True)

    assert captured, "record request should be made"
    assert captured[0]["method"] == "POST"
    assert captured[0]["request_data"] == {"id": 1, "name": "Ada"}
    assert isinstance(target._sinks_active["users"], RecordSink)


def test_target_batch_flow(monkeypatch: pytest.MonkeyPatch) -> None:
    captured_batches: list[list[dict]] = []
    captured_methods: list[str] = []

    def _fake_request_api(self, http_method, endpoint=None, params=None, request_data=None, headers=None, verify=True):
        captured_methods.append(http_method)
        captured_batches.append(request_data)

        class _Resp:
            ok = True

            def json(self):
                return {"id": "batch-1"}

        return _Resp()

    monkeypatch.setattr(ApiSink, "request_api", _fake_request_api, raising=True)

    target = TargetApi(
        config={
            "url": "https://example.com/{stream}",
            "process_as_batch": True,
            "batch_size": 2,
            "inject_batch_ids": True,
        }
    )
    input_buf = _singer_input(
        [{"id": 1, "name": "Ada"}, {"id": 2, "name": "Lin"}, {"id": 3, "name": "Ken"}]
    )

    target_sync_test(target, input_buf, finalize=True)

    assert len(captured_batches) == 2
    assert captured_methods == ["POST", "POST"]
    assert len(captured_batches[0]) == 2
    assert len(captured_batches[1]) == 1
    for batch in captured_batches:
        batch_ids = {rec.get("hgBatchId") for rec in batch}
        assert len(batch_ids) == 1
        assert None not in batch_ids
    assert isinstance(target._sinks_active["users"], BatchSink)


def _make_response(status_code: int) -> requests.Response:
    response = requests.Response()
    response.status_code = status_code
    response.reason = "OK" if status_code < 400 else "Server Error"
    request = requests.Request(
        method="POST",
        url="https://example.com/users",
        headers={"x-api-key": "secret"},
        data='{"id":1}',
    )
    response.request = request.prepare()
    response._content = b"error"
    return response


def test_request_retries_on_retriable_error(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = 0

    def _fake_request(*, method, url, params=None, headers=None, data=None, verify=True, timeout=None):
        nonlocal calls
        calls += 1
        if calls < 3:
            return _make_response(500)
        return _make_response(200)

    monkeypatch.setattr(requests, "request", _fake_request, raising=True)
    monkeypatch.setattr(backoff_sync.time, "sleep", lambda *_args, **_kwargs: None, raising=True)

    target = TargetApi(config={"url": "https://example.com/{stream}"})
    schema = {"type": "object", "properties": {"id": {"type": "integer"}}}
    sink = RecordSink(target, "users", schema, ["id"])

    response = sink._request("POST", "", request_data={"id": 1}, headers={}, params={}, verify=True)

    assert calls == 3
    assert response.status_code == 200


def test_request_retries_on_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = 0

    def _fake_request(*, method, url, params=None, headers=None, data=None, verify=True, timeout=None):
        nonlocal calls
        calls += 1
        if calls < 3:
            raise requests.exceptions.Timeout("timeout")
        return _make_response(200)

    monkeypatch.setattr(requests, "request", _fake_request, raising=True)
    monkeypatch.setattr(backoff_sync.time, "sleep", lambda *_args, **_kwargs: None, raising=True)

    target = TargetApi(config={"url": "https://example.com/{stream}"})
    schema = {"type": "object", "properties": {"id": {"type": "integer"}}}
    sink = RecordSink(target, "users", schema, ["id"])

    response = sink._request("POST", "", request_data={"id": 1}, headers={}, params={}, verify=True)

    assert calls == 3
    assert response.status_code == 200


def test_base_url_uses_env_and_api_key_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TENANT", "t1")
    monkeypatch.setenv("FLOW", "f1")
    monkeypatch.setenv("TAP", "tap1")
    monkeypatch.setenv("CONNECTOR_ID", "c1")

    target = TargetApi(
        config={
            "url": "https://example.com/{tenant}/{flow}/{tap}/{connector_id}/{stream}",
            "api_key_url": True,
            "api_key_header": "x-api-key",
            "api_key": "secret",
        }
    )
    schema = {"type": "object", "properties": {"id": {"type": "integer"}}}
    sink = RecordSink(target, "users", schema, ["id"])

    assert sink.base_url == "https://example.com/t1/f1/tap1/c1/users?x-api-key=secret"


def test_default_and_custom_headers_applied(monkeypatch: pytest.MonkeyPatch) -> None:
    captured = {}

    def _fake_request(*, method, url, params=None, headers=None, data=None, verify=True, timeout=None):
        captured["headers"] = headers
        captured["timeout"] = timeout
        return _make_response(200)

    monkeypatch.setattr(requests, "request", _fake_request, raising=True)
    monkeypatch.setattr(backoff_sync.time, "sleep", lambda *_args, **_kwargs: None, raising=True)

    target = TargetApi(
        config={
            "url": "https://example.com/{stream}",
            "auth": True,
            "api_key_header": "x-api-key",
            "api_key": "secret",
            "user_agent": "ua-test",
            "custom_headers": [{"name": "X-Custom", "value": "1"}, {"name": 1, "value": "no"}],
            "timeout": 123,
        }
    )
    schema = {"type": "object", "properties": {"id": {"type": "integer"}}}
    sink = RecordSink(target, "users", schema, ["id"])

    sink._request("POST", "", request_data={"id": 1}, headers=sink.custom_headers, params={}, verify=True)

    headers = captured["headers"]
    assert headers["x-api-key"] == "secret"
    assert headers["User-Agent"] == "ua-test"
    assert headers["X-Custom"] == "1"
    assert headers["Content-Type"] == "application/json"
    assert captured["timeout"] == 123


def test_is_full_with_max_size_in_bytes() -> None:
    target = TargetApi(config={"url": "https://example.com/{stream}", "max_size_in_bytes": 1})
    schema = {"type": "object", "properties": {"id": {"type": "integer"}}}
    sink = RecordSink(target, "users", schema, ["id"])

    sink._pending_batch = {"records": [{"id": 1, "payload": "x" * 1000}]}

    assert sink.is_full is True


def test_metadata_and_stream_key_injection() -> None:
    target = TargetApi(
        config={
            "url": "https://example.com/{stream}",
            "add_stream_key": True,
            "metadata": '{"a": 1}',
        }
    )
    schema = {"type": "object", "properties": {"id": {"type": "integer"}, "metadata": {"type": "object"}}}

    record_sink = RecordSink(target, "users", schema, ["id"])
    record = record_sink.preprocess_record({"id": 1}, {})
    assert record["stream"] == "users"
    assert record["metadata"]["a"] == 1

    batch_sink = BatchSink(target, "users", schema, ["id"])
    batch_record = batch_sink.process_batch_record({"id": 1}, 0)
    assert batch_record["stream"] == "users"
    assert batch_record["metadata"]["a"] == 1


def test_enforce_order_parallelism() -> None:
    target = TargetApi(config={"url": "https://example.com/{stream}", "enforce_order": True})
    assert target.MAX_PARALLELISM == 1
    target = TargetApi(config={"url": "https://example.com/{stream}", "enforce_order": False})
    assert target.MAX_PARALLELISM == 10
