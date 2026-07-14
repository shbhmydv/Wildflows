"""Black-box coverage for the localhost MCP transport boundary."""
from __future__ import annotations

from dataclasses import dataclass
from http import HTTPStatus
import http.client
import json
import threading
from typing import cast

import pytest
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

from wildflows.frame import (
    AskRequest,
    AskResult,
    ChildResult,
    DispatchRequest,
    DispatchResult,
    GateRequest,
    GateResult,
    ToolName,
    ToolRequest,
    ToolResponse,
)
from wildflows.mcp import MAX_BODY_BYTES, MCPServer

_TOKEN = "test-mcp-token"


@dataclass(frozen=True)
class RecordedCall:
    frame_id: str
    call_index: int
    tool: ToolName
    request: ToolRequest


class FakeHandler:
    """A typed stand-in for the frame engine's MCP tool boundary."""

    def __init__(self) -> None:
        self.calls: list[RecordedCall] = []

    def handle_tool(
        self,
        frame_id: str,
        call_index: int,
        tool: ToolName,
        request: ToolRequest,
    ) -> ToolResponse:
        self.calls.append(RecordedCall(frame_id, call_index, tool, request))
        if tool == "dispatch":
            assert isinstance(request, DispatchRequest)
            if request.rig == "blocked":
                return DispatchResult(
                    outcome="refused",
                    error_code="dispatch_cap",
                    message="dispatch breadth exceeds cap",
                )
            return DispatchResult(
                outcome="ok",
                children=[ChildResult(frame_id="child-1", outcome="ok", text="done")],
            )
        if tool == "gate":
            assert isinstance(request, GateRequest)
            return GateResult(exit_code=0, stdout=request.cmd, stderr="")
        assert tool == "ask"
        assert isinstance(request, AskRequest)
        return AskResult(answer=f"answer: {request.question}")


class BlockingDispatchHandler:
    """A dispatch handler whose completion is controlled by a transport test."""

    def __init__(self) -> None:
        self.started = threading.Event()
        self.release = threading.Event()

    def handle_tool(
        self,
        frame_id: str,
        call_index: int,
        tool: ToolName,
        request: ToolRequest,
    ) -> ToolResponse:
        del frame_id, call_index
        assert tool == "dispatch"
        assert isinstance(request, DispatchRequest)
        self.started.set()
        if not self.release.wait(timeout=5):
            raise RuntimeError("test handler was not released")
        return DispatchResult(
            outcome="ok",
            children=[ChildResult(frame_id="slow-child", outcome="ok", text="done")],
        )


class MemoizingBlockingGateHandler:
    """Minimal engine-like exact-call memoization for disconnect coverage."""

    def __init__(self) -> None:
        self.started = threading.Event()
        self.refetch_waiting = threading.Event()
        self.release = threading.Event()
        self.completed = threading.Event()
        self.executions = 0
        self._condition = threading.Condition()
        self._inflight: set[tuple[str, int, ToolName, str]] = set()
        self._responses: dict[tuple[str, int, ToolName, str], GateResult] = {}

    def handle_tool(
        self,
        frame_id: str,
        call_index: int,
        tool: ToolName,
        request: ToolRequest,
    ) -> ToolResponse:
        assert tool == "gate"
        assert isinstance(request, GateRequest)
        identity = (frame_id, call_index, tool, request.model_dump_json())
        with self._condition:
            cached = self._responses.get(identity)
            if cached is not None:
                return cached
            if identity in self._inflight:
                self.refetch_waiting.set()
                while identity in self._inflight:
                    self._condition.wait()
                return self._responses[identity]
            self._inflight.add(identity)
            self.executions += 1

        self.started.set()
        if not self.release.wait(timeout=5):
            raise RuntimeError("test handler was not released")
        response = GateResult(exit_code=0, stdout=request.cmd, stderr="")
        with self._condition:
            self._responses[identity] = response
            self._inflight.remove(identity)
            self._condition.notify_all()
        self.completed.set()
        return response


def json_object(value: object) -> dict[str, object]:
    assert isinstance(value, dict)
    return cast(dict[str, object], value)


def json_body(raw: bytes) -> object:
    return cast(object, json.loads(raw.decode("utf-8")))


def post_json(
    server: MCPServer,
    payload: object,
    *,
    token: str | None = _TOKEN,
    frame_id: str | None = None,
) -> tuple[int, object | None]:
    headers = {"Content-Type": "application/json"}
    if token is not None:
        headers["Authorization"] = f"Bearer {token}"
    if frame_id is not None:
        headers["X-Wildflows-Frame"] = frame_id
    request = Request(
        server.endpoint,
        data=json.dumps(payload).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    try:
        with urlopen(request, timeout=5) as response:  # noqa: S310 - local test server
            return response.status, json_body(response.read())
    except HTTPError as error:
        raw = error.read()
        return error.code, json_body(raw) if raw else None


def raw_post(
    server: MCPServer,
    body: bytes,
    *,
    content_length: int | None = None,
) -> tuple[int, bytes]:
    endpoint = urlsplit(server.endpoint)
    assert endpoint.hostname == "127.0.0.1"
    assert endpoint.port is not None
    connection = http.client.HTTPConnection(endpoint.hostname, endpoint.port, timeout=5)
    try:
        connection.request(
            "POST",
            endpoint.path,
            body=body,
            headers={
                "Authorization": f"Bearer {_TOKEN}",
                "Content-Type": "application/json",
                "Content-Length": str(len(body) if content_length is None else content_length),
            },
        )
        response = connection.getresponse()
        return response.status, response.read()
    finally:
        connection.close()


def rpc_request(method: str, request_id: int, params: object = None) -> dict[str, object]:
    request: dict[str, object] = {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": method,
    }
    if params is not None:
        request["params"] = params
    return request


def rpc_result(body: object) -> dict[str, object]:
    response = json_object(body)
    assert response["jsonrpc"] == "2.0"
    assert "error" not in response
    return json_object(response["result"])


def rpc_error_code(body: object) -> int:
    response = json_object(body)
    error = json_object(response["error"])
    code = error["code"]
    assert type(code) is int
    return code


def gate_call_payload(call_index: int = 0) -> dict[str, object]:
    return rpc_request(
        "tools/call",
        call_index,
        {
            "name": "gate",
            "arguments": {"cmd": "sleep"},
            "_meta": {"wildflows": {"callIndex": call_index}},
        },
    )


def dispatch_call_payload(call_index: int = 0) -> dict[str, object]:
    return rpc_request(
        "tools/call",
        call_index,
        {
            "name": "dispatch",
            "arguments": {
                "tasks": ["slow child"],
                "rig": "echo",
                "parallel": False,
                "skills": [[]],
            },
            "_meta": {"wildflows": {"callIndex": call_index}},
        },
    )


def test_mcp_heartbeat_interval_must_be_positive_and_finite() -> None:
    handler = FakeHandler()
    for interval in (0.0, -1.0, float("inf"), float("nan")):
        with pytest.raises(ValueError, match="heartbeat interval"):
            MCPServer(handler, token=_TOKEN, heartbeat_interval=interval)


def test_mcp_slow_dispatch_streams_heartbeats_before_its_final_json_rpc_payload() -> None:
    handler = BlockingDispatchHandler()
    with MCPServer(handler, token=_TOKEN, heartbeat_interval=0.01) as server:
        endpoint = urlsplit(server.endpoint)
        assert endpoint.hostname == "127.0.0.1"
        assert endpoint.port is not None
        connection = http.client.HTTPConnection(
            endpoint.hostname, endpoint.port, timeout=5
        )
        try:
            connection.request(
                "POST",
                endpoint.path,
                body=json.dumps(dispatch_call_payload(12)).encode("utf-8"),
                headers={
                    "Authorization": f"Bearer {_TOKEN}",
                    "Content-Type": "application/json",
                    "X-Wildflows-Frame": "frame-12",
                },
            )
            response = connection.getresponse()
            assert response.status == HTTPStatus.OK
            assert response.getheader("Transfer-Encoding") == "chunked"
            assert handler.started.wait(timeout=2)

            # HTTPResponse de-chunks the wire body. Each byte is still an
            # independent whitespace chunk, so the body cannot idle while the
            # controlled synchronous handler remains blocked.
            first_heartbeat = response.read(1)
            second_heartbeat = response.read(1)
            assert first_heartbeat == b" "
            assert second_heartbeat == b" "

            handler.release.set()
            raw = first_heartbeat + second_heartbeat + response.read()
        finally:
            handler.release.set()
            connection.close()

    body = json_object(json_body(raw))
    assert body["id"] == 12
    assert raw.count(b'{"jsonrpc"') == 1


def test_mcp_disconnected_tool_call_continues_for_an_exact_memoized_refetch() -> None:
    handler = MemoizingBlockingGateHandler()
    payload = gate_call_payload(4)
    with MCPServer(handler, token=_TOKEN, heartbeat_interval=0.01) as server:
        endpoint = urlsplit(server.endpoint)
        assert endpoint.hostname == "127.0.0.1"
        assert endpoint.port is not None
        connection = http.client.HTTPConnection(
            endpoint.hostname, endpoint.port, timeout=5
        )
        try:
            connection.request(
                "POST",
                endpoint.path,
                body=json.dumps(payload).encode("utf-8"),
                headers={
                    "Authorization": f"Bearer {_TOKEN}",
                    "Content-Type": "application/json",
                    "X-Wildflows-Frame": "frame-4",
                },
            )
            response = connection.getresponse()
            assert response.status == HTTPStatus.OK
            assert handler.started.wait(timeout=2)
            assert response.read(1) == b" "
            connection.close()

            refetched: list[tuple[int, object | None]] = []

            def refetch() -> None:
                refetched.append(post_json(server, payload, frame_id="frame-4"))

            retry = threading.Thread(target=refetch)
            retry.start()
            assert handler.refetch_waiting.wait(timeout=2)
            handler.release.set()
            retry.join(timeout=5)
            assert not retry.is_alive()
        finally:
            handler.release.set()
            connection.close()

    assert handler.completed.is_set()
    assert handler.executions == 1
    assert len(refetched) == 1
    status, body = refetched[0]
    assert status == HTTPStatus.OK
    assert rpc_result(body)["structuredContent"] == {
        "exit_code": 0,
        "stdout": "sleep",
        "stderr": "",
    }


def test_mcp_requires_token_and_exposes_fixed_loopback_tool_surface() -> None:
    handler = FakeHandler()
    with MCPServer(handler, token=_TOKEN) as server:
        endpoint = urlsplit(server.endpoint)
        assert server.endpoint == server.url
        assert endpoint.scheme == "http"
        assert endpoint.hostname == "127.0.0.1"
        assert endpoint.path == "/mcp"
        assert endpoint.port is not None

        initialize = rpc_request("initialize", 1, {"protocolVersion": "2024-11-05"})
        assert post_json(server, initialize, token=None) == (HTTPStatus.UNAUTHORIZED, None)
        assert post_json(server, initialize, token="wrong-token") == (
            HTTPStatus.UNAUTHORIZED,
            None,
        )

        status, body = post_json(server, initialize)
        assert status == HTTPStatus.OK
        initialized = rpc_result(body)
        assert initialized["protocolVersion"] == "2024-11-05"
        assert initialized["capabilities"] == {"tools": {}}
        assert initialized["serverInfo"] == {"name": "wildflows", "version": "2"}

        status, body = post_json(server, rpc_request("tools/list", 2))
        assert status == HTTPStatus.OK
        tools = rpc_result(body)["tools"]
        assert isinstance(tools, list)
        assert [json_object(tool)["name"] for tool in tools] == [
            "dispatch",
            "gate",
            "ask",
        ]
        dispatch_schema = json_object(json_object(tools[0])["inputSchema"])
        properties = json_object(dispatch_schema["properties"])
        skills_schema = json_object(properties["skills"])
        assert skills_schema["description"] == "One ordered skill-name list per task."
        assert json_object(json_object(skills_schema["items"])["items"])["type"] == "string"


def test_frame_capability_cannot_spoof_another_active_frame() -> None:
    handler = FakeHandler()
    with MCPServer(handler, token=_TOKEN) as server:
        capability = server.register_frame("child")
        payload = rpc_request(
            "tools/call",
            0,
            {
                "name": "gate",
                "arguments": {"cmd": "true"},
                "_meta": {"wildflows": {"callIndex": 0}},
            },
        )
        assert post_json(
            server, payload, token=capability, frame_id="parent"
        ) == (HTTPStatus.UNAUTHORIZED, None)
        assert handler.calls == []
        status, body = post_json(
            server, payload, token=capability, frame_id="child"
        )
        assert status == HTTPStatus.OK
        assert rpc_result(body)["isError"] is False
        assert handler.calls[-1].frame_id == "child"


def test_mcp_tool_calls_use_hidden_index_and_return_typed_results() -> None:
    handler = FakeHandler()
    with MCPServer(handler, token=_TOKEN) as server:
        missing_index = rpc_request(
            "tools/call", 1, {"name": "gate", "arguments": {"cmd": "pwd"}}
        )
        status, body = post_json(server, missing_index, frame_id="frame-7")
        assert status == HTTPStatus.OK
        assert rpc_error_code(body) == -32602
        assert handler.calls == []

        boolean_index = rpc_request(
            "tools/call",
            2,
            {
                "name": "gate",
                "arguments": {"cmd": "pwd"},
                "_meta": {"wildflows": {"callIndex": True}},
            },
        )
        status, body = post_json(server, boolean_index, frame_id="frame-7")
        assert status == HTTPStatus.OK
        assert rpc_error_code(body) == -32602
        assert handler.calls == []

        calls: list[
            tuple[ToolName, dict[str, object], ToolRequest, int, ToolResponse]
        ] = [
            (
                "dispatch",
                {
                    "tasks": ["child task"],
                    "rig": "echo",
                    "parallel": True,
                    "skills": [["long", "repo-conventions"]],
                },
                DispatchRequest(
                    tasks=["child task"],
                    rig="echo",
                    parallel=True,
                    skills=[["long", "repo-conventions"]],
                ),
                2,
                DispatchResult(
                    outcome="ok",
                    children=[
                        ChildResult(frame_id="child-1", outcome="ok", text="done")
                    ],
                ),
            ),
            (
                "gate",
                {"cmd": "pwd"},
                GateRequest(cmd="pwd"),
                3,
                GateResult(exit_code=0, stdout="pwd", stderr=""),
            ),
            (
                "ask",
                {"question": "continue?"},
                AskRequest(question="continue?"),
                4,
                AskResult(answer="answer: continue?"),
            ),
        ]
        for name, arguments, expected_request, call_index, expected_response in calls:
            payload = rpc_request(
                "tools/call",
                call_index,
                {
                    "name": name,
                    "arguments": arguments,
                    "_meta": {"wildflows": {"callIndex": call_index}},
                },
            )
            status, body = post_json(server, payload, frame_id="frame-7")
            assert status == HTTPStatus.OK
            result = rpc_result(body)
            assert result["isError"] is False
            content = result["content"]
            assert isinstance(content, list)
            assert content == [
                {"type": "text", "text": expected_response.as_text()}
            ]
            assert result["structuredContent"] == expected_response.model_dump(mode="json")
            assert handler.calls[-1] == RecordedCall(
                "frame-7", call_index, name, expected_request
            )

        status, body = post_json(
            server,
            rpc_request(
                "tools/call",
                5,
                {
                    "name": "dispatch",
                    "arguments": {"tasks": ["too many"], "rig": "blocked"},
                    "_meta": {"wildflows": {"callIndex": 5}},
                },
            ),
            frame_id="frame-7",
        )
        assert status == HTTPStatus.OK
        dispatch = rpc_result(body)
        assert dispatch["isError"] is True
        assert dispatch["structuredContent"] == {
            "outcome": "refused",
            "children": [],
            "error_code": "dispatch_cap",
            "message": "dispatch breadth exceeds cap",
        }
        content = dispatch["content"]
        assert isinstance(content, list)
        assert json_object(content[0])["text"] == (
            "dispatch refused [dispatch_cap]: dispatch breadth exceeds cap"
        )


def test_mcp_rejects_malformed_oversized_and_unknown_requests() -> None:
    handler = FakeHandler()
    with MCPServer(handler, token=_TOKEN) as server:
        status, raw = raw_post(server, b"{not json")
        assert status == HTTPStatus.OK
        assert rpc_error_code(json_body(raw)) == -32700

        nonstandard_number = (
            b'{"jsonrpc":"2.0","id":1,"method":"initialize",'
            b'"params":{"noise":NaN}}'
        )
        status, raw = raw_post(server, nonstandard_number)
        assert status == HTTPStatus.OK
        assert rpc_error_code(json_body(raw)) == -32700

        surrogate_id = b'{"jsonrpc":"2.0","id":"\\ud800","method":"tools/list"}'
        status, raw = raw_post(server, surrogate_id)
        assert status == HTTPStatus.OK
        assert json_object(json_body(raw))["id"] == "\ud800"

        status, raw = raw_post(server, b"", content_length=MAX_BODY_BYTES + 1)
        assert status == HTTPStatus.REQUEST_ENTITY_TOO_LARGE
        assert raw == b""

        status, body = post_json(server, rpc_request("not/a/mcp/method", 1))
        assert status == HTTPStatus.OK
        assert rpc_error_code(body) == -32601

        status, body = post_json(
            server,
            rpc_request(
                "tools/call",
                2,
                {
                    "name": "dispatch",
                    "arguments": {"tasks": ["x"], "rig": "echo", "parallel": 1},
                    "_meta": {"wildflows": {"callIndex": 0}},
                },
            ),
            frame_id="frame",
        )
        assert status == HTTPStatus.OK
        assert rpc_error_code(body) == -32602
