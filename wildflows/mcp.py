"""The localhost MCP boundary exposed to resident v2 frames.

The server deliberately owns only transport validation.  Frame admission, durable
call de-duplication, and tool execution belong to the typed handler supplied by
the frame engine.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
import hmac
import json
import math
import secrets
import threading
import time
from collections.abc import Iterator, Mapping
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Protocol, cast, runtime_checkable

from pydantic import ValidationError

from wildflows.frame import (
    AskRequest,
    AskResult,
    DispatchRequest,
    DispatchResult,
    GateRequest,
    GateResult,
    ToolFailure,
    ToolName,
    ToolRequest,
    ToolResponse,
)

__all__ = [
    "DEFAULT_HEARTBEAT_INTERVAL",
    "MAX_BODY_BYTES",
    "FrameCallJoin",
    "MCPServer",
    "ToolHandler",
    "ValidatedToolCall",
    "ToolProtocolError",
]


class ToolProtocolError(RuntimeError):
    """A safe, caller-caused logical tool error."""


MAX_BODY_BYTES = 1 << 20
"""Largest accepted JSON-RPC request body in bytes."""

DEFAULT_HEARTBEAT_INTERVAL = 15.0
"""Seconds between whitespace chunks for a pending ``tools/call`` response."""

_JSON_RPC_VERSION = "2.0"
_MCP_PROTOCOL_VERSION = "2024-11-05"


def _reject_non_json_constant(value: str) -> None:
    raise ValueError(f"non-JSON numeric constant: {value}")


@runtime_checkable
class ToolHandler(Protocol):
    """The engine-owned synchronous tool boundary for one MCP run."""

    def handle_tool(
        self,
        frame_id: str,
        call_index: int,
        tool: ToolName,
        request: ToolRequest,
    ) -> ToolResponse: ...


@dataclass(frozen=True)
class ValidatedToolCall:
    """An id-bearing request admitted to an independent MCP worker."""

    frame_id: str
    call_index: int
    tool: ToolName
    request: ToolRequest
    request_id: object


@dataclass
class _TrackedToolCall:
    call: ValidatedToolCall
    execute_handler: bool
    completed: threading.Event = field(default_factory=threading.Event)


@dataclass(frozen=True)
class FrameCallJoin:
    """Snapshot returned after closing one frame's MCP worker frontier."""

    completed: tuple[ValidatedToolCall, ...]
    active: tuple[ValidatedToolCall, ...]


class _MCPHTTPServer(ThreadingHTTPServer):
    """Thread-per-request so a blocking dispatch can recursively use MCP."""

    daemon_threads = True

    def __init__(self, application: "MCPServer") -> None:
        # This must remain a literal loopback IPv4 bind.  Frames receive the
        # resulting ephemeral URL as an unforgeable local capability endpoint.
        super().__init__(("127.0.0.1", 0), _MCPRequestHandler)
        self.application = application


class _MCPRequestHandler(BaseHTTPRequestHandler):
    server_version = "wildflows-mcp"
    sys_version = ""
    protocol_version = "HTTP/1.1"

    def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        try:
            if self.path != "/mcp":
                self._empty(HTTPStatus.NOT_FOUND)
                return
            application = cast(_MCPHTTPServer, self.server).application
            application._serve_post(self)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            # A peer can close while a normal response is being written. These
            # are expected transport outcomes, not server failures worth a
            # ThreadingHTTPServer traceback.
            return

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        self._method_not_allowed()

    def do_PUT(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        self._method_not_allowed()

    def do_PATCH(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        self._method_not_allowed()

    def do_DELETE(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        self._method_not_allowed()

    def do_HEAD(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        self._method_not_allowed()

    def _method_not_allowed(self) -> None:
        if self.path == "/mcp":
            self.send_response(HTTPStatus.METHOD_NOT_ALLOWED)
            self.send_header("Allow", "POST")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        self._empty(HTTPStatus.NOT_FOUND)

    def _empty(self, status: HTTPStatus) -> None:
        self.send_response(status)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, format: str, *args: object) -> None:
        # Agent stderr is part of a frame result; do not contaminate it with
        # ordinary successful MCP request logs.
        del format, args


class MCPServer:
    """A context-safe per-run localhost MCP server.

    ``start`` is idempotent and the context manager is reference counted, so an
    enclosing supervisor and a helper may safely use nested contexts.  Request
    work never holds the lifecycle lock; a dispatch handler can therefore make
    nested MCP calls without serializing or deadlocking the HTTP workers.
    """

    def __init__(
        self,
        handler: ToolHandler,
        token: str | None = None,
        *,
        heartbeat_interval: float = DEFAULT_HEARTBEAT_INTERVAL,
    ) -> None:
        if token is not None and not token:
            raise ValueError("MCP token must be non-empty")
        if (
            not isinstance(heartbeat_interval, int | float)
            or isinstance(heartbeat_interval, bool)
            or not math.isfinite(heartbeat_interval)
            or heartbeat_interval <= 0
        ):
            raise ValueError("MCP heartbeat interval must be a positive finite number")
        self._handler = handler
        self.token = token if token is not None else secrets.token_urlsafe(32)
        self._heartbeat_interval = float(heartbeat_interval)
        self._server: _MCPHTTPServer | None = None
        self._thread: threading.Thread | None = None
        self._lock = threading.RLock()
        self._context_depth = 0
        self._frame_capabilities: dict[str, str] = {}
        self._call_condition = threading.Condition(self._lock)
        self._frame_calls: dict[str, list[_TrackedToolCall]] = {}
        self._closing_frames: set[str] = set()

    @property
    def endpoint(self) -> str:
        """The running endpoint, including its fixed MCP path."""
        with self._lock:
            if self._server is None:
                raise RuntimeError("MCP server is not running")
            port = self._server.server_address[1]
            return f"http://127.0.0.1:{port}/mcp"

    @property
    def url(self) -> str:
        """Alias for :attr:`endpoint` for HTTP clients."""
        return self.endpoint

    def register_frame(self, frame_id: str) -> str:
        """Issue a per-attempt capability cryptographically bound to one frame."""
        if not frame_id:
            raise ValueError("frame id must be non-empty")
        capability = secrets.token_urlsafe(32)
        with self._call_condition:
            if any(
                not tracked.completed.is_set()
                for tracked in self._frame_calls.get(frame_id, [])
            ):
                raise RuntimeError(f"frame {frame_id!r} still has active MCP calls")
            self._frame_calls.pop(frame_id, None)
            self._closing_frames.discard(frame_id)
            self._frame_capabilities[capability] = frame_id
        return capability

    def join_frame(self, frame_id: str, timeout: float) -> FrameCallJoin:
        """Close call admission and wait at most ``timeout`` for its workers."""
        if not math.isfinite(timeout) or timeout < 0:
            raise ValueError("frame call join timeout must be finite and non-negative")
        deadline = time.monotonic() + timeout
        with self._call_condition:
            self._closing_frames.add(frame_id)
            while True:
                tracked = list(self._frame_calls.get(frame_id, []))
                active = [item for item in tracked if not item.completed.is_set()]
                if not active:
                    break
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                self._call_condition.wait(timeout=remaining)
            return FrameCallJoin(
                completed=tuple(
                    item.call for item in tracked if item.completed.is_set()
                ),
                active=tuple(
                    item.call for item in tracked if not item.completed.is_set()
                ),
            )

    def revoke_frame(self, capability: str) -> None:
        with self._call_condition:
            frame_id = self._frame_capabilities.pop(capability, None)
            if frame_id is not None:
                self._frame_calls.pop(frame_id, None)
                self._closing_frames.discard(frame_id)

    def start(self) -> "MCPServer":
        """Start serving on a fresh loopback ephemeral port, if not running."""
        with self._lock:
            if self._server is not None:
                return self
            server = _MCPHTTPServer(self)
            thread = threading.Thread(
                target=server.serve_forever,
                name="wildflows-mcp",
                daemon=True,
            )
            self._server = server
            self._thread = thread
            thread.start()
        return self

    def stop(self) -> None:
        """Stop accepting requests and release the ephemeral port."""
        with self._lock:
            server = self._server
            thread = self._thread
            if server is None:
                return
            # Keep the lifecycle lock while shutting down so a concurrent start
            # cannot race an old server that still owns the port.
            server.shutdown()
            server.server_close()
            self._server = None
            self._thread = None
            if thread is not None and thread is not threading.current_thread():
                thread.join()

    close = stop

    def __enter__(self) -> "MCPServer":
        with self._lock:
            if self._context_depth == 0:
                self.start()
            self._context_depth += 1
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object | None,
    ) -> None:
        del exc_type, exc, traceback
        with self._lock:
            if self._context_depth == 0:
                raise RuntimeError("MCP server context exited without entering")
            self._context_depth -= 1
            if self._context_depth == 0:
                self.stop()

    @contextmanager
    def running(self) -> Iterator["MCPServer"]:
        """An explicit context-manager spelling for callers that prefer it."""
        with self:
            yield self

    def _serve_post(self, request: _MCPRequestHandler) -> None:
        authorized, bound_frame = self._authorization(request)
        if not authorized:
            # Do not disclose whether authentication was omitted or merely
            # incorrect. In particular, do not parse a bad caller's body.
            request.send_response(HTTPStatus.UNAUTHORIZED)
            request.send_header("WWW-Authenticate", "Bearer")
            request.send_header("Content-Length", "0")
            request.end_headers()
            return

        body = self._read_body(request)
        if body is None:
            return
        try:
            payload = json.loads(body, parse_constant=_reject_non_json_constant)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
            self._send_json(request, self._error(None, -32700, "Parse error"))
            return

        claimed_frame = request.headers.get("X-Wildflows-Frame")
        if bound_frame is not None and claimed_frame not in (None, bound_frame):
            request.send_response(HTTPStatus.UNAUTHORIZED)
            request.send_header("WWW-Authenticate", "Bearer")
            request.send_header("Content-Length", "0")
            request.end_headers()
            return
        frame_id = bound_frame or claimed_frame
        prepared = self._streamable_tool_call(payload, frame_id)
        if prepared is not None:
            self._stream_tool_call(request, prepared)
            return
        response = self._dispatch(payload, frame_id)
        if response is None:
            request._empty(HTTPStatus.NO_CONTENT)
            return
        self._send_json(request, response)

    def _authorization(
        self, request: _MCPRequestHandler
    ) -> tuple[bool, str | None]:
        supplied = request.headers.get("Authorization")
        if supplied is None or not supplied.startswith("Bearer "):
            return False, None
        candidate = supplied.removeprefix("Bearer ")
        with self._lock:
            for capability, frame_id in self._frame_capabilities.items():
                if hmac.compare_digest(candidate, capability):
                    return True, frame_id
            if hmac.compare_digest(candidate, self.token):
                # The run token can initialize/inspect the protocol. Once frame
                # capabilities exist it cannot impersonate one for tools/call.
                frame = request.headers.get("X-Wildflows-Frame")
                return True, frame if not self._frame_capabilities else ""
        return False, None

    def _read_body(self, request: _MCPRequestHandler) -> bytes | None:
        length_header = request.headers.get("Content-Length")
        if length_header is None:
            self._send_json(request, self._error(None, -32600, "Invalid Request"))
            return None
        try:
            length = int(length_header)
        except ValueError:
            self._send_json(request, self._error(None, -32600, "Invalid Request"))
            return None
        if length < 0:
            self._send_json(request, self._error(None, -32600, "Invalid Request"))
            return None
        if length > MAX_BODY_BYTES:
            request._empty(HTTPStatus.REQUEST_ENTITY_TOO_LARGE)
            return None
        return request.rfile.read(length)

    def _dispatch(
        self, payload: object, frame_id: str | None
    ) -> dict[str, object] | None:
        if not isinstance(payload, dict):
            return self._error(None, -32600, "Invalid Request")
        if payload.get("jsonrpc") != _JSON_RPC_VERSION:
            return self._error(None, -32600, "Invalid Request")
        method = payload.get("method")
        if not isinstance(method, str):
            return self._error(None, -32600, "Invalid Request")

        has_id = "id" in payload
        request_id = payload.get("id")
        if has_id and not self._valid_id(request_id):
            return self._error(None, -32600, "Invalid Request")
        response_id = request_id if has_id else None
        params = payload.get("params", {})
        if not isinstance(params, dict):
            return self._maybe_error(
                has_id, response_id, -32602, "Invalid params"
            )

        if method == "initialize":
            return self._initialize(has_id, response_id, params)
        if method == "notifications/initialized":
            # JSON-RPC notifications intentionally have no result.  Tolerate an
            # id-bearing request for simple diagnostic clients, but never make
            # the normal notification path wait for a body.
            if not has_id:
                return None
            return self._result(response_id, {})
        if method == "tools/list":
            return self._result(response_id, {"tools": self._tools()}) if has_id else None
        if method == "tools/call":
            response = self._call_tool(params, frame_id, response_id)
            return response if has_id else None
        return self._maybe_error(has_id, response_id, -32601, "Method not found")

    @staticmethod
    def _valid_id(value: object) -> bool:
        return value is None or isinstance(value, str) or (
            isinstance(value, int) and not isinstance(value, bool)
        ) or (isinstance(value, float) and math.isfinite(value))

    def _initialize(
        self,
        has_id: bool,
        request_id: object,
        params: dict[str, object],
    ) -> dict[str, object] | None:
        protocol_version = params.get("protocolVersion", _MCP_PROTOCOL_VERSION)
        if not isinstance(protocol_version, str):
            return self._maybe_error(has_id, request_id, -32602, "Invalid params")
        # The currently supported protocol's initialization shape is stable;
        # replying with it also keeps older clients on the same tool surface.
        result: dict[str, object] = {
            "protocolVersion": _MCP_PROTOCOL_VERSION,
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "wildflows", "version": "2"},
        }
        return self._result(request_id, result) if has_id else None

    def _streamable_tool_call(
        self, payload: object, frame_id: str | None
    ) -> _TrackedToolCall | None:
        """Validate and join-track an id-bearing call before returning it."""
        if not isinstance(payload, dict):
            return None
        if payload.get("jsonrpc") != _JSON_RPC_VERSION:
            return None
        if payload.get("method") != "tools/call" or "id" not in payload:
            return None
        request_id = payload["id"]
        if not self._valid_id(request_id):
            return None
        params = payload.get("params", {})
        if not isinstance(params, dict):
            return None
        call = self._prepare_tool_call(params, frame_id, request_id)
        if call is None:
            return None
        with self._call_condition:
            execute_handler = call.frame_id not in self._closing_frames
            tracked = _TrackedToolCall(call, execute_handler)
            self._frame_calls.setdefault(call.frame_id, []).append(tracked)
            if not execute_handler:
                # A call validated after closure is a completed no-effect refusal.
                tracked.completed.set()
            return tracked

    def _call_tool(
        self,
        params: dict[str, object],
        frame_id: str | None,
        request_id: object,
    ) -> dict[str, object]:
        prepared = self._prepare_tool_call(params, frame_id, request_id)
        if prepared is None:
            return self._error(request_id, -32602, "Invalid params")
        return self._complete_tool_call(prepared)

    def _prepare_tool_call(
        self,
        params: dict[str, object],
        frame_id: str | None,
        request_id: object,
    ) -> ValidatedToolCall | None:
        if not isinstance(frame_id, str) or not frame_id:
            return None
        name = params.get("name")
        arguments = params.get("arguments")
        if not isinstance(name, str) or not isinstance(arguments, dict):
            return None
        call_index = self._call_index(params)
        if call_index is None:
            return None
        try:
            tool, validated = self._validate_tool(name, arguments)
        except (KeyError, ValidationError, ValueError):
            return None
        return ValidatedToolCall(frame_id, call_index, tool, validated, request_id)

    def _complete_tool_call(self, call: ValidatedToolCall) -> dict[str, object]:
        try:
            tool_response = self._handler.handle_tool(
                call.frame_id, call.call_index, call.tool, call.request
            )
        except ToolProtocolError as exc:
            return self._error(call.request_id, -32602, str(exc))
        except Exception:
            # Tool failures that are meaningful to a frame are values returned
            # by its handler. An exception is a transport/server fault instead.
            return self._error(call.request_id, -32603, "Internal error")
        if not isinstance(
            tool_response, (DispatchResult, GateResult, AskResult, ToolFailure)
        ):
            return self._error(call.request_id, -32603, "Internal error")

        result = {
            "content": [{"type": "text", "text": tool_response.as_text()}],
            "structuredContent": tool_response.model_dump(mode="json"),
            "isError": (
                isinstance(tool_response, ToolFailure)
                or (
                    isinstance(tool_response, DispatchResult)
                    and tool_response.outcome == "refused"
                )
            ),
        }
        return self._result(call.request_id, result)

    def _stream_tool_call(
        self, request: _MCPRequestHandler, tracked: _TrackedToolCall
    ) -> None:
        """Stream a tracked call without coupling execution to the client socket."""
        started = threading.Event()
        call = tracked.call
        response: dict[str, object] = self._error(
            call.request_id, -32603, "Internal error"
        )

        def execute() -> None:
            nonlocal response
            try:
                started.wait()
                if tracked.execute_handler:
                    response = self._complete_tool_call(call)
                else:
                    response = self._error(
                        call.request_id,
                        -32602,
                        f"frame {call.frame_id!r} is terminating",
                    )
            finally:
                with self._call_condition:
                    tracked.completed.set()
                    self._call_condition.notify_all()

        worker = threading.Thread(
            target=execute,
            name="wildflows-mcp-tool",
            daemon=True,
        )
        worker.start()
        try:
            self._start_chunked_json(request)
        finally:
            # Even a disconnect while headers are sent must not cancel a call
            # that has already passed MCP validation.
            started.set()

        try:
            while not tracked.completed.wait(self._heartbeat_interval):
                self._write_chunk(request, b" ")
            raw = json.dumps(
                response, separators=(",", ":"), ensure_ascii=True
            ).encode("utf-8")
            self._write_chunk(request, raw)
            self._finish_chunks(request)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            # The independent worker retains responsibility for completing the
            # engine call; only this client's response stream is gone.
            return

    @staticmethod
    def _call_index(params: Mapping[str, object]) -> int | None:
        meta = params.get("_meta")
        if not isinstance(meta, dict):
            return None
        wildflows = meta.get("wildflows")
        if not isinstance(wildflows, dict):
            return None
        value = wildflows.get("callIndex")
        if type(value) is not int or value < 0:
            return None
        return value

    @staticmethod
    def _validate_tool(
        name: str, arguments: dict[str, object]
    ) -> tuple[ToolName, ToolRequest]:
        if name == "dispatch":
            MCPServer._only_keys(
                arguments,
                {"tasks", "rig", "parallel", "skills", "kinds", "retry_frame"},
            )
            return "dispatch", DispatchRequest.model_validate(arguments)
        if name == "gate":
            MCPServer._only_keys(arguments, {"cmd"})
            return "gate", GateRequest.model_validate(arguments)
        if name == "ask":
            MCPServer._only_keys(arguments, {"question"})
            return "ask", AskRequest.model_validate(arguments)
        raise KeyError(name)

    @staticmethod
    def _only_keys(arguments: Mapping[str, object], allowed: set[str]) -> None:
        if set(arguments).difference(allowed):
            raise ValueError("unexpected tool argument")

    @staticmethod
    def _result(request_id: object, result: dict[str, object]) -> dict[str, object]:
        return {"jsonrpc": _JSON_RPC_VERSION, "id": request_id, "result": result}

    @staticmethod
    def _error(
        request_id: object,
        code: int,
        message: str,
    ) -> dict[str, object]:
        return {
            "jsonrpc": _JSON_RPC_VERSION,
            "id": request_id,
            "error": {"code": code, "message": message},
        }

    def _maybe_error(
        self,
        has_id: bool,
        request_id: object,
        code: int,
        message: str,
    ) -> dict[str, object] | None:
        return self._error(request_id, code, message) if has_id else None

    @staticmethod
    def _start_chunked_json(request: _MCPRequestHandler) -> None:
        request.send_response(HTTPStatus.OK)
        request.send_header("Content-Type", "application/json")
        request.send_header("Transfer-Encoding", "chunked")
        request.end_headers()
        request.wfile.flush()

    @staticmethod
    def _write_chunk(request: _MCPRequestHandler, data: bytes) -> None:
        request.wfile.write(f"{len(data):X}\r\n".encode("ascii"))
        request.wfile.write(data)
        request.wfile.write(b"\r\n")
        request.wfile.flush()

    @staticmethod
    def _finish_chunks(request: _MCPRequestHandler) -> None:
        request.wfile.write(b"0\r\n\r\n")
        request.wfile.flush()

    @staticmethod
    def _send_json(
        request: _MCPRequestHandler, payload: dict[str, object]
    ) -> None:
        raw = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
        request.send_response(HTTPStatus.OK)
        request.send_header("Content-Type", "application/json")
        request.send_header("Content-Length", str(len(raw)))
        request.end_headers()
        request.wfile.write(raw)

    @staticmethod
    def _tools() -> list[dict[str, object]]:
        return [
            {
                "name": "dispatch",
                "description": (
                    "Dispatch one or more child frame tasks; a task list runs serially "
                    "by default, with each integrated result available before the next. "
                    "Set parallel=true only to fan out independent tasks and join them. "
                    "The call blocks until child results and commits are integrated. "
                    "Make sequential dispatch calls to compose pipelines, loops, and "
                    "parallel-then-review shapes."
                ),
                "inputSchema": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "tasks": {
                            "type": "array",
                            "items": {
                                "type": "string",
                                "minLength": 1,
                                "pattern": r".*\S.*",
                            },
                            "minItems": 1,
                        },
                        "rig": {
                            "type": "string",
                            "description": (
                                "Explicit rig for every task; omit only when each kind "
                                "has a rigs.yaml default."
                            ),
                            "minLength": 1,
                            "pattern": r".*\S.*",
                        },
                        "parallel": {"type": "boolean", "default": False},
                        "skills": {
                            "type": "array",
                            "description": "One ordered skill-name list per task.",
                            "items": {
                                "type": "array",
                                "items": {
                                    "type": "string",
                                    "minLength": 1,
                                    "pattern": r".*\S.*",
                                },
                            },
                        },
                        "kinds": {
                            "type": "array",
                            "description": (
                                "Optional free-text kind per task; suggested: implement, "
                                "review, research, artifact."
                            ),
                            "items": {
                                "type": "string",
                                "minLength": 1,
                                "pattern": r".*\S.*",
                            },
                        },
                    },
                    "required": ["tasks"],
                },
            },
            {
                "name": "gate",
                "description": "Run a deterministic command in this frame's worktree.",
                "inputSchema": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "cmd": {
                            "type": "string",
                            "minLength": 1,
                            "pattern": r".*\S.*",
                        }
                    },
                    "required": ["cmd"],
                },
            },
            {
                "name": "ask",
                "description": "Park this frame for an owner answer.",
                "inputSchema": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "question": {
                            "type": "string",
                            "minLength": 1,
                            "pattern": r".*\S.*",
                        }
                    },
                    "required": ["question"],
                },
            },
        ]
