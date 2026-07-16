"""Generate the small per-frame Pi extension used by the MCP reference rig."""
from __future__ import annotations

import json
import os
from pathlib import Path
from collections.abc import Sequence

__all__ = ["create_pi_extension", "write_pi_shim"]


def write_pi_shim(
    runtime_dir: Path,
    endpoint: str,
    token: str,
    frame_id: str,
    next_call_index: int,
    replay_calls: Sequence[tuple[int, str, dict[str, object]]] | None = None,
) -> Path:
    """Write a private Pi extension into a frame-owned runtime directory.

    ``runtime_dir`` is supplied by the frame engine rather than a worktree, so
    the extension never becomes an agent-visible repository change.  The three
    capability values are JSON-encoded TypeScript literals, not environment
    lookups: a launched frame receives only its own immutable connection data.
    """
    if type(next_call_index) is not int or next_call_index < 0:
        raise ValueError("next_call_index must be a nonnegative integer")
    if not endpoint:
        raise ValueError("MCP endpoint must be non-empty")
    if not token:
        raise ValueError("MCP token must be non-empty")
    if not frame_id:
        raise ValueError("frame_id must be non-empty")

    directory = Path(runtime_dir)
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    path = directory / "wildflows-pi-extension.ts"
    content = _extension_source(
        endpoint, token, frame_id, next_call_index, replay_calls or []
    )

    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(content)
    except BaseException:
        # fdopen owns the descriptor after it succeeds.  Before that, close the
        # descriptor ourselves so a failed extension write does not leak it.
        try:
            os.close(descriptor)
        except OSError:
            pass
        raise
    return path


def create_pi_extension(
    runtime_dir: Path,
    endpoint: str,
    token: str,
    frame_id: str,
    next_call_index: int,
    replay_calls: Sequence[tuple[int, str, dict[str, object]]] | None = None,
) -> Path:
    """Compatibility spelling for callers creating a Pi extension."""
    return write_pi_shim(
        runtime_dir, endpoint, token, frame_id, next_call_index, replay_calls
    )


def _literal(value: str) -> str:
    """Return a JavaScript string literal without interpolation opportunities."""
    return json.dumps(value, ensure_ascii=False)


def _extension_source(
    endpoint: str,
    token: str,
    frame_id: str,
    next_call_index: int,
    replay_calls: Sequence[tuple[int, str, dict[str, object]]],
) -> str:
    replay = [
        {"callIndex": index, "name": name, "arguments": arguments}
        for index, name, arguments in replay_calls
    ]
    return f'''import type {{ ExtensionAPI }} from "@earendil-works/pi-coding-agent";
import {{ Type }} from "typebox";

const endpoint = {_literal(endpoint)};
const token = {_literal(token)};
const frameId = {_literal(frame_id)};
let nextCallIndex = {next_call_index};
const replayCalls: Array<{{callIndex: number; name: string; arguments: Record<string, unknown>}}> = {json.dumps(replay, ensure_ascii=False)};
const claimedReplayCalls = new Set<number>();
let sealedFrontierError: Error | undefined;

type TextContent = {{ type: "text"; text: string }};
type ToolResult = {{
  content: TextContent[];
  structuredContent: unknown;
  isError: boolean;
}};

function isRecord(value: unknown): value is Record<string, unknown> {{
  return typeof value === "object" && value !== null && !Array.isArray(value);
}}

class EngineCallError extends Error {{}}

function engineError(value: unknown): EngineCallError | undefined {{
  if (!isRecord(value) || typeof value.code !== "number" ||
      typeof value.message !== "string") {{
    return undefined;
  }}
  return new EngineCallError(
    `wildflows MCP error ${{value.code}}: ${{value.message}}`,
  );
}}

function toolResult(value: unknown): ToolResult {{
  if (!isRecord(value) || !Array.isArray(value.content) ||
      !("structuredContent" in value) || typeof value.isError !== "boolean") {{
    throw new Error("wildflows: malformed tools/call result");
  }}
  const content: TextContent[] = value.content.map((item) => {{
    if (!isRecord(item) || item.type !== "text" || typeof item.text !== "string") {{
      throw new Error("wildflows: malformed text content");
    }}
    return {{ type: "text", text: item.text }};
  }});
  return {{
    content,
    structuredContent: value.structuredContent,
    isError: value.isError,
  }};
}}

function allocateCallIndex(
  name: string,
  arguments_: Record<string, unknown>,
): number {{
  const encoded = JSON.stringify(arguments_);
  const replay = replayCalls.find((call) =>
    !claimedReplayCalls.has(call.callIndex) && call.name === name &&
    JSON.stringify(call.arguments) === encoded
  );
  if (replay !== undefined) {{
    claimedReplayCalls.add(replay.callIndex);
    return replay.callIndex;
  }}
  return nextCallIndex++;
}}

const retryInitialDelayMs = 100;
const retryMaximumDelayMs = 1_000;

function abortError(): Error {{
  return new Error("wildflows: tool call aborted");
}}

function throwIfAborted(signal: AbortSignal): void {{
  if (signal.aborted) {{
    throw abortError();
  }}
}}

function waitForRetry(delayMs: number, signal: AbortSignal): Promise<void> {{
  return new Promise((resolve, reject) => {{
    if (signal.aborted) {{
      reject(abortError());
      return;
    }}
    const onAbort = () => {{
      clearTimeout(timer);
      reject(abortError());
    }};
    const timer = setTimeout(() => {{
      signal.removeEventListener("abort", onAbort);
      resolve();
    }}, delayMs);
    signal.addEventListener("abort", onAbort, {{ once: true }});
  }});
}}

async function callAttempt(
  body: string,
  callIndex: number,
  signal: AbortSignal,
): Promise<ToolResult> {{
  const response = await fetch(endpoint, {{
    method: "POST",
    headers: {{
      "content-type": "application/json",
      Authorization: `Bearer ${{token}}`,
      "X-Wildflows-Frame": frameId,
    }},
    body,
    signal,
  }});
  if (!response.ok) {{
    throw new Error(`wildflows MCP HTTP ${{response.status}}`);
  }}
  const payload: unknown = await response.json();
  if (!isRecord(payload) || payload.jsonrpc !== "2.0" || payload.id !== callIndex) {{
    throw new Error("wildflows: malformed JSON-RPC response");
  }}
  if ("error" in payload) {{
    const error = engineError(payload.error);
    if (error !== undefined) {{
      throw error;
    }}
    throw new Error("wildflows: malformed JSON-RPC error");
  }}
  if (!("result" in payload)) {{
    throw new Error("wildflows: JSON-RPC response omitted result");
  }}
  return toolResult(payload.result);
}}

async function callTool(
  name: string,
  arguments_: Record<string, unknown>,
  signal: AbortSignal,
): Promise<ToolResult> {{
  // Allocate before awaiting fetch, so concurrent Pi tool calls cannot reuse an
  // index. Exact calls from a replay digest reclaim their durable logical index.
  if (sealedFrontierError !== undefined) {{
    throw sealedFrontierError;
  }}
  throwIfAborted(signal);
  const callIndex = allocateCallIndex(name, arguments_);
  const body = JSON.stringify({{
    jsonrpc: "2.0",
    id: callIndex,
    method: "tools/call",
    params: {{
      name,
      arguments: arguments_,
      _meta: {{ wildflows: {{ callIndex }} }},
    }},
  }});
  let delayMs = retryInitialDelayMs;
  try {{
    for (;;) {{
      throwIfAborted(signal);
      try {{
        return await callAttempt(body, callIndex, signal);
      }} catch (error) {{
        throwIfAborted(signal);
        if (error instanceof EngineCallError) {{
          throw error;
        }}
        await waitForRetry(delayMs, signal);
        delayMs = Math.min(delayMs * 2, retryMaximumDelayMs);
      }}
    }}
  }} catch (error) {{
    if (signal.aborted) {{
      sealedFrontierError = abortError();
      throw sealedFrontierError;
    }}
    throw error;
  }}
}}

function piResult(result: ToolResult) {{
  return {{
    content: result.content,
    details: {{
      structuredContent: result.structuredContent,
      isError: result.isError,
    }},
  }};
}}

export default function (pi: ExtensionAPI) {{
  pi.registerTool({{
    name: "wildflows_dispatch",
    label: "Wildflows dispatch",
    description: "Dispatch tasks serially by default and block until their results and commits are integrated. Set retry_frame alone to relaunch a failed direct child on its existing branch. Set parallel only for independent fan-out; use sequential calls to compose pipelines, loops, and parallel-then-review shapes.",
    parameters: Type.Object({{
      tasks: Type.Optional(Type.Array(Type.String({{ description: "Self-contained child task" }}))),
      rig: Type.Optional(Type.String({{ description: "Explicit rig for all tasks; omit when kinds map to defaults" }})),
      parallel: Type.Optional(Type.Boolean({{ description: "Run siblings in parallel" }})),
      skills: Type.Optional(Type.Array(Type.Array(Type.String({{
        description: "Bundled skill name for this task",
      }}), {{ description: "Ordered skill bundle for one task" }}), {{
        description: "One ordered skill-name list per task",
      }})),
      kinds: Type.Optional(Type.Array(Type.String({{
        description: "Free-text task kind (suggested: implement, review, research, artifact)",
      }}), {{ description: "One optional kind hint per task" }})),
      retry_frame: Type.Optional(Type.String({{
        description: "Failed direct child frame id to relaunch on its existing branch",
      }})),
    }}),
    async execute(_toolCallId, params, signal, _onUpdate, _ctx) {{
      const tasks = params.tasks ?? [];
      return piResult(await callTool("dispatch", {{
        tasks,
        rig: params.rig,
        parallel: params.parallel ?? false,
        skills: params.skills ?? tasks.map(() => []),
        kinds: params.kinds ?? [],
        retry_frame: params.retry_frame,
      }}, signal));
    }},
  }});

  pi.registerTool({{
    name: "wildflows_gate",
    label: "Wildflows gate",
    description: "Run a deterministic gate in this frame's worktree.",
    parameters: Type.Object({{
      cmd: Type.String({{ description: "Command to run" }}),
    }}),
    async execute(_toolCallId, params, signal, _onUpdate, _ctx) {{
      return piResult(await callTool("gate", {{ cmd: params.cmd }}, signal));
    }},
  }});

  pi.registerTool({{
    name: "wildflows_ask",
    label: "Wildflows ask",
    description: "Park this frame for an owner decision.",
    parameters: Type.Object({{
      question: Type.String({{ description: "Question for the owner" }}),
    }}),
    async execute(_toolCallId, params, signal, _onUpdate, _ctx) {{
      return piResult(await callTool("ask", {{ question: params.question }}, signal));
    }},
  }});
}}
'''
