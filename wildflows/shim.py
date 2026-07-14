"""Generate the small per-frame Pi extension used by the MCP reference rig."""
from __future__ import annotations

import json
import os
from pathlib import Path

__all__ = ["create_pi_extension", "write_pi_shim"]


def write_pi_shim(
    runtime_dir: Path,
    endpoint: str,
    token: str,
    frame_id: str,
    next_call_index: int,
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
    content = _extension_source(endpoint, token, frame_id, next_call_index)

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
) -> Path:
    """Compatibility spelling for callers creating a Pi extension."""
    return write_pi_shim(runtime_dir, endpoint, token, frame_id, next_call_index)


def _literal(value: str) -> str:
    """Return a JavaScript string literal without interpolation opportunities."""
    return json.dumps(value, ensure_ascii=False)


def _extension_source(
    endpoint: str,
    token: str,
    frame_id: str,
    next_call_index: int,
) -> str:
    return f'''import type {{ ExtensionAPI }} from "@earendil-works/pi-coding-agent";
import {{ Type }} from "typebox";

const endpoint = {_literal(endpoint)};
const token = {_literal(token)};
const frameId = {_literal(frame_id)};
let nextCallIndex = {next_call_index};

type TextContent = {{ type: "text"; text: string }};
type ToolResult = {{
  content: TextContent[];
  structuredContent: unknown;
  isError: boolean;
}};

function isRecord(value: unknown): value is Record<string, unknown> {{
  return typeof value === "object" && value !== null && !Array.isArray(value);
}}

function errorText(value: unknown): string {{
  if (!isRecord(value) || typeof value.code !== "number" ||
      typeof value.message !== "string") {{
    throw new Error("wildflows: malformed JSON-RPC error");
  }}
  return `wildflows MCP error ${{value.code}}: ${{value.message}}`;
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

async function callTool(
  name: string,
  arguments_: Record<string, unknown>,
  signal: AbortSignal,
): Promise<ToolResult> {{
  // Allocate before awaiting fetch, so concurrent Pi tool calls cannot reuse an
  // index and resumed frames retain the same monotonically advancing sequence.
  const callIndex = nextCallIndex++;
  const response = await fetch(endpoint, {{
    method: "POST",
    headers: {{
      "content-type": "application/json",
      Authorization: `Bearer ${{token}}`,
      "X-Wildflows-Frame": frameId,
    }},
    body: JSON.stringify({{
      jsonrpc: "2.0",
      id: callIndex,
      method: "tools/call",
      params: {{
        name,
        arguments: arguments_,
        _meta: {{ wildflows: {{ callIndex }} }},
      }},
    }}),
    signal,
  }});
  if (!response.ok) {{
    throw new Error(`wildflows MCP HTTP ${{response.status}}`);
  }}

  let payload: unknown;
  try {{
    payload = await response.json();
  }} catch {{
    throw new Error("wildflows: MCP response was not JSON");
  }}
  if (!isRecord(payload) || payload.jsonrpc !== "2.0" || payload.id !== callIndex) {{
    throw new Error("wildflows: malformed JSON-RPC response");
  }}
  if ("error" in payload) {{
    throw new Error(errorText(payload.error));
  }}
  if (!("result" in payload)) {{
    throw new Error("wildflows: JSON-RPC response omitted result");
  }}
  return toolResult(payload.result);
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
    description: "Dispatch child frame tasks through the wildflows engine.",
    parameters: Type.Object({{
      tasks: Type.Array(Type.String({{ description: "Self-contained child task" }})),
      rig: Type.String({{ description: "Allowed wildflows rig name" }}),
      parallel: Type.Optional(Type.Boolean({{ description: "Run siblings in parallel" }})),
    }}),
    async execute(_toolCallId, params, signal, _onUpdate, _ctx) {{
      return piResult(await callTool("dispatch", {{
        tasks: params.tasks,
        rig: params.rig,
        parallel: params.parallel ?? false,
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
