from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

from tests.conftest import executable
from wildflows.frame import FrameRuntime
from wildflows.rig import ScriptRig
from wildflows.rigconfig import load_rigs
from wildflows.shim import write_pi_shim


def test_pi_shim_is_private_and_outside_worktree(tmp_path: Path) -> None:
    worktree = tmp_path / "repo"
    worktree.mkdir()
    runtime = tmp_path / "run" / "runtime" / "f0"
    shim = write_pi_shim(
        runtime, "http://127.0.0.1:1234/mcp", "secret-token", "f0", 3
    )
    assert not shim.is_relative_to(worktree)
    assert shim.stat().st_mode & 0o777 == 0o600
    source = shim.read_text(encoding="utf-8")
    assert 'const endpoint = "http://127.0.0.1:1234/mcp"' in source
    assert 'const token = "secret-token"' in source
    assert "let nextCallIndex = 3" in source
    assert "wildflows_dispatch" in source
    assert "wildflows_gate" in source
    assert "wildflows_ask" in source


def test_pi_shim_carries_replay_call_identity(tmp_path: Path) -> None:
    shim = write_pi_shim(
        tmp_path / "runtime",
        "http://127.0.0.1:1/mcp",
        "token",
        "f0",
        2,
        [(0, "gate", {"cmd": "true"}), (1, "ask", {"question": "ship?"})],
    )
    source = shim.read_text(encoding="utf-8")
    assert '"callIndex": 0' in source
    assert '"name": "gate"' in source
    assert "allocateCallIndex" in source
    assert "claimedReplayCalls" in source


def test_pi_shim_retries_transport_failures_with_one_request_identity(
    tmp_path: Path,
) -> None:
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node is required to execute the generated Pi extension")

    source = write_pi_shim(
        tmp_path / "runtime",
        "http://127.0.0.1:1/mcp",
        "token",
        "f0",
        12,
    ).read_text(encoding="utf-8")
    imports = (
        'import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";\n'
        'import { Type } from "typebox";\n'
    )
    assert imports in source
    extension = tmp_path / "extension.ts"
    extension.write_text(
        source.replace(
            imports,
            """const Type = {
  Object: (value) => value,
  Array: (value) => value,
  String: (value) => value,
  Optional: (value) => value,
  Boolean: () => ({}),
};
""",
        ),
        encoding="utf-8",
    )
    runner = tmp_path / "run-extension.mjs"
    runner.write_text(
        """import extension from "./extension.ts";

const tools = new Map();
extension({ registerTool(tool) { tools.set(tool.name, tool); } });
const gate = tools.get("wildflows_gate");
if (gate === undefined) {
  throw new Error("gate tool was not registered");
}

const requestBodies = [];
const requests = [];
globalThis.fetch = async (_endpoint, init) => {
  if (typeof init?.body !== "string") {
    throw new Error("request body was not a string");
  }
  requestBodies.push(init.body);
  requests.push(JSON.parse(init.body));
  if (requests.length === 1) {
    throw new TypeError("temporary transport failure");
  }
  if (requests.length === 2) {
    return { ok: false, status: 502, json: async () => ({}) };
  }
  if (requests.length === 3) {
    return { ok: true, json: async () => { throw new SyntaxError("truncated JSON"); } };
  }
  if (requests.length === 4) {
    return { ok: true, json: async () => ({ jsonrpc: "2.0", id: 999 }) };
  }
  return {
    ok: true,
    json: async () => ({
      jsonrpc: "2.0",
      id: requests[0].id,
      result: {
        content: [{ type: "text", text: "done" }],
        structuredContent: { ok: true },
        isError: false,
      },
    }),
  };
};

const result = await gate.execute(
  "tool-call",
  { cmd: "true" },
  new AbortController().signal,
  () => {},
  {},
);
if (result.content[0]?.text !== "done" || requests.length !== 5) {
  throw new Error("transport failures were not retried through success");
}
const firstRequestBody = requestBodies[0];
if (requestBodies.some((body) => body !== firstRequestBody)) {
  throw new Error("retry changed the JSON-RPC request identity");
}
const request = requests[0];
if (
  request.id !== 12 || request.method !== "tools/call" ||
  request.params.name !== "gate" || request.params.arguments.cmd !== "true" ||
  request.params._meta.wildflows.callIndex !== 12
) {
  throw new Error("retry request did not preserve the allocated call index");
}

let preAbortedAttempts = 0;
globalThis.fetch = async () => {
  preAbortedAttempts += 1;
  throw new Error("pre-aborted call reached fetch");
};
const preAbortedController = new AbortController();
preAbortedController.abort();
try {
  await gate.execute(
    "tool-call",
    { cmd: "pre-aborted" },
    preAbortedController.signal,
    () => {},
    {},
  );
  throw new Error("pre-aborted call unexpectedly resolved");
} catch (error) {
  if (!(error instanceof Error) || error.message !== "wildflows: tool call aborted") {
    throw error;
  }
}
if (preAbortedAttempts !== 0) {
  throw new Error("pre-aborted call reached the transport");
}

let protocolAttempts = 0;
globalThis.fetch = async () => {
  protocolAttempts += 1;
  return {
    ok: true,
    json: async () => ({
      jsonrpc: "2.0",
      id: 13,
      error: { code: -32000, message: "engine failure" },
    }),
  };
};
try {
  await gate.execute(
    "tool-call",
    { cmd: "protocol-error" },
    new AbortController().signal,
    () => {},
    {},
  );
  throw new Error("JSON-RPC error unexpectedly resolved");
} catch (error) {
  if (!(error instanceof Error) ||
      error.message !== "wildflows MCP error -32000: engine failure") {
    throw error;
  }
}
if (protocolAttempts !== 1) {
  throw new Error("JSON-RPC error was retried");
}

let abortAttempts = 0;
globalThis.fetch = async () => {
  abortAttempts += 1;
  throw new TypeError("temporary transport failure");
};
const controller = new AbortController();
const aborted = gate.execute("tool-call", { cmd: "false" }, controller.signal, () => {}, {});
setTimeout(() => controller.abort(), 10);
try {
  await aborted;
  throw new Error("aborted retry unexpectedly resolved");
} catch (error) {
  if (!(error instanceof Error) || error.message !== "wildflows: tool call aborted") {
    throw error;
  }
}
if (abortAttempts !== 1) {
  throw new Error("abort retried instead of stopping the transport loop");
}
let postAbortAttempts = 0;
globalThis.fetch = async () => {
  postAbortAttempts += 1;
  throw new Error("sealed frontier reached fetch");
};
try {
  await gate.execute(
    "tool-call",
    { cmd: "later-call" },
    new AbortController().signal,
    () => {},
    {},
  );
  throw new Error("call after an ambiguous abort unexpectedly resolved");
} catch (error) {
  if (!(error instanceof Error) || error.message !== "wildflows: tool call aborted") {
    throw error;
  }
}
if (postAbortAttempts !== 0) {
  throw new Error("call after abort advanced the logical call frontier");
}
""",
        encoding="utf-8",
    )

    subprocess.run(
        [node, "--experimental-strip-types", str(runner)],
        check=True,
        capture_output=True,
        text=True,
    )


def test_script_rig_passes_frame_capability_out_of_band(tmp_path: Path) -> None:
    worktree = tmp_path / "worktree"
    worktree.mkdir()
    script = executable(
        tmp_path / "adapter",
        """#!/usr/bin/env python3
import os, pathlib, sys
args = sys.argv[1:]
pathlib.Path(os.environ['CAPTURE']).write_text('|'.join(args) + '\\n' +
    os.environ['WILDFLOWS_MCP_URL'] + '\\n' +
    os.environ['WILDFLOWS_RUN_TOKEN'] + '\\n' +
    os.environ['WILDFLOWS_FRAME_ID'] + '\\n' +
    os.environ['WILDFLOWS_PI_EXTENSION'])
print('adapter report')
""",
    )
    capture = tmp_path / "capture"
    shim = tmp_path / "shim.ts"
    shim.write_text("shim", encoding="utf-8")
    rig = ScriptRig(
        script,
        tmp_path / "logs",
        timeout_s=10,
        env={"CAPTURE": str(capture)},
    )
    runtime = FrameRuntime(
        endpoint="http://127.0.0.1:1/mcp",
        token="token",
        frame_id="f0",
        shim_path=shim,
        runtime_dir=tmp_path / "runtime",
        next_call_index=0,
    )
    result = rig.run("job", worktree, runtime)
    assert result.outcome == "ok"
    assert result.stdout == "adapter report\n"
    recorded = capture.read_text(encoding="utf-8")
    assert f"--worktree|{worktree}" in recorded
    assert "http://127.0.0.1:1/mcp\ntoken\nf0\n" in recorded


def test_worker_picodex_loads_generated_extension(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    worktree = tmp_path / "worktree"
    worktree.mkdir()
    prompt = tmp_path / "prompt"
    prompt.write_text("hello", encoding="utf-8")
    log_dir = tmp_path / "logs"
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    args_file = tmp_path / "pi-args"
    executable(
        bin_dir / "pi",
        "#!/usr/bin/env bash\nprintf '%s\\n' \"$*\" > \"$PI_ARGS\"\ncat\n",
    )
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("PI_ARGS", str(args_file))
    shim = tmp_path / "extension.ts"
    shim.write_text("extension", encoding="utf-8")
    monkeypatch.setenv("WILDFLOWS_PI_EXTENSION", str(shim))
    adapter = Path("rigs/worker-picodex.sh").resolve()
    process = subprocess.run(
        [
            str(adapter),
            "--worktree", str(worktree),
            "--prompt", str(prompt),
            "--log-dir", str(log_dir),
            "--handle-out", str(tmp_path / "handle"),
            "--timeout", "10",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert process.stdout == "hello"
    assert f"-e {shim}" in args_file.read_text(encoding="utf-8")


def test_rig_yaml_builds_frame_rigs_relative_to_config(tmp_path: Path) -> None:
    executable(tmp_path / "adapter", "#!/bin/sh\ncat \"$4\"\n")
    config = tmp_path / "rigs.yaml"
    config.write_text(
        """rigs:
  root:
    kind: script
    script: adapter
    log_dir: logs
    timeout_s: 10
  local:
    kind: shell
    template: "printf done"
    timeout_s: 5
""",
        encoding="utf-8",
    )
    registry = load_rigs(config)
    assert registry.names == frozenset({"root", "local"})
