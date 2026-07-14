import { buildTree, expressionChildren, laneLabel, phaseLayout } from "./tree.js";

const $ = selector => document.querySelector(selector);
const make = (tag, className, text) => {
  const element = document.createElement(tag);
  if (className) element.className = className;
  if (text !== undefined) element.textContent = text;
  return element;
};
const state = {
  runs: [],
  run: null,
  source: null,
  selected: null,
  selectedArtifact: null,
  token: sessionStorage.getItem("wf-token") || "",
  action: null,
  actionTimer: null,
  request: 0,
};

function toast(message, bad = false) {
  const target = $("#toast");
  target.textContent = message;
  target.classList.toggle("bad", bad);
  target.classList.add("show");
  window.setTimeout(() => target.classList.remove("show"), 3200);
}

async function api(path, options = {}) {
  const headers = { ...(options.headers || {}) };
  if (options.body) headers["Content-Type"] = "application/json";
  if (options.method && options.method !== "GET") headers["X-Wildflows-Token"] = state.token;
  const response = await fetch(path, { ...options, headers });
  if (!response.ok) {
    let detail = `${response.status} ${response.statusText}`;
    try {
      detail = (await response.json()).detail || detail;
    } catch {
      detail = detail.trim();
    }
    throw new Error(detail);
  }
  return response.json();
}

function requireToken() {
  if (state.token) return true;
  $("#token-dialog").showModal();
  return false;
}

function shortId(value, left = 8, right = 6) {
  if (!value) return "—";
  const text = String(value);
  return text.length > left + right + 1 ? `${text.slice(0, left)}…${text.slice(-right)}` : text;
}

function formatDuration(seconds) {
  if (!Number.isFinite(seconds) || seconds < 0) return "—";
  if (seconds < 1) return `${Math.round(seconds * 1000)}ms`;
  if (seconds < 100) return `${seconds.toFixed(1)}s`;
  const minutes = Math.floor(seconds / 60);
  return `${minutes}m ${Math.round(seconds % 60)}s`;
}

function formatBytes(value) {
  if (!Number.isFinite(value)) return "—";
  return value < 1024 ? `${value} B` : `${(value / 1024).toFixed(1)} KB`;
}

function basename(path) {
  return String(path || "").split("/").filter(Boolean).at(-1) || "—";
}

function stateTone(value) {
  if (["completed", "integrated"].includes(value)) return "integrated";
  if (["failed", "crashed", "invalid"].includes(value)) return "failed";
  if (["parked", "parked-ask", "railed", "stopped"].includes(value)) return "parked-ask";
  if (value === "running") return "running";
  return "pending";
}

function setDot(target, tone) {
  target.className = `status-dot ${stateTone(tone)}`;
}

function closeStream() {
  if (state.source) state.source.close();
  state.source = null;
  setStreamLive(false);
}

async function loadRuns(preferred) {
  const data = await api("/api/runs");
  state.runs = Array.isArray(data.runs) ? data.runs : [];
  const select = $("#run-select");
  const current = preferred || state.run?.run_id || select.value;
  select.replaceChildren();
  renderRunRail();
  renderStatusBar();
  if (!state.runs.length) {
    select.append(new Option("No runs yet", ""));
    state.run = null;
    state.selected = null;
    state.selectedArtifact = null;
    closeStream();
    render();
    return;
  }
  for (const run of state.runs) {
    const option = new Option(`${shortId(run.run_id)} · ${run.state}`, run.run_id);
    option.disabled = run.state === "invalid";
    select.append(option);
  }
  const selectable = state.runs.filter(run => run.state !== "invalid");
  if (!selectable.length) {
    state.run = null;
    state.selected = null;
    closeStream();
    render();
    return;
  }
  const target = selectable.some(run => run.run_id === current) ? current : selectable[0].run_id;
  select.value = target;
  await selectRun(target);
}

async function selectRun(runId) {
  if (!runId) return;
  closeStream();
  const request = ++state.request;
  const run = await api(`/api/runs/${encodeURIComponent(runId)}`);
  if (request !== state.request) return;
  state.run = run;
  state.selected = run.expression?.node_id && run.nodes[run.expression.node_id] ? run.expression.node_id : Object.keys(run.nodes)[0] || null;
  state.selectedArtifact = null;
  $("#run-select").value = runId;
  $("#flow-viewport").scrollTo(0, 0);
  render();
  const last = run.events.at(-1)?.seq ?? -1;
  const source = new EventSource(`/api/runs/${encodeURIComponent(runId)}/events?after=${last}`);
  state.source = source;
  source.addEventListener("journal", event => receiveEvent(runId, event));
  source.onopen = () => {
    if (state.source === source) setStreamLive(true);
  };
  source.onerror = () => {
    if (state.source === source) setStreamLive(false);
  };
}

async function receiveEvent(runId, message) {
  if (state.run?.run_id !== runId) return;
  const event = JSON.parse(message.data);
  if (!state.run.events.some(item => item.seq === event.seq)) state.run.events.push(event);
  appendEvent(event);
  updateJournalFacts();
  try {
    const fresh = await api(`/api/runs/${encodeURIComponent(runId)}`);
    if (state.run?.run_id === runId) {
      state.run = fresh;
      render();
    }
  } catch (error) {
    toast(error.message, true);
  }
}

function render() {
  renderHeader();
  renderRunRail();
  renderTree();
  renderInspector();
  renderEvents();
  renderStatusBar();
}

function renderHeader() {
  const run = state.run;
  if (!run) {
    $("#run-id").textContent = "no runs";
    const badge = $("#run-state");
    badge.lastChild.textContent = "ready";
    setDot(badge.querySelector(".status-dot"), "pending");
    badge.className = "run-state state-ready";
    $("#rails").textContent = "—";
    $("#deadline-fill").style.width = "0%";
    $("#deadline-track").setAttribute("aria-label", "No deadline");
    setRationale("waiting", "Launch a workflow to begin.");
    renderStats([]);
    $("#resume-button").disabled = true;
    $("#kill-button").disabled = true;
    document.title = "WILDFLOWS";
    return;
  }
  $("#run-id").textContent = shortId(run.run_id);
  $("#run-id").title = run.run_id;
  document.title = `WILDFLOWS — run ${shortId(run.run_id)}`;
  const badge = $("#run-state");
  badge.className = `run-state state-${run.state}`;
  badge.lastChild.textContent = run.state;
  setDot(badge.querySelector(".status-dot"), run.state);
  const rails = railsLabel(run.rails);
  $("#rails").textContent = rails;
  const percent = deadlinePercent(run);
  $("#deadline-fill").style.width = `${percent}%`;
  $("#deadline-track").setAttribute("aria-label", `${percent.toFixed(1)} percent of deadline used`);
  setRationale(run.epoch == null ? "planner" : `epoch ${run.epoch}`, run.rationale || run.completed?.summary || "Waiting for planner rationale");
  const events = run.events || [];
  const elapsed = eventWindow(events, run.active);
  const nodeCount = Object.keys(run.nodes || {}).length;
  const parallel = maxParallel(run.expression);
  const exitTone = ["failed", "crashed", "invalid"].includes(run.state) ? "failed" : run.state === "running" ? "running" : "integrated";
  renderStats([
    { value: nodeCount, label: `node${nodeCount === 1 ? "" : "s"}` },
    { value: parallel, label: "parallel" },
    { value: run.epoch_count, label: `epoch${run.epoch_count === 1 ? "" : "s"}` },
    { value: formatDuration(elapsed), label: "elapsed" },
    { value: run.state === "running" ? "live" : ["failed", "crashed"].includes(run.state) ? "exit" : "exit", label: run.state === "running" ? "active" : exitTone === "failed" ? "failed" : "ok", tone: exitTone },
  ]);
  $("#resume-button").disabled = Boolean(run.active) || run.state === "completed";
  $("#kill-button").disabled = !run.killable;
}

function setRationale(prefix, copy) {
  const target = $("#rationale");
  target.replaceChildren();
  const strong = make("strong", "", prefix);
  target.append(strong, document.createTextNode(` — ${copy}`));
  target.title = copy;
}

function railsLabel(rails) {
  if (!rails) return "open rails";
  const bits = [];
  if (rails.deadline_s != null) bits.push(`${Math.round(rails.deadline_s)}s`);
  if (rails.max_epochs != null) bits.push(`${rails.max_epochs} epochs`);
  return bits.join(" · ") || "open rails";
}

function deadlinePercent(run) {
  const deadline = Number(run.rails?.deadline_s);
  if (!Number.isFinite(deadline) || deadline <= 0) return 0;
  const start = Number(run.started_at) || Number(run.events?.[0]?.ts);
  if (!Number.isFinite(start)) return 0;
  const end = run.active ? Date.now() / 1000 : Number(run.events?.at(-1)?.ts) || start;
  return Math.max(0, Math.min(100, ((end - start) / deadline) * 100));
}

function eventWindow(events, live = false) {
  if (!events?.length) return 0;
  const start = Number(events[0].ts);
  const end = live ? Date.now() / 1000 : Number(events.at(-1).ts);
  return Number.isFinite(start) && Number.isFinite(end) ? Math.max(0, end - start) : 0;
}

function renderStats(items) {
  const target = $("#run-stats");
  target.replaceChildren();
  for (const item of items) {
    const chip = make("span", "stat-chip");
    if (item.tone) {
      const dot = make("span", "status-dot");
      setDot(dot, item.tone);
      chip.append(dot);
    }
    chip.append(make("b", "", String(item.value)), document.createTextNode(` ${item.label}`));
    target.append(chip);
  }
}

function maxParallel(expr) {
  if (!expr || typeof expr !== "object") return 0;
  const own = expr.kind === "dispatch" && Array.isArray(expr.children) ? expr.children.length : 0;
  return Math.max(own, ...expressionChildren(expr).map(maxParallel), 0);
}

function renderRunRail() {
  const target = $("#live-runs");
  target.replaceChildren();
  if (!state.runs.length) {
    target.append(make("div", "recent-empty", "No runs yet"));
    return;
  }
  const ordered = [...state.runs].sort((left, right) => Number(right.state === "running") - Number(left.state === "running"));
  for (const run of ordered.slice(0, 4)) {
    const button = make("button", `recent-run${state.run?.run_id === run.run_id ? " selected" : ""}`);
    button.type = "button";
    button.title = run.run_id;
    const dot = make("span", "status-dot");
    setDot(dot, run.state);
    const copy = make("span", "recent-copy");
    copy.append(make("span", "recent-id", shortId(run.run_id)), make("span", "recent-meta", `${run.state} · ${run.epoch_count || 0} epoch${run.epoch_count === 1 ? "" : "s"}`));
    button.append(dot, copy);
    button.addEventListener("click", () => selectRun(run.run_id).catch(error => toast(error.message, true)));
    target.append(button);
  }
}

function expressionSExpr(expr) {
  if (!expr || typeof expr !== "object") return "";
  const id = expr.node_id || "?";
  const rig = expr.rig?.name ? ` :rig ${expr.rig.name}` : "";
  if (expr.kind === "dispatch" || expr.kind === "seq") return `(${expr.kind} ${id}${expressionChildren(expr).map(child => ` ${expressionSExpr(child)}`).join("")})`;
  if (expr.kind === "combine") return `(combine ${id}${rig}${expressionChildren(expr).map(child => ` ${expressionSExpr(child)}`).join("")})`;
  if (expr.kind === "loop") return `(loop ${id} :cap ${expr.cap} ${expressionSExpr(expr.body)})`;
  if (expr.kind === "do") return `(do ${id}${rig} ${JSON.stringify(expr.task)})`;
  if (expr.kind === "ask") return `(ask ${id} ${JSON.stringify(expr.question)})`;
  if (expr.kind === "setup") return `(setup ${id} ${JSON.stringify(expr.cmd)})`;
  if (expr.kind === "inplace") return `(inplace ${id}${(expr.edits || []).map(edit => ` ${JSON.stringify(edit.path)}`).join("")})`;
  return `(${expr.kind || "unknown"} ${id})`;
}

function renderTree() {
  const grid = $("#lane-grid");
  const empty = $("#tree-empty");
  const stage = $("#flow-stage");
  const expression = state.run?.expression;
  grid.replaceChildren();
  $("#flow-lines").replaceChildren();
  if (!expression) {
    empty.hidden = false;
    empty.textContent = state.run ? "No admitted expression in the current epoch." : "Select or launch a run to inspect its expression.";
    $("#expression-line").textContent = "No admitted expression";
    $("#expression-line").title = "";
    $("#epoch-chip").lastChild.textContent = "no epoch";
    setDot($("#epoch-chip .status-dot"), "pending");
    stage.style.setProperty("--lane-count", "1");
    return;
  }
  empty.hidden = true;
  const sexpr = expressionSExpr(expression);
  $("#expression-line").textContent = sexpr;
  $("#expression-line").title = sexpr;
  const root = buildTree(expression, state.run.nodes);
  const layout = phaseLayout(root);
  stage.style.setProperty("--lane-count", String(layout.lanes.length));
  const closed = state.run.closed_epochs > state.run.epoch;
  $("#epoch-chip").lastChild.textContent = `epoch ${state.run.epoch} / ${closed ? "closed" : state.run.state}`;
  setDot($("#epoch-chip .status-dot"), closed ? "integrated" : state.run.state);
  for (const lane of layout.lanes) {
    const column = make("section", "lane");
    const header = make("div", "lane-label");
    header.append(make("span", "lane-number", String(lane.index + 1).padStart(2, "0")), document.createTextNode(laneLabel(lane.nodes)));
    if (lane.index < layout.lanes.length - 1) header.append(make("span", "lane-arrow", "→"));
    const cards = make("div", "lane-nodes");
    for (const node of lane.nodes) cards.append(renderNodeCard(node));
    column.append(header, cards);
    grid.append(column);
  }
  window.requestAnimationFrame(() => drawConnectors(layout.edges));
}

function renderNodeCard(node) {
  const card = make("button", `node-card state-${node.state}${state.selected === node.id ? " selected" : ""}`);
  card.type = "button";
  card.dataset.node = node.id;
  card.title = node.label;
  const dot = make("span", "status-dot");
  setDot(dot, node.state);
  const top = make("span", "node-top");
  top.append(make("span", "node-id", node.id), make("span", "kind-chip", node.rig || node.kind));
  if (node.children.length > 1) top.append(make("span", "fan-count", `× ${node.children.length}`));
  const duration = nodeDuration(node.id);
  if (duration != null) top.append(make("span", "duration", formatDuration(duration)));
  const meta = [];
  if (node.detail.artifact) meta.push(basename(node.detail.artifact));
  if (node.kind === "loop" && node.detail.loop_iterations != null) meta.push(`${node.detail.loop_iterations} iteration${node.detail.loop_iterations === 1 ? "" : "s"}`);
  meta.push(node.state.replace("parked-ask", "owner input"));
  card.append(dot, top, make("span", "node-sub", node.label), make("span", "node-meta", meta.join(" · ")));
  if (node.state === "failed") card.append(make("span", "node-reason", failureReason(node.detail)));
  if (node.kind === "combine" && node.detail.result?.text) card.append(make("span", "combine-preview", node.detail.result.text));
  card.addEventListener("click", () => {
    state.selected = node.id;
    state.selectedArtifact = null;
    renderTree();
    renderInspector();
  });
  return card;
}

function failureReason(detail) {
  return String(detail.result?.text || detail.result?.outcome || "execution failed").split("\n")[0].slice(0, 100);
}

function nodeDuration(nodeId) {
  const events = state.run?.events || [];
  let started = null;
  let duration = null;
  for (const event of events) {
    if (event.node_id !== nodeId) continue;
    if (event.kind === "dispatched") started = Number(event.ts);
    if (event.kind === "result" && Number.isFinite(started)) duration = Math.max(0, Number(event.ts) - started);
  }
  if (duration != null) return duration;
  if (started != null && state.run?.active) return Math.max(0, Date.now() / 1000 - started);
  return null;
}

function drawConnectors(edges) {
  const svg = $("#flow-lines");
  const stage = $("#flow-stage");
  const bounds = stage.getBoundingClientRect();
  const width = Math.max(stage.scrollWidth, bounds.width);
  const height = Math.max(stage.scrollHeight, bounds.height);
  svg.replaceChildren();
  svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
  svg.setAttribute("width", String(width));
  svg.setAttribute("height", String(height));
  const namespace = "http://www.w3.org/2000/svg";
  const defs = document.createElementNS(namespace, "defs");
  const marker = document.createElementNS(namespace, "marker");
  marker.setAttribute("id", "flow-arrow");
  marker.setAttribute("markerWidth", "6");
  marker.setAttribute("markerHeight", "6");
  marker.setAttribute("refX", "5");
  marker.setAttribute("refY", "3");
  marker.setAttribute("orient", "auto");
  const arrow = document.createElementNS(namespace, "path");
  arrow.setAttribute("d", "M0 0L6 3L0 6Z");
  arrow.setAttribute("fill", "var(--violet)");
  marker.append(arrow);
  defs.append(marker);
  svg.append(defs);
  for (const [from, to] of edges) {
    const source = stage.querySelector(`[data-node="${CSS.escape(from)}"]`);
    const target = stage.querySelector(`[data-node="${CSS.escape(to)}"]`);
    if (!source || !target) continue;
    const left = source.getBoundingClientRect();
    const right = target.getBoundingClientRect();
    const x1 = left.right - bounds.left;
    const y1 = left.top + left.height / 2 - bounds.top;
    const x2 = right.left - bounds.left;
    const y2 = right.top + right.height / 2 - bounds.top;
    const bend = Math.max(16, (x2 - x1) * 0.45);
    const path = document.createElementNS(namespace, "path");
    path.setAttribute("d", `M${x1} ${y1} C${x1 + bend} ${y1} ${x2 - bend} ${y2} ${x2} ${y2}`);
    path.setAttribute("fill", "none");
    path.setAttribute("stroke", "var(--violet)");
    path.setAttribute("stroke-width", "1.25");
    path.setAttribute("vector-effect", "non-scaling-stroke");
    path.setAttribute("marker-end", "url(#flow-arrow)");
    svg.append(path);
  }
}

function findExpression(expr, nodeId) {
  if (!expr || typeof expr !== "object") return null;
  if (expr.node_id === nodeId) return expr;
  for (const child of expressionChildren(expr)) {
    const found = findExpression(child, nodeId);
    if (found) return found;
  }
  return null;
}

function renderInspector() {
  const target = $("#inspector-body");
  target.replaceChildren();
  const detail = state.selected ? state.run?.nodes?.[state.selected] : null;
  if (!detail) {
    $("#inspector-count").textContent = "no selection";
    $("#node-state").lastChild.textContent = "pending";
    setDot($("#node-state .status-dot"), "pending");
    target.className = "inspector-body empty-pane";
    target.textContent = state.run ? "No node is available in the current expression." : "Select a node to inspect its task, result, receipts, and artifacts.";
    return;
  }
  const expression = findExpression(state.run.expression, detail.node_id) || detail.expression;
  $("#inspector-count").textContent = `${detail.node_id} · ${detail.kind}`;
  $("#node-state").lastChild.textContent = detail.state.replace("parked-ask", "owner input");
  setDot($("#node-state .status-dot"), detail.state);
  target.className = "inspector-body";
  const main = make("div", "inspector-main");
  main.append(copyBlock("Task", detail.task || expressionSExpr(expression), "task-copy"));
  const children = expressionChildren(expression);
  if (children.length) main.append(renderInputs(children));
  const ask = state.run.pending_questions.find(item => item.node_id === detail.node_id);
  if (ask) main.append(renderAsk(ask));
  if (detail.result) main.append(resultBlock(detail.result));
  if (detail.loop_status) main.append(copyBlock("Loop", `${detail.loop_status} · ${detail.loop_iterations} iteration${detail.loop_iterations === 1 ? "" : "s"}`, "task-copy"));
  if (detail.receipts.length) main.append(copyBlock("Integration receipt", receiptSummary(detail.receipts), "result-box receipt-box"));
  const files = inspectorFiles(detail);
  const selectedFile = files.find(item => item.path === state.selectedArtifact) || detail.artifacts[0] || null;
  if (selectedFile) {
    state.selectedArtifact = selectedFile.path;
    main.append(renderArtifactPreview(selectedFile));
  }
  const side = renderInspectorSide(detail, files);
  target.append(main, side);
}

function copyBlock(label, value, className) {
  const block = make("section", "copy-block");
  block.append(make("div", "micro-label", label), make("p", className, value || "—"));
  return block;
}

function renderInputs(children) {
  const wrap = make("section", "worker-inputs");
  wrap.setAttribute("aria-label", "Child tasks and results");
  for (const child of children) {
    const detail = state.run.nodes[child.node_id] || {};
    const card = make("article", "input-card");
    const head = make("div", "input-head");
    head.append(make("span", "", `${child.node_id} · ${detail.rig || child.rig?.name || child.kind}`), make("span", "", formatDuration(nodeDuration(child.node_id))));
    card.append(head, make("p", "prompt-line", detail.task || child.task || child.question || child.cmd || expressionSExpr(child)));
    if (detail.result?.text) card.append(make("p", "answer-line", detail.result.text));
    else card.append(make("p", "answer-line muted-copy", detail.state || "pending"));
    wrap.append(card);
  }
  return wrap;
}

function resultBlock(result) {
  const block = make("section", "copy-block");
  block.append(make("div", "micro-label", "Result"));
  const box = make("div", `result-box${result.outcome === "failed" ? " failed-result" : ""}`);
  box.append(make("span", "result-key", `${result.outcome || "result"} /`), document.createTextNode(result.text || "No result text"));
  block.append(box);
  return block;
}

function receiptSummary(receipts) {
  const commits = receipts.flatMap(receipt => receipt.sha ? [receipt] : receipt.commits || []);
  if (!commits.length) return JSON.stringify(receipts, null, 2);
  return commits.map(commit => `${shortId(commit.sha)} · ${(commit.paths || []).join(", ") || "no paths"}`).join("\n");
}

function inspectorFiles(detail) {
  const own = detail.artifacts || [];
  const paths = new Set(own.map(item => item.path));
  return [...own, ...(state.run.files || []).filter(item => !paths.has(item.path))];
}

function renderInspectorSide(detail, files) {
  const side = make("aside", "inspector-side");
  side.setAttribute("aria-label", "Selected node metadata");
  side.append(make("div", "micro-label", "Node detail"));
  const list = make("dl", "detail-list");
  const result = detail.result;
  list.append(
    detailRow("node", detail.node_id),
    detailRow("kind", detail.kind),
    detailRow("rig", detail.rig || "—", detail.rig ? "rig-chip" : ""),
    detailRow("duration", formatDuration(nodeDuration(detail.node_id))),
    detailRow("exit", result ? `${result.exit_code ?? "—"} / ${result.outcome}` : "—"),
    detailRow("dispatches", detail.dispatch_count),
  );
  side.append(list, make("div", "micro-label", `Run artifacts · ${files.length}`));
  const artifacts = make("div", "artifact-list");
  if (!files.length) artifacts.append(make("span", "artifact-empty", "No public artifacts"));
  for (const item of files) {
    const link = make("a", `artifact-chip${state.selectedArtifact === item.path ? " selected" : ""}`);
    link.href = item.url;
    link.target = "_blank";
    link.rel = "noopener";
    link.title = `${item.path} · ${formatBytes(item.size)}`;
    link.append(make("span", "file-mark"), make("span", "artifact-name", item.name));
    link.addEventListener("click", () => {
      state.selectedArtifact = item.path;
      window.setTimeout(renderInspector, 0);
    });
    artifacts.append(link);
  }
  side.append(artifacts);
  const decision = files.find(item => item.path.startsWith("decisions/"));
  if (decision) side.append(make("div", "decision-note", `decision / ${decision.name}`));
  return side;
}

function detailRow(label, value, valueClass = "") {
  const row = make("div", "detail-row");
  const term = make("dt", "", label);
  const description = make("dd");
  description.append(make("span", valueClass, String(value ?? "—")));
  row.append(term, description);
  return row;
}

function renderArtifactPreview(item) {
  const block = make("section", "copy-block artifact-preview");
  const heading = make("div", "preview-head");
  heading.append(make("div", "micro-label", "Artifact preview"));
  const link = make("a", "preview-link", `${item.name} ↗`);
  link.href = item.url;
  link.target = "_blank";
  link.rel = "noopener";
  heading.append(link);
  const frame = make("div", "preview-frame");
  block.append(heading, frame);
  if (item.mime.startsWith("image/")) {
    const image = make("img");
    image.src = item.url;
    image.alt = item.name;
    frame.append(image);
  } else if (item.mime === "text/html") {
    const iframe = make("iframe");
    iframe.src = item.url;
    iframe.sandbox = "";
    iframe.title = item.name;
    frame.append(iframe);
  } else if (item.mime.startsWith("text/") || item.mime === "application/json") {
    const output = make("pre", "preview-text", "Loading…");
    frame.append(output);
    fetch(item.url).then(response => response.text()).then(text => {
      if (state.selectedArtifact === item.path) output.textContent = text;
    }).catch(() => {
      output.textContent = "Preview unavailable";
    });
  } else {
    frame.append(make("p", "preview-unavailable", `${item.mime} · ${formatBytes(item.size)} · Open the artifact to inspect it.`));
  }
  return block;
}

function renderAsk(ask) {
  const form = make("form", "ask-card");
  form.append(make("span", "micro-label", "Owner input required"), make("h3", "", ask.question));
  const input = make("textarea");
  input.placeholder = "Answer the planner…";
  input.required = true;
  if (ask.options.length) {
    const options = make("div", "ask-options");
    for (const option of ask.options) {
      const button = make("button", "option-chip", option);
      button.type = "button";
      button.addEventListener("click", () => {
        input.value = option;
        input.focus();
      });
      options.append(button);
    }
    form.append(options);
  }
  const submit = make("button", "btn primary", "Answer & resume");
  submit.type = "submit";
  form.append(input, submit);
  form.addEventListener("submit", event => {
    event.preventDefault();
    if (!requireToken()) return;
    mutate(`/api/runs/${encodeURIComponent(state.run.run_id)}/answer`, { answer: input.value, node_id: ask.node_id }, "Answer submitted").catch(() => {});
  });
  return form;
}

function renderEvents() {
  const target = $("#journal");
  target.replaceChildren();
  for (const event of state.run?.events || []) appendEvent(event, false);
  target.scrollTop = target.scrollHeight;
  updateJournalFacts();
}

function appendEvent(event, scroll = true) {
  const target = $("#journal");
  if (target.querySelector(`[data-seq="${event.seq}"]`)) return;
  const row = make("div", `event-row event-${event.kind}`);
  row.dataset.seq = String(event.seq);
  const time = make("time", "event-time", formatEventTime(event.ts));
  const kind = make("span", `event-kind kind-${event.kind}`, eventKind(event.kind));
  const node = make("span", "event-node", event.node_id || `e${event.epoch}`);
  const detail = make("span", "event-detail", eventSummary(event));
  detail.title = detail.textContent;
  const ref = make("span", "event-ref", eventRef(event));
  ref.title = eventRef(event, false);
  row.append(time, kind, node, detail, ref);
  target.append(row);
  if (scroll) target.scrollTop = target.scrollHeight;
}

function eventKind(kind) {
  if (kind === "dispatched") return "dispatch";
  if (kind === "loop_iter") return "loop";
  return kind;
}

function eventSummary(event) {
  if (event.kind === "boundary") return `epoch ${event.epoch} ${event.phase}${event.reason ? ` · ${event.reason}` : ""}`;
  if (event.kind === "dispatched") return `${event.rig || (event.host ? "host" : "worker")} started${event.task ? ` · ${event.task}` : event.cmd ? ` · ${event.cmd}` : ""}`;
  if (event.kind === "result") return `${event.exit_code ?? "—"} / ${event.outcome}${event.text ? ` · ${event.text.replace(/\s+/g, " ")}` : ""}`;
  if (event.kind === "integrated") return `${event.commits?.length || 0} commit${event.commits?.length === 1 ? "" : "s"} integrated`;
  if (event.kind === "asked") return event.question;
  if (event.kind === "answered") return event.answer;
  if (event.kind === "judged") return `${event.verdict} · ${event.ok ? "accepted" : "rejected"}`;
  if (event.kind === "loop_iter") return `iteration ${event.iteration + 1}${event.converged ? " · converged" : ""}`;
  return event.kind;
}

function eventRef(event, compact = true) {
  if (event.kind === "dispatched" && event.workdir) {
    const worktree = basename(event.workdir);
    return compact ? `wt/${shortId(worktree, 8, 4)}` : `wt/${worktree}`;
  }
  if (event.kind === "result" && event.artifact) return basename(event.artifact);
  if (event.kind === "integrated" && event.commits?.length) return shortId(event.commits.at(-1).sha);
  if (event.kind === "boundary") {
    const value = event.phase === "opened" ? event.run_branch || event.base_commit : event.reason;
    return value || `epoch/${event.epoch}`;
  }
  if (event.kind === "loop_iter" && event.commit) return shortId(event.commit);
  if (event.kind === "asked" && event.options?.length) return `${event.options.length} options`;
  return `seq/${String(event.seq).padStart(3, "0")}`;
}

function formatEventTime(timestamp) {
  const date = new Date(Number(timestamp) * 1000);
  if (Number.isNaN(date.getTime())) return "—";
  return `${date.toLocaleTimeString([], { hour12: false, hour: "2-digit", minute: "2-digit", second: "2-digit" })}.${String(date.getMilliseconds()).padStart(3, "0")}`;
}

function updateJournalFacts() {
  const events = state.run?.events || [];
  $("#journal-count").textContent = `${events.length} event${events.length === 1 ? "" : "s"}`;
  const facts = $("#journal-facts");
  facts.replaceChildren();
  const opened = events.find(event => event.kind === "boundary" && event.phase === "opened");
  const decision = state.run?.files?.find(item => item.path.startsWith("decisions/"));
  if (opened?.base_commit) facts.append(factRow("base commit", shortId(opened.base_commit, 12, 4)));
  if (decision) facts.append(factRow("decision", decision.name));
  if (!facts.childNodes.length) facts.append(factRow("projection", state.run ? `epoch ${state.run.epoch ?? "—"}` : "no run selected"));
  const foot = $("#journal-foot");
  foot.replaceChildren();
  const first = events[0];
  const last = events.at(-1);
  foot.append(make("span", "", first ? `seq ${String(first.seq).padStart(3, "0")}–${String(last.seq).padStart(3, "0")}` : "seq —"), make("span", "", `${formatDuration(eventWindow(events))} window`));
}

function factRow(label, value) {
  const row = make("div", "fact-row");
  row.append(make("span", "", label), make("b", "", value));
  return row;
}

function setStreamLive(live) {
  setDot($("#live-dot"), live ? "integrated" : "pending");
  $("#live-label").textContent = live ? "Live" : "Offline";
  const stream = $("#stream-state");
  setDot(stream.querySelector(".status-dot"), live ? "integrated" : "pending");
  stream.lastChild.textContent = live ? "live event stream" : "reconnecting";
  const status = $("#status-live");
  setDot(status.querySelector(".status-dot"), live ? "integrated" : "pending");
  status.lastChild.textContent = live ? "live · SSE" : "SSE offline";
}

function renderStatusBar() {
  const running = state.runs.filter(run => run.state === "running").length;
  $("#status-counts").textContent = `${state.runs.length} run${state.runs.length === 1 ? "" : "s"} · ${running} running`;
  $("#status-path").textContent = state.run ? `runs/${state.run.run_id}` : "runs/—";
  $("#status-path").title = $("#status-path").textContent;
}

async function mutate(path, body, message) {
  try {
    const data = await api(path, { method: "POST", body: JSON.stringify(body) });
    toast(message);
    if (data.action_id) watchAction(data.action_id);
    window.setTimeout(() => loadRuns(data.run_id || state.run?.run_id).catch(error => toast(error.message, true)), 300);
    return data;
  } catch (error) {
    toast(error.message, true);
    throw error;
  }
}

function watchAction(actionId) {
  state.action = actionId;
  window.clearInterval(state.actionTimer);
  const poll = async () => {
    try {
      const action = await api(`/api/actions/${actionId}`);
      $("#action-log").textContent = action.log || "Waiting for output…";
      $("#action-log").scrollTop = $("#action-log").scrollHeight;
      if (action.state === "finished") {
        window.clearInterval(state.actionTimer);
        await loadRuns(action.run_id);
      }
    } catch (error) {
      window.clearInterval(state.actionTimer);
      toast(error.message, true);
    }
  };
  poll();
  state.actionTimer = window.setInterval(poll, 500);
}

function openTokenDialog() {
  $("#token-input").value = state.token;
  $("#token-dialog").showModal();
}

function effectiveTheme() {
  return document.documentElement.dataset.theme || (window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light");
}

function updateThemeButton() {
  const dark = effectiveTheme() === "dark";
  $("#theme-button").textContent = dark ? "☀" : "☾";
  $("#theme-button").title = `Switch to ${dark ? "light" : "dark"} theme`;
  $("#theme-button").setAttribute("aria-label", $("#theme-button").title);
}

$("#run-select").addEventListener("change", event => selectRun(event.target.value).catch(error => toast(error.message, true)));
$("#token-button").addEventListener("click", openTokenDialog);
$("#sidebar-token-button").addEventListener("click", openTokenDialog);
$("#token-form").addEventListener("submit", event => {
  if (event.submitter?.value === "cancel") return;
  state.token = $("#token-input").value;
  sessionStorage.setItem("wf-token", state.token);
  toast("Control token saved for this tab");
});
$("#theme-button").addEventListener("click", () => {
  const theme = effectiveTheme() === "dark" ? "light" : "dark";
  document.documentElement.dataset.theme = theme;
  localStorage.setItem("wf-theme", theme);
  updateThemeButton();
  window.requestAnimationFrame(() => drawConnectors(phaseLayout(buildTree(state.run?.expression, state.run?.nodes)).edges));
});
window.matchMedia("(prefers-color-scheme: dark)").addEventListener("change", () => {
  if (!document.documentElement.dataset.theme) updateThemeButton();
});
$("#launch-button").addEventListener("click", () => {
  if (requireToken()) $("#launch-dialog").showModal();
});
$("#launch-form").addEventListener("submit", async event => {
  if (event.submitter?.value === "cancel") return;
  event.preventDefault();
  const form = new FormData(event.target);
  const body = Object.fromEntries(form.entries());
  body.max_workers = Number(body.max_workers);
  if (!body.run_id) delete body.run_id;
  if (!body.run_branch) delete body.run_branch;
  try {
    const data = await mutate("/api/runs", body, "Run launched");
    $("#launch-dialog").close();
    await loadRuns(data.run_id);
  } catch {
    $("#launch-submit").disabled = false;
  }
});
$("#resume-button").addEventListener("click", () => {
  if (state.run && requireToken()) mutate(`/api/runs/${encodeURIComponent(state.run.run_id)}/resume`, {}, "Resume launched").catch(() => {});
});
$("#kill-button").addEventListener("click", () => {
  if (state.run && requireToken() && window.confirm("Kill the active expression? Durable journal state will be preserved for resume.")) {
    mutate(`/api/runs/${encodeURIComponent(state.run.run_id)}/kill`, {}, "Expression killed").catch(() => {});
  }
});
document.querySelectorAll(".tab").forEach(tab => tab.addEventListener("click", () => {
  document.querySelectorAll(".tab").forEach(item => item.classList.toggle("active", item === tab));
  $("#journal-view").classList.toggle("hidden", tab.dataset.stream !== "journal");
  $("#action-log").classList.toggle("hidden", tab.dataset.stream !== "action");
}));
window.addEventListener("resize", () => {
  if (state.run?.expression) drawConnectors(phaseLayout(buildTree(state.run.expression, state.run.nodes)).edges);
});
window.addEventListener("beforeunload", closeStream);

updateThemeButton();
loadRuns().catch(error => {
  toast(error.message, true);
  render();
});
