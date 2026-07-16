"use strict";

const $ = selector => document.querySelector(selector);
const el = (tag, className, text) => {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
};
const FRAME_COLUMN_MIN = 280;
const CANVAS_PADDING = 18;
const MIN_CANVAS_ZOOM = 0.01;
const MAX_CANVAS_ZOOM = 2;
const CANVAS_ZOOM_STEP = 0.1;
let canvasLayoutFrame = null;
let canvasPan = null;

const state = {
  runs: [],
  repositories: [],
  run: null,
  source: null,
  canvasRoot: null,
  canvasZoom: 1,
  resetCanvasView: true,
  expandedFrames: new Set(),
  expandedCalls: new Set(),
  token: sessionStorage.getItem("wf-token") || "",
  refreshTimer: null,
};

function toast(message, bad = false) {
  const target = $("#toast");
  target.textContent = message;
  target.classList.toggle("bad", bad);
  target.classList.add("show");
  window.setTimeout(() => target.classList.remove("show"), 3000);
}

async function api(path, options = {}) {
  const headers = { ...(options.headers || {}) };
  if (options.body) headers["Content-Type"] = "application/json";
  const response = await fetch(path, { ...options, headers });
  if (!response.ok) {
    let detail = `${response.status} ${response.statusText}`;
    try { detail = (await response.json()).detail || detail; } catch { /* response is not JSON */ }
    throw new Error(detail);
  }
  return response.json();
}

function dot(tone) {
  return el("i", `status-dot ${tone || "muted"}`);
}

function short(value, size = 8) {
  const text = String(value || "—");
  return text.length > size ? text.slice(0, size) : text;
}

function firstLine(value, fallback = "—") {
  const line = String(value || "").split("\n").find(item => item.trim());
  return line ? line.trim() : fallback;
}

function framePath(frameId) {
  return String(frameId).split(".").join(" › ");
}

function expandableCopy(className, value, title = "Expand text") {
  const control = el("button", className);
  control.type = "button";
  control.setAttribute("aria-expanded", "false");
  const copy = el("span", "clamped", value);
  control.append(copy);
  control.title = title;
  control.addEventListener("click", () => {
    copy.classList.toggle("expanded");
    control.setAttribute("aria-expanded", String(copy.classList.contains("expanded")));
  });
  return control;
}

function failedChildrenChip(frame, reserve = false) {
  const count = Number(frame.failed_children || 0);
  const chip = el("span", `failed-children-chip${count ? "" : " empty"}`);
  if (count) chip.textContent = `${count} failed ${count === 1 ? "child" : "children"}`;
  if (!count && !reserve) return null;
  return chip;
}

function formatDuration(seconds) {
  const value = Number(seconds);
  if (!Number.isFinite(value) || value < 0) return "—";
  if (value < 1) return `${Math.round(value * 1000)}ms`;
  if (value < 60) return `${value.toFixed(value < 10 ? 1 : 0)}s`;
  const minutes = Math.floor(value / 60);
  if (minutes < 60) return `${minutes}m ${Math.floor(value % 60)}s`;
  return `${Math.floor(minutes / 60)}h ${minutes % 60}m`;
}

function currentDuration(started, ended = null) {
  const start = Number(started);
  if (!Number.isFinite(start)) return null;
  const finish = ended == null ? Date.now() / 1000 : Number(ended);
  return Number.isFinite(finish) ? Math.max(0, finish - start) : null;
}

function runTone(value) {
  if (value === "completed" || value === "done") return "success";
  if (value === "failed" || value === "invalid") return "failed";
  if (value === "parked") return "parked";
  if (value === "interrupted") return "interrupted";
  if (value === "running" || value === "banked") return value;
  return "muted";
}

function parseKey(key) {
  const separator = key.indexOf(":");
  return [key.slice(0, separator), key.slice(separator + 1)];
}

function detailUrl(run = state.run) {
  return `/api/repos/${encodeURIComponent(run.repo.id)}/runs/${encodeURIComponent(run.run_id)}`;
}

function updateQuery(run, frameId = null) {
  const params = new URLSearchParams(location.search);
  params.set("repo", run.repo.name);
  params.set("run", run.run_short);
  if (frameId && frameId !== run.root_frame_id) params.set("frame", frameId);
  else params.delete("frame");
  history.replaceState(null, "", `${location.pathname}?${params}${location.hash}`);
}

async function loadRuns() {
  const payload = await api("/api/runs");
  state.runs = payload.runs || [];
  state.repositories = payload.repositories || [];
  renderRunPicker();
  renderLiveRuns();
  const params = new URLSearchParams(location.search);
  const preferredRepo = params.get("repo");
  const preferredRun = params.get("run");
  let selected = state.runs.find(run => run.state !== "invalid" &&
    (!preferredRepo || run.repo_id === preferredRepo || run.repo_name === preferredRepo || run.repository.endsWith(`/${preferredRepo}`)) &&
    (!preferredRun || run.run_id === preferredRun || run.run_id.startsWith(preferredRun)));
  if (!selected) selected = state.runs.find(run => run.state !== "invalid");
  if (!selected) {
    state.run = null;
    render();
    return;
  }
  await selectRun(selected.key, false);
}

function renderRunPicker() {
  const select = $("#run-picker");
  select.replaceChildren();
  if (!state.runs.length) {
    select.append(new Option("No watched runs", ""));
    return;
  }
  for (const run of state.runs) {
    const option = new Option(`${run.repo_name} / ${run.run_short} · ${run.state}`, run.key);
    option.disabled = run.state === "invalid";
    select.append(option);
  }
}

function renderLiveRuns() {
  const target = $("#live-list");
  target.replaceChildren();
  const ordered = [...state.runs].sort((left, right) => {
    const rank = value => value === "running" ? 4 : value === "parked" ? 3 : value === "interrupted" ? 2 : value === "completed" ? 1 : 0;
    return rank(right.state) - rank(left.state) || Number(right.started_at || 0) - Number(left.started_at || 0);
  });
  if (!ordered.length) target.append(el("p", "empty-copy", "No runs in watched repositories."));
  for (const run of ordered.slice(0, 8)) {
    const button = el("button", `live-run${state.run?.key === run.key ? " selected" : ""}`);
    button.type = "button";
    button.disabled = run.state === "invalid";
    button.title = `${run.repository} / ${run.run_id}${run.error ? `\n${run.error}` : ""}`;
    const copy = el("span", "live-run-copy");
    copy.append(el("span", "live-run-id", `${run.repo_name} / ${run.run_short}`), el("span", "live-run-meta", `${run.state} · ${run.frames} frames · ${run.event_count} events`));
    button.append(dot(runTone(run.state)), copy);
    if (!button.disabled) button.addEventListener("click", () => selectRun(run.key).catch(error => toast(error.message, true)));
    target.append(button);
  }
  $("#repo-count").textContent = `${state.repositories.length} repo${state.repositories.length === 1 ? "" : "s"}`;
  $("#run-count").textContent = `${state.runs.length} run${state.runs.length === 1 ? "" : "s"}`;
}

function closeStream() {
  state.source?.close();
  state.source = null;
  setConnection(false);
}

async function selectRun(key, updateUrl = true) {
  if (!key) return;
  closeStream();
  const [repoId, runId] = parseKey(key);
  const run = await api(`/api/repos/${encodeURIComponent(repoId)}/runs/${encodeURIComponent(runId)}`);
  state.run = run;
  state.expandedFrames.clear();
  state.expandedCalls.clear();
  state.canvasZoom = 1;
  state.resetCanvasView = true;
  const requestedRoot = new URLSearchParams(location.search).get("frame");
  state.canvasRoot = requestedRoot && run.frames[requestedRoot] ? requestedRoot : run.root_frame_id;
  $("#run-picker").value = key;
  if (updateUrl) updateQuery(run, state.canvasRoot);
  render();
  openStream();
}

function openStream() {
  if (!state.run) return;
  const last = state.run.events.at(-1)?.seq ?? -1;
  const source = new EventSource(`${detailUrl()}/events?after=${last}`);
  state.source = source;
  source.onopen = () => { if (state.source === source) setConnection(true); };
  source.onerror = () => { if (state.source === source) setConnection(false); };
  source.addEventListener("journal", () => scheduleRefresh());
}

function scheduleRefresh() {
  window.clearTimeout(state.refreshTimer);
  state.refreshTimer = window.setTimeout(async () => {
    if (!state.run) return;
    try {
      const fresh = await api(detailUrl());
      if (state.run?.key === fresh.key) {
        state.run = fresh;
        render();
      }
      const listing = await api("/api/runs");
      state.runs = listing.runs || [];
      state.repositories = listing.repositories || [];
      renderRunPicker();
      renderLiveRuns();
    } catch (error) { toast(error.message, true); }
  }, 80);
}

function setConnection(live) {
  $("#connection-dot").className = `status-dot ${live ? "success" : "muted"}`;
  $("#connection-copy").textContent = live ? "live" : "reconnecting";
  const stream = $("#stream-state");
  stream.replaceChildren(dot(live ? "success" : "muted"), document.createTextNode(live ? "live · SSE" : "SSE offline"));
}

function render() {
  renderHeader();
  renderLiveRuns();
  renderCanvas();
  renderJournal();
  renderStatus();
}

function renderHeader() {
  const run = state.run;
  if (!run) {
    $("#run-id").textContent = "no run";
    $("#state-copy").textContent = "No run selected";
    $("#cap-list").replaceChildren();
    return;
  }
  $("#run-id").textContent = `${run.repo.name} / ${run.run_short}`;
  $("#run-id").title = `${run.repo.path} / ${run.run_id}`;
  document.title = `WILDFLOWS — ${run.repo.name} / ${run.run_short}`;
  const badge = $("#run-state");
  badge.className = `state-pill ${run.state}`;
  badge.replaceChildren(dot(runTone(run.state)), document.createTextNode(run.state));
  updateLiveClocks();
  const policy = run.policy || {};
  const caps = [
    ["depth", policy.max_depth],
    ["breadth", policy.max_breadth],
    ["frames", policy.max_subtree_frames],
    ["spend", policy.max_subtree_spend],
    ["subtree", formatDuration(policy.subtree_timeout_s)],
  ];
  const target = $("#cap-list");
  target.replaceChildren();
  for (const [label, value] of caps) {
    const chip = el("span", "cap-chip");
    chip.append(document.createTextNode(`${label} `), el("b", "", String(value ?? "open")));
    target.append(chip);
  }
  if (run.artifacts.length) {
    const chip = el("a", "cap-chip", `${run.artifacts.length} artifact${run.artifacts.length === 1 ? "" : "s"} ↗`);
    chip.id = "artifacts";
    chip.href = run.artifacts[0].url;
    chip.target = "_blank";
    chip.rel = "noopener";
    target.append(chip);
  }
}

function updateLiveClocks() {
  const run = state.run;
  if (!run) return;
  const elapsed = currentDuration(run.started_at, run.active ? null : run.ended_at);
  $("#state-copy").textContent = `${run.state_line} · ${formatDuration(elapsed)}`;
  document.querySelectorAll("[data-started]").forEach(target => {
    const finish = target.dataset.ended || null;
    target.textContent = formatDuration(currentDuration(target.dataset.started, finish));
  });
  $("#status-clock").textContent = run.active ? `live · ${formatDuration(elapsed)}` : `final · ${formatDuration(elapsed)}`;
}

function parentCall(frame) {
  if (!frame?.parent_frame_id) return null;
  const parent = state.run.frames[frame.parent_frame_id];
  return parent?.calls.find(call => call.call_index === frame.parent_call_index) || null;
}

function fullyDone(frame) {
  return frame.state === "done" && frame.calls.every(call => call.status === "completed");
}

function defaultCollapsed(frame) {
  const nested = frame.frame_id !== state.run.root_frame_id;
  if (nested && fullyDone(frame)) return true;
  if (!["done", "failed"].includes(frame.state)) return false;
  const call = parentCall(frame);
  return call?.status === "completed" || (!nested && !state.run.active);
}

function canvasSurface() {
  return $("#canvas .canvas-surface");
}

function clampCanvasZoom(value) {
  return Math.min(MAX_CANVAS_ZOOM, Math.max(MIN_CANVAS_ZOOM, value));
}

function updateZoomControls() {
  const enabled = Boolean(state.run && canvasSurface());
  $("#zoom-value").textContent = `${Math.round(state.canvasZoom * 100)}%`;
  $("#zoom-out").disabled = !enabled || state.canvasZoom <= MIN_CANVAS_ZOOM;
  $("#zoom-in").disabled = !enabled || state.canvasZoom >= MAX_CANVAS_ZOOM;
  $("#zoom-fit").disabled = !enabled;
}

function updateCanvasGeometry() {
  const viewport = $("#canvas");
  const surface = canvasSurface();
  const space = viewport.querySelector(".canvas-space");
  if (!surface || !space) {
    updateZoomControls();
    return;
  }
  surface.style.setProperty("--canvas-zoom", String(state.canvasZoom));
  const naturalWidth = Math.max(surface.offsetWidth, surface.scrollWidth);
  const naturalHeight = Math.max(surface.offsetHeight, surface.scrollHeight);
  space.style.width = `${Math.max(viewport.clientWidth, Math.ceil(naturalWidth * state.canvasZoom + CANVAS_PADDING * 2))}px`;
  space.style.height = `${Math.max(viewport.clientHeight, Math.ceil(naturalHeight * state.canvasZoom + CANVAS_PADDING * 2))}px`;
  updateZoomControls();
}

function scheduleCanvasGeometry() {
  if (canvasLayoutFrame !== null) window.cancelAnimationFrame(canvasLayoutFrame);
  canvasLayoutFrame = window.requestAnimationFrame(() => {
    canvasLayoutFrame = null;
    updateCanvasGeometry();
  });
}

function setCanvasZoom(value, anchor = null, animate = true) {
  const viewport = $("#canvas");
  const surface = canvasSurface();
  if (!surface) return;
  const oldZoom = state.canvasZoom;
  const nextZoom = clampCanvasZoom(value);
  if (Math.abs(nextZoom - oldZoom) < 0.0001) return;
  const rect = viewport.getBoundingClientRect();
  const anchorX = anchor ? anchor.clientX - rect.left : viewport.clientWidth / 2;
  const anchorY = anchor ? anchor.clientY - rect.top : viewport.clientHeight / 2;
  const naturalX = (viewport.scrollLeft + anchorX - CANVAS_PADDING) / oldZoom;
  const naturalY = (viewport.scrollTop + anchorY - CANVAS_PADDING) / oldZoom;
  if (animate) {
    surface.classList.remove("zoom-animate");
    void surface.offsetWidth;
    surface.classList.add("zoom-animate");
  }
  state.canvasZoom = nextZoom;
  updateCanvasGeometry();
  viewport.scrollLeft = CANVAS_PADDING + naturalX * nextZoom - anchorX;
  viewport.scrollTop = CANVAS_PADDING + naturalY * nextZoom - anchorY;
  if (animate) window.setTimeout(() => surface.classList.remove("zoom-animate"), 180);
}

function fitCanvasToWidth() {
  const viewport = $("#canvas");
  const surface = canvasSurface();
  if (!surface) return;
  const naturalWidth = Math.max(surface.offsetWidth, surface.scrollWidth);
  if (!naturalWidth) return;
  const fitted = Math.min(1, (viewport.clientWidth - CANVAS_PADDING * 2) / naturalWidth);
  setCanvasZoom(fitted, {
    clientX: viewport.getBoundingClientRect().left + CANVAS_PADDING,
    clientY: viewport.getBoundingClientRect().top + CANVAS_PADDING,
  });
  viewport.scrollLeft = 0;
}

function renderCanvas() {
  const target = $("#canvas");
  const previousLeft = target.scrollLeft;
  const previousTop = target.scrollTop;
  const resetView = state.resetCanvasView;
  state.resetCanvasView = false;
  target.replaceChildren();
  if (!state.run || !state.run.frames[state.canvasRoot]) {
    target.append(el("div", "empty-state", "Select a run to inspect its call stack."));
    updateZoomControls();
    return;
  }
  $("#frame-count").textContent = `${state.run.frame_order.length} frame${state.run.frame_order.length === 1 ? "" : "s"}`;
  renderBreadcrumb();
  const space = el("div", "canvas-space");
  const surface = el("div", "canvas-surface");
  surface.style.setProperty("--canvas-zoom", String(state.canvasZoom));
  surface.style.setProperty("--frame-column-min", `${FRAME_COLUMN_MIN}px`);
  const root = el("div", "stack-root");
  root.append(renderFrame(state.canvasRoot, 0));
  surface.append(root);
  space.append(surface);
  target.append(space);
  updateCanvasGeometry();
  target.scrollTo(resetView ? 0 : previousLeft, resetView ? 0 : previousTop);
}

function ancestors(frameId) {
  const values = [];
  let current = state.run.frames[frameId];
  while (current) {
    values.unshift(current);
    current = current.parent_frame_id ? state.run.frames[current.parent_frame_id] : null;
  }
  return values;
}

function renderBreadcrumb() {
  const target = $("#canvas-breadcrumb");
  target.replaceChildren();
  const chain = ancestors(state.canvasRoot);
  chain.forEach((frame, index) => {
    if (index) target.append(document.createTextNode("›"));
    const button = el("button", `crumb${index === chain.length - 1 ? " current" : ""}`, frame.frame_id);
    button.type = "button";
    button.addEventListener("click", () => rebase(frame.frame_id));
    target.append(button);
  });
}

function rebase(frameId) {
  state.canvasRoot = frameId;
  state.resetCanvasView = true;
  updateQuery(state.run, frameId);
  renderCanvas();
}

function renderFrame(frameId, relativeDepth) {
  const frame = state.run.frames[frameId];
  const collapsed = defaultCollapsed(frame) && !state.expandedFrames.has(frameId);
  const node = el("section", `frame-node frame-card ${frame.state}${collapsed ? " collapsed-container" : ""}`);
  node.dataset.frame = frameId;
  node.append(collapsed ? renderCollapsedFrame(frame) : renderFullFrame(frame));
  if (collapsed) return node;
  if (relativeDepth >= 3 && frame.calls.length) {
    const drill = el("button", "drill-button", `drill in · rebase on ${frame.path}`);
    drill.type = "button";
    drill.addEventListener("click", () => rebase(frame.frame_id));
    node.append(drill);
    return node;
  }
  if (frame.calls.length) {
    const calls = el("div", "frame-calls");
    for (const call of frame.calls) calls.append(renderCall(frame, call, relativeDepth));
    node.append(calls);
  }
  return node;
}

function timed(target, frame) {
  target.classList.add("frame-duration");
  if (frame.started_at != null) target.dataset.started = frame.started_at;
  if (frame.ended_at != null) target.dataset.ended = frame.ended_at;
  target.textContent = formatDuration(currentDuration(frame.started_at, frame.ended_at));
  return target;
}

function renderCollapsedFrame(frame) {
  const card = el("button", "frame-summary collapsed");
  card.type = "button";
  card.title = `Expand ${frame.path}`;
  const result = frame.state === "failed" ? frame.reason : firstLine(frame.text, "completed");
  card.append(dot(frame.state), el("span", "frame-name", frame.name), el("span", "rig-chip", frame.rig), timed(el("span"), frame), failedChildrenChip(frame, true), el("span", `collapsed-result${frame.state === "failed" ? " failure-reason" : ""}`, result));
  card.addEventListener("click", () => { state.expandedFrames.add(frame.frame_id); renderCanvas(); });
  return card;
}

function renderFullFrame(frame) {
  const card = el("div", "frame-summary");
  const collapsible = defaultCollapsed(frame) && state.expandedFrames.has(frame.frame_id);
  const top = el(collapsible ? "button" : "div", `frame-top${collapsible ? " frame-toggle" : ""}`);
  if (collapsible) top.type = "button";
  top.append(dot(frame.state), el("span", "frame-path", frame.path), el("span", "rig-chip", frame.rig));
  const pendingDispatch = frame.calls.find(call => call.tool === "dispatch" && call.status === "pending");
  let label = frame.state;
  if (frame.state === "banked" && pendingDispatch) label = `banked · waiting on call ${pendingDispatch.call_index}`;
  top.append(el("span", `state-chip ${frame.state}`, label));
  const failedChip = failedChildrenChip(frame);
  if (failedChip) top.append(failedChip);
  top.append(timed(el("span"), frame));
  card.append(top, el("p", "frame-prompt", frame.prompt));
  const meta = el("div", "frame-meta");
  meta.append(el("span", "", `depth ${frame.depth}`), el("span", "", `attempt branch ${short(frame.branch, 18)}`));
  if (frame.skills.length) meta.append(el("span", "", `skills · ${frame.skills.join(", ")}`));
  card.append(meta);
  if (frame.text || frame.reason) {
    card.append(expandableCopy(
      `frame-result${frame.state === "failed" ? " failure-reason" : ""}`,
      frame.state === "failed" ? frame.reason : frame.text,
      "Expand result text",
    ));
  }
  if (collapsible) {
    top.title = "Collapse the completed frame";
    top.addEventListener("click", () => { state.expandedFrames.delete(frame.frame_id); renderCanvas(); });
  }
  return card;
}

function renderCall(frame, call, relativeDepth) {
  if (call.tool === "gate") return renderGate(frame, call);
  if (call.tool === "ask") return renderAsk(frame, call);
  const block = el("section", "call-block");
  const heading = el("div", "call-heading");
  heading.append(el("span", "call-chip", `call ${call.call_index} · dispatch`));
  const shapeLabel = call.parallel ? `parallel fan-out × ${call.requested}` : `serial dispatch × ${call.requested}`;
  const kinds = [...new Set(call.kinds || [])];
  const taskLabel = kinds.length ? `${shapeLabel} · ${kinds.join(" / ")}` : shapeLabel;
  heading.append(el("span", "call-command", taskLabel), el("span", "call-counts", callSummary(call)));
  block.append(heading);
  if (call.result?.outcome === "refused") {
    block.append(expandableCopy(
      "call-error failure-reason",
      call.result.message || "dispatch refused",
      "Expand dispatch error",
    ));
    return block;
  }
  const callKey = `${frame.frame_id}:${call.call_index}`;
  const expanded = state.expandedCalls.has(callKey);
  const all = dispatchSlots(call);
  const visible = expanded || all.length <= 5 ? all : all.slice(0, 5);
  const children = el("div", `call-children${call.parallel ? "" : " serial"}${expanded ? " full-grid" : ""}`);
  children.dataset.slots = String(all.length);
  for (const slot of visible) {
    children.append(slot.frameId ? renderFrame(slot.frameId, relativeDepth + 1) : renderQueued(slot));
  }
  if (!expanded && all.length > 5) {
    const ghost = el("button", "ghost-card", `× ${call.requested}\n${callSummary(call)}\nshow full grid`);
    ghost.type = "button";
    ghost.addEventListener("click", () => { state.expandedCalls.add(callKey); renderCanvas(); });
    children.append(ghost);
  } else if (expanded && all.length > 5) {
    const ghost = el("button", "ghost-card", "collapse grid");
    ghost.type = "button";
    ghost.addEventListener("click", () => { state.expandedCalls.delete(callKey); renderCanvas(); });
    children.append(ghost);
  }
  block.append(children);
  return block;
}

function dispatchSlots(call) {
  const byIndex = new Map(call.children.map(frameId => [state.run.frames[frameId].task_index, frameId]));
  const tasks = call.request?.tasks || [];
  const kinds = call.kinds || [];
  const futureFrameIds = call.future_frame_ids || [];
  return Array.from({ length: call.requested }, (_, index) => ({
    index,
    frameId: byIndex.get(index) || null,
    futureFrameId: futureFrameIds[index],
    task: tasks[index] || `task ${index}`,
    kind: kinds[index] || null,
  }));
}

function renderQueued(slot) {
  const card = el("article", "frame-node frame-card queued");
  card.dataset.frame = slot.futureFrameId;
  const summary = el("div", "frame-summary");
  const top = el("div", "frame-top");
  top.append(dot("queued"), el("span", "frame-path", framePath(slot.futureFrameId)));
  if (slot.kind) top.append(el("span", "kind-badge", slot.kind));
  top.append(el("span", "state-chip", "queued"));
  summary.append(top, el("p", "frame-prompt", slot.task));
  card.append(summary);
  return card;
}

function callSummary(call) {
  const counts = call.counts || {};
  const bits = [];
  if (counts.done) bits.push(`${counts.done} done`);
  if (counts.running) bits.push(`${counts.running} running`);
  if (counts.banked) bits.push(`${counts.banked} banked`);
  if (counts.parked) bits.push(`${counts.parked} parked`);
  if (counts.failed) bits.push(`${counts.failed} failed`);
  if (call.queued) bits.push(`${call.queued} queued`);
  return bits.join(" · ") || call.status;
}

function renderGate(frame, call) {
  const row = el("section", "gate-row");
  const result = call.result;
  const failed = result && result.exit_code !== 0;
  const language = call.gate_language || "gate: RUNNING";
  const main = el("button", "gate-main");
  main.type = "button";
  main.disabled = !result;
  main.setAttribute("aria-expanded", "false");
  main.append(dot(result ? (failed ? "failed" : "success") : "running"), el("span", `gate-language${failed ? " fail" : ""}`, language), el("code", "call-command", call.request.cmd), el("span", "frame-duration", formatDuration(currentDuration(call.started_at, call.ended_at))));
  row.append(main);
  if (result) {
    const streams = el("div", "gate-streams");
    streams.append(streamBox("stdout", result.stdout), streamBox("stderr", result.stderr));
    row.append(streams);
    main.title = "Expand captured stdout and stderr";
    main.addEventListener("click", () => {
      row.classList.toggle("expanded");
      main.setAttribute("aria-expanded", String(row.classList.contains("expanded")));
    });
  }
  return row;
}

function streamBox(label, value) {
  const box = el("pre", "stream-box");
  box.append(el("b", "", label), document.createTextNode(value || "(empty)"));
  return box;
}

function renderAsk(frame, call) {
  const card = el("section", "ask-card");
  card.append(
    el("span", "micro-label", call.status === "pending" ? "Owner input required" : "Owner answered"),
    expandableCopy("ask-question", call.request.question, "Expand owner question"),
  );
  if (call.status === "completed") {
    card.append(expandableCopy("frame-result", call.result?.answer || "answered", "Expand owner answer"));
    return card;
  }
  if (!state.run.controls.answer) {
    card.append(el("span", "read-only-note", "Read-only dashboard · answer through the CLI/control seam."));
    return card;
  }
  const form = el("form", "answer-form");
  const input = el("input");
  input.required = true;
  input.placeholder = "Answer this parked frame…";
  const submit = el("button", "button primary", "Answer");
  submit.type = "submit";
  form.append(input, submit);
  form.addEventListener("submit", async event => {
    event.preventDefault();
    if (!state.token) { $("#token-dialog").showModal(); return; }
    submit.disabled = true;
    try {
      await api(`${detailUrl()}/answer`, { method: "POST", headers: { "X-Wildflows-Token": state.token }, body: JSON.stringify({ answer: input.value, frame_id: frame.frame_id, call_index: call.call_index }) });
      toast("Answer delivered to the resident run");
      scheduleRefresh();
    } catch (error) { toast(error.message, true); }
    finally { submit.disabled = false; }
  });
  card.append(form);
  return card;
}

function renderJournal() {
  const target = $("#journal");
  target.replaceChildren();
  const events = state.run?.events || [];
  $("#event-count").textContent = `${events.length} event${events.length === 1 ? "" : "s"}`;
  if (!events.length) {
    target.append(el("div", "empty-state", "Journal events appear here."));
    return;
  }
  for (const event of events) target.append(renderEvent(event));
  target.scrollTop = target.scrollHeight;
}

function eventFrame(event) {
  return event.frame_id ? event.frame_id.split(".").join(" › ") : event.root_frame_id || "run";
}

function eventCall(frameId, callIndex) {
  const frame = state.run.frames[frameId];
  return frame?.calls.find(call => call.call_index === callIndex) || null;
}

function renderEvent(event) {
  const info = eventInfo(event);
  const row = el("article", `journal-row${info.expandable ? " expandable" : ""}`);
  row.dataset.seq = event.seq;
  const date = new Date(Number(event.ts) * 1000);
  const timestamp = Number.isNaN(date.getTime()) ? "—" : date.toLocaleTimeString([], { hour12: false, hour: "2-digit", minute: "2-digit", second: "2-digit" });
  row.append(el("time", "journal-time", timestamp), el("span", `kind-badge ${info.tone}`, info.kind), el("span", "journal-frame", eventFrame(event)), el("span", "journal-detail clamped", info.detail), el("span", "journal-ref", info.ref));
  if (info.streams) {
    const streams = el("div", "journal-streams");
    streams.append(streamBox("stdout", info.streams.stdout), streamBox("stderr", info.streams.stderr));
    row.append(streams);
  }
  if (info.expandable) row.addEventListener("click", () => {
    row.classList.toggle("expanded");
    row.querySelector(".journal-detail").classList.toggle("expanded");
  });
  return row;
}

function eventInfo(event) {
  const ref = event.frame_id != null && event.call_index != null ? `${eventFrame(event)} / call ${event.call_index}` : `seq / ${event.seq}`;
  if (event.kind === "run_opened") return { kind: "run open", tone: "owner", detail: firstLine(event.root_prompt), ref: event.run_branch, expandable: String(event.root_prompt).length > 120 };
  if (event.kind === "run_finished") return { kind: "run result", tone: event.outcome === "ok" ? "result" : "failure", detail: event.text || event.outcome, ref: `result / ${short(event.root_head)}`, expandable: String(event.text).length > 120 };
  if (event.kind === "run_interrupted") return { kind: "interrupted", tone: "owner", detail: event.reason, ref: `seq / ${event.seq}`, expandable: String(event.reason).length > 120 };
  if (event.kind === "frame_pushed") return { kind: "frame push", tone: "frame", detail: `${event.rig} started · ${firstLine(event.prompt)}`, ref: `attempt ${event.attempt}`, expandable: String(event.prompt).length > 120 };
  if (event.kind === "frame_slot_queued") return { kind: "slot queue", tone: "owner", detail: `${event.rig} waiting for an active slot`, ref: `attempt ${event.attempt}` };
  if (event.kind === "frame_slot_acquired") return { kind: "slot acquired", tone: "frame", detail: `${event.rig}${event.slot == null ? " active" : ` lane ${event.slot}`}`, ref: `attempt ${event.attempt}` };
  if (event.kind === "frame_slot_released") return { kind: "slot released", tone: "", detail: `${event.reason} · ${formatDuration(event.active_s)} self-time`, ref: `attempt ${event.attempt}` };
  if (event.kind === "frame_commit_warning") return { kind: "commit warning", tone: "owner", detail: event.message, ref: `attempt ${event.attempt}`, expandable: String(event.message).length > 100 };
  if (event.kind === "frame_exited") {
    const frame = state.run.frames[event.frame_id];
    return { kind: "frame result", tone: event.outcome === "ok" ? "result" : "failure", detail: event.text || `${event.outcome}: ${firstLine(event.stderr)}`, ref: `${event.outcome} / ${formatDuration(frame?.duration_s)}`, expandable: String(event.text || event.stderr).length > 100 };
  }
  if (event.kind === "frame_integrating") return { kind: "integrating", tone: "frame", detail: `${event.landed_commits?.length || 0} landed commit receipts`, ref: `candidate / ${short(event.candidate_head)}` };
  if (event.kind === "frame_integrated") return { kind: "integrated", tone: "result", detail: `${event.landed_commits?.length || 0} commits integrated`, ref: `result / ${short(event.candidate_head)}` };
  if (event.kind === "frame_popped") return { kind: "frame pop", tone: event.outcome === "ok" ? "result" : "failure", detail: `frame unwound · ${event.outcome}`, ref: `attempt ${event.attempt}` };
  if (event.kind === "dispatch_called") {
    const kinds = event.request.kinds?.length ? ` · ${event.request.kinds.join(" / ")}` : "";
    return { kind: "dispatch", tone: "", detail: `${event.request.parallel ? "parallel" : "serial"} × ${event.request.tasks.length}${kinds} · ${firstLine(event.request.tasks[0])}`, ref: `${ref} / request`, expandable: event.request.tasks.join(" ").length > 100 };
  }
  if (event.kind === "dispatch_returned") return { kind: "result", tone: event.result.outcome === "ok" ? "result" : "failure", detail: dispatchResultText(event.result), ref: `${ref} / result`, expandable: dispatchResultText(event.result).length > 100 };
  if (event.kind === "gate_called") return { kind: "gate", tone: "", detail: event.request.cmd, ref: `${ref} / request`, expandable: String(event.request.cmd).length > 100 };
  if (event.kind === "gate_returned") {
    const pass = event.result.exit_code === 0;
    return { kind: "gate result", tone: pass ? "result" : "failure", detail: `gate: ${pass ? "PASS" : "FAIL"} (exit ${event.result.exit_code})`, ref: `${ref} / result`, expandable: true, streams: event.result };
  }
  if (event.kind === "asked") return { kind: "ask", tone: "owner", detail: event.request.question, ref: `${ref} / request`, expandable: String(event.request.question).length > 100 };
  if (event.kind === "answered") return { kind: "answer", tone: "owner", detail: event.answer, ref: `${ref} / result`, expandable: String(event.answer).length > 100 };
  if (event.kind === "call_refused") return { kind: "call refused", tone: "failure", detail: event.reason, ref, expandable: String(event.reason).length > 100 };
  return { kind: event.kind, tone: "", detail: event.kind, ref: `seq / ${event.seq}` };
}

function dispatchResultText(result) {
  if (result.message) return result.message;
  return (result.children || []).map(child => `${child.frame_id}: ${child.outcome} · ${firstLine(child.text)}`).join(" · ") || result.outcome;
}

function renderStatus() {
  if (!state.run) return;
  const live = state.runs.filter(run => run.state === "running" || run.state === "parked").length;
  $("#status-summary").textContent = `${state.runs.length} runs · ${live} live`;
  $("#status-path").textContent = `${state.run.repo.path}/.wildflows/runs/${state.run.run_id}`;
  $("#status-path").title = $("#status-path").textContent;
  updateLiveClocks();
}

function effectiveTheme() {
  return document.documentElement.dataset.theme || (matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light");
}
function updateThemeButton() {
  const dark = effectiveTheme() === "dark";
  $("#theme-toggle").textContent = dark ? "☀" : "☾";
  $("#theme-toggle").title = `Switch to ${dark ? "light" : "dark"} theme`;
}
function applyQueryTheme() {
  const theme = new URLSearchParams(location.search).get("theme");
  if (theme === "light" || theme === "dark") document.documentElement.dataset.theme = theme;
}

$("#run-picker").addEventListener("change", event => selectRun(event.target.value).catch(error => toast(error.message, true)));
$("#theme-toggle").addEventListener("click", () => {
  const theme = effectiveTheme() === "dark" ? "light" : "dark";
  document.documentElement.dataset.theme = theme;
  localStorage.setItem("wf-theme", theme);
  const params = new URLSearchParams(location.search);
  params.set("theme", theme);
  history.replaceState(null, "", `${location.pathname}?${params}${location.hash}`);
  updateThemeButton();
});
$("#token-open").addEventListener("click", () => {
  $("#token-input").value = state.token;
  $("#token-dialog").showModal();
});
$("#token-form").addEventListener("submit", event => {
  if (event.submitter?.value === "cancel") return;
  state.token = $("#token-input").value;
  sessionStorage.setItem("wf-token", state.token);
  toast("Control token saved for this tab");
});
const canvas = $("#canvas");
canvas.addEventListener("wheel", event => {
  if (!event.ctrlKey && !event.metaKey) return;
  event.preventDefault();
  setCanvasZoom(state.canvasZoom * Math.exp(-event.deltaY * 0.002), event);
}, { passive: false });
canvas.addEventListener("pointerdown", event => {
  const space = canvas.querySelector(".canvas-space");
  if (event.button !== 0 || (event.target !== canvas && event.target !== space)) return;
  canvasPan = {
    pointerId: event.pointerId,
    x: event.clientX,
    y: event.clientY,
    left: canvas.scrollLeft,
    top: canvas.scrollTop,
  };
  canvas.setPointerCapture(event.pointerId);
  canvas.classList.add("panning");
  event.preventDefault();
});
canvas.addEventListener("pointermove", event => {
  if (!canvasPan || canvasPan.pointerId !== event.pointerId) return;
  canvas.scrollLeft = canvasPan.left - (event.clientX - canvasPan.x);
  canvas.scrollTop = canvasPan.top - (event.clientY - canvasPan.y);
});
function endCanvasPan(event) {
  if (!canvasPan || canvasPan.pointerId !== event.pointerId) return;
  canvasPan = null;
  canvas.classList.remove("panning");
}
canvas.addEventListener("pointerup", endCanvasPan);
canvas.addEventListener("pointercancel", endCanvasPan);
canvas.addEventListener("lostpointercapture", endCanvasPan);
$("#zoom-out").addEventListener("click", () => setCanvasZoom(state.canvasZoom - CANVAS_ZOOM_STEP));
$("#zoom-in").addEventListener("click", () => setCanvasZoom(state.canvasZoom + CANVAS_ZOOM_STEP));
$("#zoom-fit").addEventListener("click", fitCanvasToWidth);
window.addEventListener("beforeunload", closeStream);
window.addEventListener("resize", scheduleCanvasGeometry);
window.setInterval(updateLiveClocks, 1000);
applyQueryTheme();
updateThemeButton();
loadRuns().catch(error => { toast(error.message, true); render(); });
