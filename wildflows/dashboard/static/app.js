import { buildTree } from "./tree.js";

const $ = selector => document.querySelector(selector);
const state = { runs: [], run: null, source: null, selected: null, token: sessionStorage.getItem("wf-token") || "", action: null, actionTimer: null };

function toast(message, bad = false) {
  const node = $("#toast");
  node.textContent = message;
  node.classList.toggle("bad", bad);
  node.classList.add("show");
  setTimeout(() => node.classList.remove("show"), 3200);
}

async function api(path, options = {}) {
  const headers = { ...(options.headers || {}) };
  if (options.body) headers["Content-Type"] = "application/json";
  if (options.method && options.method !== "GET") headers["X-Wildflows-Token"] = state.token;
  const response = await fetch(path, { ...options, headers });
  if (!response.ok) {
    let detail = `${response.status} ${response.statusText}`;
    try { detail = (await response.json()).detail || detail; } catch (_) { /* response was not JSON */ }
    throw new Error(detail);
  }
  return response.json();
}

function requireToken() {
  if (state.token) return true;
  $("#token-dialog").showModal();
  return false;
}

async function loadRuns(preferred) {
  const data = await api("/api/runs");
  state.runs = data.runs;
  const select = $("#run-select");
  select.replaceChildren();
  if (!state.runs.length) {
    select.append(new Option("No runs yet", ""));
    renderEmpty();
    return;
  }
  for (const run of state.runs) select.append(new Option(`${run.run_id} · ${run.state}`, run.run_id));
  const target = preferred || select.value || state.runs[0].run_id;
  select.value = state.runs.some(run => run.run_id === target) ? target : state.runs[0].run_id;
  await selectRun(select.value);
}

async function selectRun(runId) {
  if (!runId) return;
  if (state.source) state.source.close();
  state.run = await api(`/api/runs/${encodeURIComponent(runId)}`);
  state.selected = null;
  render();
  const last = state.run.events.at(-1)?.seq ?? -1;
  state.source = new EventSource(`/api/runs/${encodeURIComponent(runId)}/events?after=${last}`);
  state.source.addEventListener("journal", async event => {
    appendEvent(JSON.parse(event.data));
    try {
      state.run = await api(`/api/runs/${encodeURIComponent(runId)}`);
      render(false);
    } catch (error) { toast(error.message, true); }
  });
  state.source.onopen = () => $("#live-dot").classList.add("on");
  state.source.onerror = () => $("#live-dot").classList.remove("on");
}

function renderEmpty() {
  $("#run-title").textContent = "No runs";
  $("#run-state").textContent = "READY";
  $("#tree").textContent = "Launch a workflow to begin.";
  $("#journal").replaceChildren();
}

function render(resetEvents = true) {
  const run = state.run;
  if (!run) return renderEmpty();
  $("#run-title").textContent = run.run_id;
  $("#rationale").textContent = run.rationale || run.completed?.summary || "Waiting for planner rationale";
  $("#run-state").textContent = run.state.toUpperCase();
  $("#run-state").className = `status-${run.state}`;
  $("#epoch-count").textContent = run.epoch == null ? "—" : `${run.epoch + 1} / ${run.epoch_count}`;
  $("#rails").textContent = railsLabel(run.rails);
  $("#kill-button").disabled = !run.killable;
  $("#resume-button").disabled = run.active || run.state === "completed";
  renderTree();
  renderInspector();
  if (resetEvents) renderEvents();
}

function railsLabel(rails) {
  if (!rails) return "—";
  const bits = [];
  if (rails.deadline_s) bits.push(`${Math.round(rails.deadline_s)}s`);
  if (rails.max_epochs) bits.push(`${rails.max_epochs} epochs`);
  return bits.join(" · ") || "open";
}

function renderTree() {
  const target = $("#tree");
  target.replaceChildren();
  const root = buildTree(state.run.expression, state.run.nodes);
  if (!root) {
    target.className = "tree empty";
    target.textContent = "No admitted expression";
    return;
  }
  target.className = "tree";
  target.append(renderBranch(root));
}

function renderBranch(node) {
  const branch = document.createElement("div");
  branch.className = "branch";
  const card = document.createElement("button");
  card.className = `node state-${node.state}${state.selected === node.id ? " selected" : ""}`;
  card.dataset.node = node.id;
  const dot = document.createElement("i"); dot.className = node.state;
  const copy = document.createElement("span");
  const kind = document.createElement("small"); kind.textContent = node.kind;
  const label = document.createElement("strong"); label.textContent = node.label || node.id;
  const id = document.createElement("code"); id.textContent = node.id;
  copy.append(kind, label, id); card.append(dot, copy);
  card.addEventListener("click", () => { state.selected = node.id; renderTree(); renderInspector(); });
  branch.append(card);
  if (node.children.length) {
    const children = document.createElement("div"); children.className = "children";
    node.children.forEach(child => children.append(renderBranch(child)));
    branch.append(children);
  }
  return branch;
}

function line(label, value) {
  const row = document.createElement("div"); row.className = "detail-line";
  const key = document.createElement("span"); key.textContent = label;
  const content = document.createElement("strong"); content.textContent = value ?? "—";
  row.append(key, content); return row;
}

function section(title, content) {
  const wrap = document.createElement("section"); wrap.className = "inspect-section";
  const heading = document.createElement("h3"); heading.textContent = title;
  wrap.append(heading, content); return wrap;
}

function pre(value) { const node = document.createElement("pre"); node.textContent = value || "—"; return node; }

function renderInspector() {
  const target = $("#inspector"); target.replaceChildren();
  if (!state.selected || !state.run.nodes[state.selected]) {
    target.className = "inspector";
    target.append(pre("Select a node to inspect its task, result, receipts, and artifacts."));
    const ask = state.run.pending_questions[0];
    if (ask) target.append(renderAsk(ask));
    if (state.run.files.length) target.append(section("Run artifacts & decisions", renderArtifacts(state.run.files)));
    return;
  }
  target.className = "inspector";
  const node = state.run.nodes[state.selected];
  const summary = document.createElement("div"); summary.className = "detail-grid";
  summary.append(line("Node", node.node_id), line("Kind", node.kind), line("State", node.state), line("Rig", node.rig), line("Attempts", node.dispatch_count));
  target.append(summary);
  target.append(section("Task", pre(node.task || JSON.stringify(node.expression, null, 2))));
  if (node.result) target.append(section(`Result · ${node.result.outcome}`, pre(node.result.text)));
  if (node.loop_status) target.append(section("Loop", pre(`${node.loop_status}\n${node.loop_iterations} iteration(s)`)));
  if (node.receipts.length) target.append(section("Integration receipts", pre(JSON.stringify(node.receipts, null, 2))));
  if (node.artifacts.length) target.append(section("Artifacts", renderArtifacts(node.artifacts)));
  const nodePaths = new Set(node.artifacts.map(item => item.path));
  const runFiles = state.run.files.filter(item => !nodePaths.has(item.path));
  if (runFiles.length) target.append(section("Run artifacts & decisions", renderArtifacts(runFiles)));
  const ask = state.run.pending_questions.find(item => item.node_id === node.node_id);
  if (ask) target.append(renderAsk(ask));
}

function renderAsk(ask) {
  const form = document.createElement("form"); form.className = "ask-card";
  const tag = document.createElement("span"); tag.className = "eyebrow"; tag.textContent = "OWNER INPUT REQUIRED";
  const title = document.createElement("h3"); title.textContent = ask.question;
  const input = document.createElement("textarea"); input.placeholder = "Answer the planner…"; input.required = true;
  if (ask.options.length) input.placeholder += `\nOptions: ${ask.options.join(" · ")}`;
  const submit = document.createElement("button"); submit.className = "button"; submit.textContent = "Answer & resume";
  form.append(tag, title, input, submit);
  form.addEventListener("submit", async event => {
    event.preventDefault();
    if (!requireToken()) return;
    await mutate(`/api/runs/${encodeURIComponent(state.run.run_id)}/answer`, { answer: input.value, node_id: ask.node_id }, "Answer submitted");
  });
  return form;
}

function renderArtifacts(items) {
  const wrap = document.createElement("div"); wrap.className = "artifacts";
  for (const item of items) {
    const card = document.createElement("article");
    const link = document.createElement("a"); link.href = item.url; link.target = "_blank"; link.rel = "noopener"; link.textContent = `${item.name} · ${formatBytes(item.size)}`;
    card.append(link);
    if (item.mime.startsWith("image/")) { const image = document.createElement("img"); image.src = item.url; image.alt = item.name; card.append(image); }
    else if (item.mime === "text/html") { const frame = document.createElement("iframe"); frame.src = item.url; frame.sandbox = ""; frame.title = item.name; card.append(frame); }
    else if (item.mime.startsWith("text/") || item.mime === "application/json") {
      const output = pre("Loading…"); card.append(output);
      fetch(item.url).then(response => response.text()).then(text => { output.textContent = text; }).catch(() => { output.textContent = "Preview unavailable"; });
    }
    wrap.append(card);
  }
  return wrap;
}

function formatBytes(value) { return value < 1024 ? `${value} B` : `${(value / 1024).toFixed(1)} KB`; }

function renderEvents() {
  const target = $("#journal"); target.replaceChildren();
  for (const event of state.run.events) appendEvent(event);
  target.scrollTop = target.scrollHeight;
}

function appendEvent(event) {
  const target = $("#journal");
  const row = document.createElement("div"); row.className = `event event-${event.kind}`;
  const seq = document.createElement("code"); seq.textContent = String(event.seq).padStart(4, "0");
  const kind = document.createElement("strong"); kind.textContent = event.kind;
  const node = document.createElement("span"); node.textContent = `e${event.epoch} · ${event.node_id}`;
  const summary = document.createElement("span"); summary.textContent = eventSummary(event);
  row.append(seq, kind, node, summary); target.append(row); target.scrollTop = target.scrollHeight;
}

function eventSummary(event) {
  if (event.kind === "boundary") return `${event.phase}${event.reason ? ` · ${event.reason}` : ""}`;
  if (event.kind === "dispatched") return event.task || event.cmd || event.rig || "started";
  if (event.kind === "result") return `${event.outcome} · ${(event.text || "").slice(0, 140)}`;
  if (event.kind === "integrated") return `${event.commits.length} commit(s)`;
  if (event.kind === "asked") return event.question;
  if (event.kind === "answered") return event.answer;
  if (event.kind === "loop_iter") return `iteration ${event.iteration + 1}${event.converged ? " · converged" : ""}`;
  return "";
}

async function mutate(path, body, message) {
  try {
    const data = await api(path, { method: "POST", body: JSON.stringify(body) });
    toast(message);
    if (data.action_id) watchAction(data.action_id);
    setTimeout(() => loadRuns(data.run_id || state.run?.run_id), 300);
    return data;
  } catch (error) { toast(error.message, true); throw error; }
}

function watchAction(actionId) {
  state.action = actionId;
  clearInterval(state.actionTimer);
  const poll = async () => {
    try {
      const action = await api(`/api/actions/${actionId}`);
      $("#action-log").textContent = action.log || "Waiting for output…";
      $("#action-log").scrollTop = $("#action-log").scrollHeight;
      if (action.state === "finished") { clearInterval(state.actionTimer); loadRuns(action.run_id); }
    } catch (error) { clearInterval(state.actionTimer); toast(error.message, true); }
  };
  poll(); state.actionTimer = setInterval(poll, 500);
}

$("#run-select").addEventListener("change", event => selectRun(event.target.value).catch(error => toast(error.message, true)));
$("#token-button").addEventListener("click", () => { $("#token-input").value = state.token; $("#token-dialog").showModal(); });
$("#token-form").addEventListener("submit", event => { if (event.submitter?.value === "cancel") return; state.token = $("#token-input").value; sessionStorage.setItem("wf-token", state.token); toast("Control token saved for this tab"); });
$("#launch-button").addEventListener("click", () => { if (requireToken()) $("#launch-dialog").showModal(); });
$("#launch-form").addEventListener("submit", async event => {
  if (event.submitter?.value === "cancel") return;
  event.preventDefault();
  const form = new FormData(event.target);
  const body = Object.fromEntries(form.entries()); body.max_workers = Number(body.max_workers); if (!body.run_id) delete body.run_id; if (!body.run_branch) delete body.run_branch;
  const data = await mutate("/api/runs", body, "Run launched"); $("#launch-dialog").close(); await loadRuns(data.run_id);
});
$("#resume-button").addEventListener("click", () => { if (requireToken()) mutate(`/api/runs/${encodeURIComponent(state.run.run_id)}/resume`, {}, "Resume launched"); });
$("#kill-button").addEventListener("click", () => { if (requireToken() && confirm("Kill the active expression? Durable journal state will be preserved for resume.")) mutate(`/api/runs/${encodeURIComponent(state.run.run_id)}/kill`, {}, "Expression killed"); });

document.querySelectorAll(".tab").forEach(tab => tab.addEventListener("click", () => {
  document.querySelectorAll(".tab").forEach(item => item.classList.toggle("active", item === tab));
  $("#journal").classList.toggle("hidden", tab.dataset.stream !== "journal");
  $("#action-log").classList.toggle("hidden", tab.dataset.stream !== "action");
}));

loadRuns().catch(error => toast(error.message, true));
