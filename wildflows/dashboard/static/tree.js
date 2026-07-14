export function expressionChildren(expr) {
  if (!expr || typeof expr !== "object") return [];
  if (expr.kind === "seq" || expr.kind === "dispatch") return Array.isArray(expr.children) ? expr.children : [];
  if (expr.kind === "combine") return Array.isArray(expr.inputs) ? expr.inputs : [];
  if (expr.kind === "loop") return expr.body ? [expr.body] : [];
  return [];
}

const priority = ["failed", "parked-ask", "running", "pending", "integrated"];

export function aggregateState(own, children) {
  if (own && own !== "pending") return own;
  if (!children.length) return own || "pending";
  const states = children.map(child => child.state);
  return priority.find(state => states.includes(state)) || "pending";
}

export function buildTree(expr, nodes = {}) {
  if (!expr || typeof expr !== "object") return null;
  const children = expressionChildren(expr).map(child => buildTree(child, nodes)).filter(Boolean);
  const detail = nodes[expr.node_id] || {};
  const rig = detail.rig || (expr.rig && expr.rig.name) || null;
  return {
    id: expr.node_id || "?",
    kind: expr.kind || "unknown",
    label: detail.task || expr.task || expr.question || expr.cmd || expr.kind || "unknown",
    rig,
    state: aggregateState(detail.state, children),
    detail,
    expression: expr,
    children,
  };
}

function connect(edges, fromIds, toIds) {
  for (const from of fromIds) {
    for (const to of toIds) {
      if (from !== to) edges.push([from, to]);
    }
  }
}

function graph(node, nodes, edges) {
  nodes.push(node);
  if (!node.children.length) return { entries: [node.id], exits: [node.id] };
  const parts = node.children.map(child => graph(child, nodes, edges));
  if (node.kind === "combine") {
    for (const part of parts) connect(edges, part.exits, [node.id]);
    return { entries: parts.flatMap(part => part.entries), exits: [node.id] };
  }
  if (node.kind === "seq") {
    connect(edges, [node.id], parts[0].entries);
    for (let index = 1; index < parts.length; index += 1) {
      connect(edges, parts[index - 1].exits, parts[index].entries);
    }
    return { entries: [node.id], exits: parts.at(-1).exits };
  }
  connect(edges, [node.id], parts.flatMap(part => part.entries));
  return { entries: [node.id], exits: parts.flatMap(part => part.exits) };
}

export function phaseLayout(root) {
  if (!root) return { lanes: [], edges: [] };
  const nodes = [];
  const rawEdges = [];
  graph(root, nodes, rawEdges);
  const ids = new Set(nodes.map(node => node.id));
  const edgeKeys = new Set();
  const edges = rawEdges.filter(([from, to]) => {
    const key = `${from}\u0000${to}`;
    if (!ids.has(from) || !ids.has(to) || edgeKeys.has(key)) return false;
    edgeKeys.add(key);
    return true;
  });
  const indegree = new Map(nodes.map(node => [node.id, 0]));
  const outgoing = new Map(nodes.map(node => [node.id, []]));
  for (const [from, to] of edges) {
    indegree.set(to, indegree.get(to) + 1);
    outgoing.get(from).push(to);
  }
  const order = new Map(nodes.map((node, index) => [node.id, index]));
  const levels = new Map(nodes.map(node => [node.id, 0]));
  const ready = nodes.filter(node => indegree.get(node.id) === 0).map(node => node.id);
  const visited = new Set();
  while (ready.length) {
    ready.sort((left, right) => order.get(left) - order.get(right));
    const id = ready.shift();
    visited.add(id);
    for (const next of outgoing.get(id)) {
      levels.set(next, Math.max(levels.get(next), levels.get(id) + 1));
      indegree.set(next, indegree.get(next) - 1);
      if (indegree.get(next) === 0) ready.push(next);
    }
  }
  for (const node of nodes) {
    if (!visited.has(node.id)) levels.set(node.id, Math.max(...levels.values(), 0) + 1);
  }
  const laneCount = Math.max(...levels.values(), 0) + 1;
  const lanes = Array.from({ length: laneCount }, (_, index) => ({ index, nodes: [] }));
  for (const node of nodes) lanes[levels.get(node.id)].nodes.push(node);
  return { lanes: lanes.filter(lane => lane.nodes.length), edges };
}

export function laneLabel(nodes) {
  const kinds = [...new Set(nodes.map(node => node.kind))];
  const rigs = [...new Set(nodes.map(node => node.rig).filter(Boolean))];
  if (kinds.length > 1) return "Mixed phase";
  const kind = kinds[0] || "phase";
  if (kind === "dispatch") return "Dispatch";
  if (kind === "combine") return `${rigs[0] ? `${rigs[0]} ` : ""}combine`;
  if (kind === "do") return rigs.length === 1 ? `${rigs[0]} workers` : "Workers";
  if (kind === "seq") return "Sequence";
  if (kind === "loop") return "Loop";
  if (kind === "ask") return "Owner ask";
  if (kind === "setup") return "Setup";
  if (kind === "inplace") return "In-place edits";
  return kind;
}
