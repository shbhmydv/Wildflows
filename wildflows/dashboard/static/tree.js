export function expressionChildren(expr) {
  if (!expr || typeof expr !== "object") return [];
  if (expr.kind === "seq" || expr.kind === "dispatch") return expr.children || [];
  if (expr.kind === "combine") return expr.inputs || [];
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
  if (!expr) return null;
  const children = expressionChildren(expr).map(child => buildTree(child, nodes)).filter(Boolean);
  const detail = nodes[expr.node_id] || {};
  return {
    id: expr.node_id || "?",
    kind: expr.kind || "unknown",
    label: detail.task || expr.task || expr.question || expr.cmd || expr.kind,
    state: aggregateState(detail.state, children),
    children,
  };
}
