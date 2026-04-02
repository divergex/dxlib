"""
Strategy graph - wires nodes together, validates topology, and drives
per-tick execution.

Graph
─────
  Holds a set of Node instances.
  Manages edges (port-level connections with type checking).
  Computes topological sort (Kahn's algorithm) for cycle detection.
  Runs a single tick: creates a Context, evaluates all sink nodes
  (pull-based - upstream nodes are evaluated on demand via the cache),
  returns the Context (with orders, variable state, etc.).

StrategyRunner
──────────────
  Wraps a Graph and maintains persistent variable state across ticks.
  Single public method: step(lob_snapshot) → list[Order].
"""
import json
import logging
from collections import defaultdict, deque
from typing import Any, Optional

from .node import Context, Node
from .nodes import LOBSource
from .types import LOBSnapshot, Order


# ---------------------------------------------------------------------------
# Edge descriptor
# ---------------------------------------------------------------------------

class Edge:
    """
    Directed, typed connection between two ports.

    src_node.src_port → dst_node.dst_port
    """

    def __init__(
            self,
            src_node: Node, src_port: str,
            dst_node: Node, dst_port: str,
    ) -> None:
        self.src_node = src_node
        self.src_port = src_port
        self.dst_node = dst_node
        self.dst_port = dst_port

    def __repr__(self) -> str:
        return (
            f"Edge({self.src_node.label}.{self.src_port} → "
            f"{self.dst_node.label}.{self.dst_port})"
        )


# ---------------------------------------------------------------------------
# Graph
# ---------------------------------------------------------------------------

class Graph:
    """
    Directed acyclic graph of strategy nodes.

    Usage
    -----
    g = Graph()
    src  = g.add(LOBSource())
    ewma = g.add(EWMA(span=20))
    cmp  = g.add(Compare(op="gt"))
    ord_ = g.add(LimitOrderGen(side="buy", symbol="SIM"))

    g.connect(src,  "mid",    ewma, "value")
    g.connect(src,  "mid",    cmp,  "a")
    g.connect(ewma, "ewma",   cmp,  "b")
    g.connect(cmp,  "result", ord_, "trigger")
    g.connect(src,  "best_ask", ord_, "price")
    g.connect(Constant(1.0), "value", ord_, "size")

    g.validate()
    """

    def __init__(self, logger: logging.Logger=None) -> None:
        self._nodes: dict[str, Node] = {}
        self._edges: list[Edge] = []
        self.logger = logger if logger is not None else logging.getLogger()

    # ------------------------------------------------------------------
    # Node management
    # ------------------------------------------------------------------

    def add(self, node: Node) -> Node:
        """Register a node with the graph and return it (fluent API)."""
        if node.node_id in self._nodes:
            raise ValueError(f"Node {node.node_id!r} is already in this graph.")
        self._nodes[node.node_id] = node
        return node

    def remove(self, node: Node) -> None:
        """Remove a node and all edges touching it."""
        self._edges = [
            e for e in self._edges
            if e.src_node is not node and e.dst_node is not node
        ]
        self._nodes.pop(node.node_id, None)

    @property
    def nodes(self) -> list[Node]:
        return list(self._nodes.values())

    # ------------------------------------------------------------------
    # Edge management
    # ------------------------------------------------------------------

    def connect(
            self,
            src_node: Node, src_port: str,
            dst_node: Node, dst_port: str,
    ) -> Edge:
        """
        Wire src_node.src_port → dst_node.dst_port.

        Both nodes are auto-registered if not already in the graph.
        Type compatibility is checked immediately via Node.wire().
        """
        for node in (src_node, dst_node):
            if node.node_id not in self._nodes:
                self.add(node)

        # Delegates type checking
        dst_node.wire(dst_port, src_node, src_port)

        edge = Edge(src_node, src_port, dst_node, dst_port)
        self._edges.append(edge)
        return edge

    def disconnect(self, edge: Edge) -> None:
        self._edges.remove(edge)
        # Unwire from destination node
        edge.dst_node._wiring.pop(edge.dst_port, None)

    def edges_from(self, node: Node) -> list[Edge]:
        return [e for e in self._edges if e.src_node is node]

    def edges_into(self, node: Node) -> list[Edge]:
        return [e for e in self._edges if e.dst_node is node]

    # ------------------------------------------------------------------
    # Topology analysis
    # ------------------------------------------------------------------

    def topological_order(self) -> list[Node]:
        """
        Kahn's algorithm over node-level dependencies.
        Raises RuntimeError on cycles.
        Returns nodes in evaluation order (sources first, sinks last).

        Variable nodes (those that override evaluate() for deferred writes)
        are treated as sources - their input ports are resolved in a deferred
        pass and therefore do not create topological dependencies.
        """
        from .nodes import Variable, BoolVariable
        _deferred_types = (Variable, BoolVariable)

        in_degree: dict[str, int] = defaultdict(int)
        successors: dict[str, list[str]] = defaultdict(list)

        for nid in self._nodes:
            in_degree[nid] = in_degree.get(nid, 0)

        for edge in self._edges:
            dst = edge.dst_node
            # Edges into deferred-write Variable nodes don't count toward
            # topological in-degree - those writes happen in pass 2.
            if isinstance(dst, _deferred_types):
                continue
            src_id = edge.src_node.node_id
            dst_id = dst.node_id
            successors[src_id].append(dst_id)
            in_degree[dst_id] += 1

        queue = deque(nid for nid, deg in in_degree.items() if deg == 0)
        order: list[Node] = []

        while queue:
            nid = queue.popleft()
            order.append(self._nodes[nid])
            for child_id in successors[nid]:
                in_degree[child_id] -= 1
                if in_degree[child_id] == 0:
                    queue.append(child_id)

        if len(order) != len(self._nodes):
            cycle_nodes = [
                self._nodes[nid].label
                for nid, deg in in_degree.items()
                if deg > 0
            ]
            raise RuntimeError(
                f"Cycle detected in strategy graph. "
                f"Nodes involved: {cycle_nodes}"
            )

        return order

    def sink_nodes(self) -> list[Node]:
        """
        Nodes with no outgoing edges (leaf sinks).
        These are the roots for the pull-based evaluator to start from.
        In practice they're OrderGen nodes, but any node with no consumers qualifies.
        """
        nodes_with_outgoing = {e.src_node.node_id for e in self._edges}
        return [n for n in self._nodes.values() if n.node_id not in nodes_with_outgoing]

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def validate(self) -> list[str]:
        """
        Full graph validation. Returns a list of warning/error strings.
        Raises RuntimeError on fatal errors (cycles, unconnected required ports).
        """
        errors: list[str] = []
        warnings: list[str] = []

        # 1. Cycle check (also ensures topological order is computable)
        try:
            topo = self.topological_order()
        except RuntimeError as e:
            raise RuntimeError(str(e)) from e

        # 2. Required input ports connected
        for node in self._nodes.values():
            for port in node.INPUT_PORTS:
                if port.required and port.name not in node._wiring:
                    errors.append(
                        f"Node {node.label!r}: required input port "
                        f"{port.name!r} ({port.dtype.name}) is not connected."
                    )

        # 3. Isolated nodes (no edges at all) - warn, not error
        connected_ids = {e.src_node.node_id for e in self._edges} | \
                        {e.dst_node.node_id for e in self._edges}
        for node in self._nodes.values():
            if node.node_id not in connected_ids:
                if node.INPUT_PORTS or node.OUTPUT_PORTS:
                    warnings.append(f"Node {node.label!r} has no connections.")

        # 4. No LOBSource → graph can't run
        if not any(isinstance(n, LOBSource) for n in self._nodes.values()):
            warnings.append(
                "Graph has no LOBSource node - strategy will have no market data."
            )

        all_messages = [f"[ERROR] {e}" for e in errors] + \
                       [f"[WARN]  {w}" for w in warnings]

        if errors:
            raise RuntimeError(
                "Graph validation failed:\n" + "\n".join(all_messages)
            )

        return all_messages  # warnings only if we reach here

    # ------------------------------------------------------------------
    # Single-tick execution
    # ------------------------------------------------------------------

    def run_tick(
            self,
            lob: LOBSnapshot,
            tick_index: int = 0,
            variables: Optional[dict[str, Any]] = None,
            metadata: Optional[dict[str, Any]] = None,
    ) -> Context:
        """
        Execute one tick of the strategy.

        Two-pass execution
        ──────────────────
        Pass 1 (pull): evaluate all sink nodes. Variable nodes return their
                       stored (previous-tick) values immediately and schedule
                       a deferred write.
        Pass 2 (write): execute all deferred Variable writes. This resolves
                        the set/clear/update inputs of Variable nodes, which
                        may now safely evaluate their upstream because the
                        full graph has already computed.

        Returns the Context, which contains:
          - ctx.orders      : list[Order] emitted this tick
          - ctx.variables   : updated variable store (persist this!)
          - ctx._cache      : per-node output dict (useful for debugging)
        """
        ctx = Context(lob, tick_index, variables=variables, metadata=metadata)
        ctx._deferred = []  # populated by Variable.evaluate()

        # Pass 1: pull evaluation from all sinks
        for sink in self.sink_nodes():
            sink.evaluate(ctx, self.logger)

        # Pass 2: deferred variable writes
        for fn in ctx._deferred:
            fn(ctx)

        return ctx

    # ------------------------------------------------------------------
    # Serialisation
    # ------------------------------------------------------------------

    def to_dict(self) -> dict:
        return {
            "nodes": [n.to_dict() for n in self._nodes.values()],
            "edges": [
                {
                    "src_node": e.src_node.node_id,
                    "src_port": e.src_port,
                    "dst_node": e.dst_node.node_id,
                    "dst_port": e.dst_port,
                }
                for e in self._edges
            ],
        }

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent)

    def __repr__(self) -> str:
        return (
            f"<Graph nodes={len(self._nodes)} edges={len(self._edges)}>"
        )


# ---------------------------------------------------------------------------
# StrategyRunner - stateful tick loop
# ---------------------------------------------------------------------------

class StrategyRunner:
    """
    Wraps a Graph and maintains persistent variable state across ticks.

    The exchange simulation layer calls ``step()`` once per tick with the
    latest LOB snapshot and receives back the list of orders to submit.

    Example
    -------
    runner = StrategyRunner(graph, symbol="SIM")
    for snapshot in exchange.feed():
        orders = runner.step(snapshot)
        for order in orders:
            exchange.submit(order)
    """

    def __init__(
            self,
            graph: Graph,
            symbol: str = "SIM",
            metadata: Optional[dict[str, Any]] = None,
            validate: bool = True,
    ) -> None:
        self.graph = graph
        self.symbol = symbol
        self.metadata = metadata or {}
        self._variables: dict[str, Any] = {}
        self._tick_index: int = 0

        if validate:
            warnings = graph.validate()
            for w in warnings:
                print(w)

    def step(self, lob: LOBSnapshot) -> list[Order]:
        """
        Advance one tick.

        Parameters
        ----------
        lob : LOBSnapshot
            Current market state.

        Returns
        -------
        list[Order]
            Orders to submit to the exchange this tick.
        """
        ctx = self.graph.run_tick(
            lob,
            tick_index=self._tick_index,
            variables=self._variables,
            metadata=self.metadata,
        )
        # Persist variable state for next tick
        self._variables = ctx.variables
        self._tick_index += 1
        return ctx.orders

    def reset(self) -> None:
        """Reset all stateful variable to their initial values."""
        self._variables = {}
        self._tick_index = 0

    @property
    def tick_index(self) -> int:
        return self._tick_index

    def debug_last_tick(self, lob: LOBSnapshot) -> dict[str, Any]:
        """
        Run a tick and return the full per-node output cache for inspection.
        Does NOT advance tick_index or persist variables.
        """
        ctx = self.graph.run_tick(
            lob,
            tick_index=self._tick_index,
            variables=dict(self._variables),
            metadata=self.metadata,
        )
        return {
            node.label: ctx.get_result(node.node_id)
            for node in self.graph.nodes
            if ctx.has_result(node.node_id)
        }
