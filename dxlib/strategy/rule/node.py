"""
Context and base Node for the strategy rule engine.

Context  - carries all tick-local and cross-tick state. One instance
            is created per tick and passed through the entire graph.

Node     - abstract base for every node type. Pull-based evaluation:
            Node.evaluate(ctx) is called at most once per tick (result
            is cached on the context).  Nodes declare their ports
            statically via class-level PortSpec helpers.
"""
import abc
import logging
import uuid
from typing import Any, Optional

from .types import DType, LOBSnapshot, Order, Port, PortDirection


# ---------------------------------------------------------------------------
# Evaluation context
# ---------------------------------------------------------------------------

class Context:
    """
    Per-tick execution context.

    Attributes
    ----------
    lob         : Current LOB snapshot (the entry data-point for the tick).
    tick_index  : Monotonically increasing tick counter (0-based).
    timestamp   : Wall or simulated time of the tick (unix seconds).
    variables   : Cross-tick mutable store. Keyed by variable node id.
                  Persists across ticks - this is where all stateful
                  node values live between evaluations.
    _cache      : Within-tick result cache. Keyed by node id.
                  Cleared at the start of each tick by the Runner.
    orders      : Orders accumulated by sink nodes during this tick.
    metadata    : Arbitrary key-value bag for strategy-level config
                  (e.g. symbol, position limits, etc.).
    """

    def __init__(
        self,
        lob:        LOBSnapshot,
        tick_index: int = 0,
        variables:  Optional[dict[str, Any]] = None,
        metadata:   Optional[dict[str, Any]] = None,
    ) -> None:
        self.lob        = lob
        self.tick_index = tick_index
        self.timestamp  = lob.timestamp
        self.variables: dict[str, Any]  = variables if variables is not None else {}
        self._cache:    dict[str, Any]  = {}
        self.orders:    list[Order]     = []
        self.metadata:  dict[str, Any]  = metadata if metadata is not None else {}

    # ------------------------------------------------------------------
    # Cache helpers (used by Node.evaluate)
    # ------------------------------------------------------------------

    def has_result(self, node_id: str) -> bool:
        return node_id in self._cache

    def get_result(self, node_id: str) -> Any:
        return self._cache[node_id]

    def set_result(self, node_id: str, value: Any) -> None:
        self._cache[node_id] = value

    def emit_order(self, order: Order) -> None:
        self.orders.append(order)

    # ------------------------------------------------------------------
    # Variable store helpers
    # ------------------------------------------------------------------

    def get_var(self, var_id: str, default: Any = None) -> Any:
        return self.variables.get(var_id, default)

    def set_var(self, var_id: str, value: Any) -> None:
        self.variables[var_id] = value


# ---------------------------------------------------------------------------
# Port specification helpers (class-level node declaration)
# ---------------------------------------------------------------------------

def input_port(
    name: str,
    dtype: DType,
    *,
    required: bool = True,
    default: Any = None,
    doc: str = "",
) -> Port:
    return Port(name, dtype, PortDirection.INPUT, required=required, default=default, doc=doc)


def output_port(name: str, dtype: DType, *, doc: str = "") -> Port:
    return Port(name, dtype, PortDirection.OUTPUT, doc=doc)


# ---------------------------------------------------------------------------
# Base Node
# ---------------------------------------------------------------------------

class Node(abc.ABC):
    """
    Abstract base class for all rule engine nodes.

    Subclasses must:
      1. Define class-level ``INPUT_PORTS`` and ``OUTPUT_PORTS`` lists
         of Port descriptors. These are used for graph validation.
      2. Implement ``_compute(ctx) -> dict[str, Any]`` which returns a
         mapping of output port name → value.

    The public ``evaluate(ctx)`` method handles caching and fan-out
    (multiple downstream consumers of the same output).

    Connections
    -----------
    A node's inputs are resolved by the Graph before ``evaluate`` is
    called - it passes in ``resolved_inputs: dict[str, Any]`` where
    missing optional inputs are replaced by their ``Port.default``.
    This keeps ``_compute`` clean and independent of graph topology.
    """

    # Subclasses override these
    INPUT_PORTS:  list[Port] = []
    OUTPUT_PORTS: list[Port] = []

    # Human-readable category shown in the visual editor
    CATEGORY: str = "misc"

    def __init__(self, node_id: Optional[str] = None, label: Optional[str] = None) -> None:
        self.node_id: str          = node_id or str(uuid.uuid4())
        self.label:   str          = label or self.__class__.__name__
        # Runtime wiring - set by Graph.connect()
        # Maps input port name → (source_node, output_port_name)
        self._wiring: dict[str, tuple["Node", str]] = {}

    # ------------------------------------------------------------------
    # Port introspection helpers
    # ------------------------------------------------------------------

    def input_port(self, name: str) -> Port:
        for p in self.INPUT_PORTS:
            if p.name == name:
                return p
        raise KeyError(f"Node {self.label!r} has no input port {name!r}")

    def output_port(self, name: str) -> Port:
        for p in self.OUTPUT_PORTS:
            if p.name == name:
                return p
        raise KeyError(f"Node {self.label!r} has no output port {name!r}")

    @property
    def input_port_names(self) -> list[str]:
        return [p.name for p in self.INPUT_PORTS]

    @property
    def output_port_names(self) -> list[str]:
        return [p.name for p in self.OUTPUT_PORTS]

    # ------------------------------------------------------------------
    # Wiring (called by Graph)
    # ------------------------------------------------------------------

    def wire(self, input_name: str, source_node: "Node", source_output: str) -> None:
        """Connect one of this node's inputs to a source node's output."""
        in_port  = self.input_port(input_name)
        out_port = source_node.output_port(source_output)

        if not out_port.dtype.compatible_with(in_port.dtype):
            raise TypeError(
                f"Type mismatch: {source_node.label}.{source_output} "
                f"({out_port.dtype.name}) → {self.label}.{input_name} "
                f"({in_port.dtype.name})"
            )
        self._wiring[input_name] = (source_node, source_output)

    # ------------------------------------------------------------------
    # Evaluation (pull-based, cached per tick)
    # ------------------------------------------------------------------

    def evaluate(self, ctx: Context, logger: logging.Logger = None) -> dict[str, Any]:
        """
        Public entry point.  Returns full output dict for this node.
        Result is cached on ctx so repeated downstream consumers are free.
        """
        logger = logger if logger is not None else logging.getLogger()
        if ctx.has_result(self.node_id):
            return ctx.get_result(self.node_id)

        # Resolve all inputs
        resolved: dict[str, Any] = {}
        for port in self.INPUT_PORTS:
            if port.name in self._wiring:
                src_node, src_out = self._wiring[port.name]
                upstream = src_node.evaluate(ctx)
                resolved[port.name] = upstream[src_out]
            elif not port.required:
                resolved[port.name] = port.default
            else:
                raise RuntimeError(
                    f"Node {self.label!r}: required input port {port.name!r} is not connected."
                )

        result = self._compute(ctx, resolved)

        # Validate outputs are complete
        for port in self.OUTPUT_PORTS:
            if port.name not in result:
                raise RuntimeError(
                    f"Node {self.label!r}: _compute did not produce output {port.name!r}."
                )

        ctx.set_result(self.node_id, result)
        logger.info(f"Node {self.label} outputted {result}")
        return result

    @abc.abstractmethod
    def _compute(self, ctx: Context, inputs: dict[str, Any]) -> dict[str, Any]:
        """
        Core computation. Receives resolved input values, returns output values.

        ``ctx`` is available for reading LOB snapshot, cross-tick variable
        state (via ctx.get_var / ctx.set_var), and emitting orders.
        Do not cache results here - the base class handles caching.
        """

    # ------------------------------------------------------------------
    # Serialisation stub (for graph XML round-trip)
    # ------------------------------------------------------------------

    def to_dict(self) -> dict:
        """Minimal descriptor for graph persistence. Subclasses extend."""
        return {
            "node_id":  self.node_id,
            "type":     self.__class__.__name__,
            "label":    self.label,
            "category": self.CATEGORY,
            "wiring": {
                inp: {"source_node": src.node_id, "source_port": out}
                for inp, (src, out) in self._wiring.items()
            },
        }

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} id={self.node_id[:8]} label={self.label!r}>"
