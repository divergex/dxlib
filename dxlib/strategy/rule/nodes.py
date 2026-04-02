"""
Concrete node library for the strategy rule engine.

Node taxonomy
─────────────
Source nodes      - extract scalar values from the LOB snapshot (no inputs)
Variable nodes    - cross-tick stateful storage with update rules
Math nodes        - arithmetic, comparison, logical operations
Condition nodes   - boolean predicates (threshold, crossover, in-range)
Branch nodes      - conditional routing (if/else gate)
Aggregator nodes  - rolling window statistics (EWMA, SMA, running extrema)
OrderGen nodes    - sinks that emit Order objects into the context

All nodes are pull-based: they implement _compute(ctx, inputs) and the
base class handles caching + input resolution (see node.py).
"""
import math
import operator
from collections import deque
from typing import Any, Callable, Optional

from .types import DType, LOBSnapshot, Order, OrderSide, OrderType
from .node import Node, Context, input_port, output_port


# ===========================================================================
# Source nodes - LOB field extractors
# ===========================================================================

class LOBSource(Node):
    """
    Entry point for the tick's LOB snapshot.
    Exposes all standard derived fields as typed outputs.
    No inputs - this is the root of every graph.
    """

    CATEGORY = "source"
    INPUT_PORTS = []
    OUTPUT_PORTS = [
        output_port("lob",        DType.LOB,   doc="Full LOB snapshot"),
        output_port("mid",        DType.FLOAT, doc="(best_bid + best_ask) / 2"),
        output_port("best_bid",   DType.FLOAT, doc="Top-of-book bid price"),
        output_port("best_ask",   DType.FLOAT, doc="Top-of-book ask price"),
        output_port("spread",     DType.FLOAT, doc="best_ask - best_bid"),
        output_port("imbalance",  DType.FLOAT, doc="(bid_depth - ask_depth) / total_depth"),
        output_port("bid_depth",  DType.FLOAT, doc="Total size on bid side"),
        output_port("ask_depth",  DType.FLOAT, doc="Total size on ask side"),
        output_port("last_price", DType.FLOAT, doc="Last trade price (may be None)"),
        output_port("last_size",  DType.FLOAT, doc="Last trade size (may be None)"),
        output_port("timestamp",  DType.FLOAT, doc="Tick timestamp (unix seconds)"),
        output_port("tick_index", DType.INT,   doc="Monotonic tick counter"),
    ]

    def _compute(self, ctx: Context, inputs: dict[str, Any]) -> dict[str, Any]:
        lob = ctx.lob
        return {
            "lob":        lob,
            "mid":        lob.mid,
            "best_bid":   lob.best_bid,
            "best_ask":   lob.best_ask,
            "spread":     lob.spread,
            "imbalance":  lob.imbalance,
            "bid_depth":  lob.bid_depth,
            "ask_depth":  lob.ask_depth,
            "last_price": lob.last_trade_price,
            "last_size":  lob.last_trade_size,
            "timestamp":  ctx.timestamp,
            "tick_index": ctx.tick_index,
        }


class LevelExtractor(Node):
    """
    Extract price or size from a specific LOB level (0-indexed).
    Separate nodes for bid/ask side.
    """

    CATEGORY = "source"
    INPUT_PORTS = [
        input_port("lob",   DType.LOB, doc="LOB snapshot"),
    ]
    OUTPUT_PORTS = [
        output_port("price", DType.FLOAT),
        output_port("size",  DType.FLOAT),
    ]

    def __init__(
        self,
        side: str = "bid",        # "bid" | "ask"
        level: int = 0,           # 0 = best
        node_id: Optional[str] = None,
        label: Optional[str] = None,
    ) -> None:
        super().__init__(node_id, label or f"{side}_L{level}")
        assert side in ("bid", "ask"), "side must be 'bid' or 'ask'"
        self.side  = side
        self.level = level

    def _compute(self, ctx: Context, inputs: dict[str, Any]) -> dict[str, Any]:
        lob: LOBSnapshot = inputs["lob"]
        lvl = lob.bid_at(self.level) if self.side == "bid" else lob.ask_at(self.level)
        price = lvl.price if lvl else float("nan")
        size  = lvl.size  if lvl else float("nan")
        return {"price": price, "size": size}

    def to_dict(self) -> dict:
        d = super().to_dict()
        d["params"] = {"side": self.side, "level": self.level}
        return d


class Constant(Node):
    """Emits a fixed scalar value every tick. Useful for thresholds."""

    CATEGORY = "source"
    INPUT_PORTS  = []
    OUTPUT_PORTS = [output_port("value", DType.FLOAT)]

    def __init__(self, value: float, **kwargs) -> None:
        super().__init__(**kwargs)
        self.value = value
        self.label = self.label or f"const({value})"

    def _compute(self, ctx: Context, inputs: dict[str, Any]) -> dict[str, Any]:
        return {"value": self.value}

    def to_dict(self) -> dict:
        d = super().to_dict()
        d["params"] = {"value": self.value}
        return d


class TickCounter(Node):
    """Outputs current tick_index mod period - useful for periodic logic."""

    CATEGORY = "source"
    INPUT_PORTS  = []
    OUTPUT_PORTS = [
        output_port("tick",   DType.INT),
        output_port("period_hit", DType.BOOL, doc="True when tick_index % period == 0"),
    ]

    def __init__(self, period: int = 10, **kwargs) -> None:
        super().__init__(**kwargs)
        self.period = period

    def _compute(self, ctx: Context, inputs: dict[str, Any]) -> dict[str, Any]:
        return {
            "tick":       ctx.tick_index,
            "period_hit": (ctx.tick_index % self.period) == 0,
        }


# ===========================================================================
# Variable nodes - cross-tick stateful storage
# ===========================================================================

class Variable(Node):
    """
    Stateful scalar variable persisting across ticks.

    Cycle-breaking semantics (same as BoolVariable)
    ────────────────────────────────────────────────
    ``value`` and ``prev`` outputs always reflect the state at the START of
    the current tick.  The ``update`` write is deferred until after the main
    evaluation pass, so downstream readers of ``value`` are not topologically
    dependent on whatever node feeds ``update``.
    """

    CATEGORY = "variable"
    INPUT_PORTS = [
        input_port("update",       DType.FLOAT, required=False, default=None,
                   doc="New value to write. If None, value is unchanged."),
        input_port("write_enable", DType.BOOL,  required=False, default=True,
                   doc="Gate: only write when True."),
    ]
    OUTPUT_PORTS = [
        output_port("value", DType.FLOAT, doc="Current stored value (start of tick)"),
        output_port("prev",  DType.FLOAT, doc="Value from previous tick"),
    ]

    def __init__(self, initial: float = 0.0, **kwargs) -> None:
        super().__init__(**kwargs)
        self.initial   = initial
        self._prev_key = f"{self.node_id}__prev"

    def evaluate(self, ctx: Context) -> dict[str, Any]:
        if ctx.has_result(self.node_id):
            return ctx.get_result(self.node_id)

        current = ctx.get_var(self.node_id,    self.initial)
        prev    = ctx.get_var(self._prev_key,  self.initial)

        result = {"value": current, "prev": prev}
        ctx.set_result(self.node_id, result)

        if not hasattr(ctx, "_deferred"):
            ctx._deferred = []
        ctx._deferred.append(self._deferred_write)

        return result

    def _deferred_write(self, ctx: Context) -> None:
        current = ctx.get_var(self.node_id, self.initial)

        new_val    = None
        write_gate = True

        if "update" in self._wiring:
            src_node, src_out = self._wiring["update"]
            new_val = src_node.evaluate(ctx).get(src_out)

        if "write_enable" in self._wiring:
            src_node, src_out = self._wiring["write_enable"]
            write_gate = src_node.evaluate(ctx).get(src_out, True)

        if new_val is not None and write_gate:
            ctx.set_var(self._prev_key, current)
            ctx.set_var(self.node_id,   new_val)

    def _compute(self, ctx: Context, inputs: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError

    def to_dict(self) -> dict:
        d = super().to_dict()
        d["params"] = {"initial": self.initial}
        return d


class BoolVariable(Node):
    """
    Boolean flag variable - e.g. 'position_open', 'cooldown_active'.

    Cycle-breaking semantics
    ────────────────────────
    Variable nodes are the canonical cycle-breakers in the graph.
    ``value`` always returns the state stored at the *start* of the tick
    (i.e. the previous tick's result), so downstream nodes that read
    ``value`` are independent of nodes that write ``set``/``clear``.

    This means the graph edge:
        LimitOrderGen.fired → BoolVariable.set → BoolVariable.value → LimitOrderGen.enabled

    is NOT a cycle - the Variable inserts a one-tick delay, exactly as
    a D flip-flop would in hardware.

    The topological sort sees Variable input ports (set/clear) as sinks and
    the output port (value) as a source that has no upstream dependencies
    within this tick.  We achieve this by:
      - Overriding ``evaluate()`` to NOT recursively resolve set/clear before
        returning ``value`` - value is read from ctx.variables immediately.
      - Deferring the set/clear write to a second pass scheduled via
        ctx._deferred (a list appended to by Variable nodes).
    The Graph.run_tick() method executes all deferred writes after the main
    evaluation pass.
    """

    CATEGORY = "variable"
    INPUT_PORTS = [
        input_port("set",   DType.BOOL, required=False, default=None,
                   doc="When True, flag is set to True."),
        input_port("clear", DType.BOOL, required=False, default=None,
                   doc="When True, flag is cleared to False."),
    ]
    OUTPUT_PORTS = [
        output_port("value", DType.BOOL),
        output_port("rose",  DType.BOOL, doc="True on the tick the flag became True"),
        output_port("fell",  DType.BOOL, doc="True on the tick the flag became False"),
    ]

    def __init__(self, initial: bool = False, **kwargs) -> None:
        super().__init__(**kwargs)
        self.initial = initial

    def evaluate(self, ctx: Context) -> dict[str, Any]:
        """
        Override: return the stored (prev-tick) value immediately without
        resolving upstream set/clear inputs.  The write is deferred.
        """
        if ctx.has_result(self.node_id):
            return ctx.get_result(self.node_id)

        current = ctx.get_var(self.node_id, self.initial)
        # We don't know rose/fell yet (depends on writes that haven't happened)
        # Emit preliminary result with no transition flags - deferred write will
        # update ctx.variables for *next* tick.
        result = {"value": current, "rose": False, "fell": False}
        ctx.set_result(self.node_id, result)

        # Schedule the deferred write: resolve set/clear after main pass
        if not hasattr(ctx, "_deferred"):
            ctx._deferred = []
        ctx._deferred.append(self._deferred_write)

        return result

    def _deferred_write(self, ctx: Context) -> None:
        """Called after main evaluation pass - resolve set/clear and persist."""
        current = ctx.get_var(self.node_id, self.initial)
        prev    = current

        set_val   = None
        clear_val = None

        if "set" in self._wiring:
            src_node, src_out = self._wiring["set"]
            set_val = src_node.evaluate(ctx).get(src_out)

        if "clear" in self._wiring:
            src_node, src_out = self._wiring["clear"]
            clear_val = src_node.evaluate(ctx).get(src_out)

        if set_val is True:
            current = True
        if clear_val is True:
            current = False

        ctx.set_var(self.node_id, current)
        # Patch the already-cached result with transition flags
        cached = ctx.get_result(self.node_id)
        cached["rose"] = (not prev) and current
        cached["fell"] = prev and (not current)

    def _compute(self, ctx: Context, inputs: dict[str, Any]) -> dict[str, Any]:
        # Never called - evaluate() is overridden
        raise NotImplementedError


# ===========================================================================
# Math nodes - arithmetic, expression, clamping
# ===========================================================================

_BINARY_OPS: dict[str, Callable] = {
    "add": operator.add,
    "sub": operator.sub,
    "mul": operator.mul,
    "div": operator.truediv,
    "mod": operator.mod,
    "pow": operator.pow,
    "min": min,
    "max": max,
}


class BinaryOp(Node):
    """
    Two-input arithmetic operation.
    op: one of add | sub | mul | div | mod | pow | min | max
    """

    CATEGORY = "math"
    INPUT_PORTS = [
        input_port("a", DType.FLOAT),
        input_port("b", DType.FLOAT),
    ]
    OUTPUT_PORTS = [output_port("result", DType.FLOAT)]

    def __init__(self, op: str = "add", **kwargs) -> None:
        if op not in _BINARY_OPS:
            raise ValueError(f"Unknown op {op!r}. Choose from {list(_BINARY_OPS)}")
        super().__init__(**kwargs)
        self.op    = op
        self.label = self.label or op
        self._fn   = _BINARY_OPS[op]

    def _compute(self, ctx: Context, inputs: dict[str, Any]) -> dict[str, Any]:
        a, b = inputs["a"], inputs["b"]
        try:
            return {"result": self._fn(a, b)}
        except ZeroDivisionError:
            return {"result": float("nan")}

    def to_dict(self) -> dict:
        d = super().to_dict()
        d["params"] = {"op": self.op}
        return d


class UnaryOp(Node):
    """Unary math: abs | neg | sqrt | log | exp | floor | ceil | sign"""

    CATEGORY = "math"
    _OPS = {
        "abs":   abs,
        "neg":   operator.neg,
        "sqrt":  math.sqrt,
        "log":   math.log,
        "exp":   math.exp,
        "floor": math.floor,
        "ceil":  math.ceil,
        "sign":  lambda x: math.copysign(1.0, x),
    }
    INPUT_PORTS  = [input_port("x", DType.FLOAT)]
    OUTPUT_PORTS = [output_port("result", DType.FLOAT)]

    def __init__(self, op: str = "abs", **kwargs) -> None:
        if op not in self._OPS:
            raise ValueError(f"Unknown op {op!r}")
        super().__init__(**kwargs)
        self.op  = op
        self._fn = self._OPS[op]

    def _compute(self, ctx: Context, inputs: dict[str, Any]) -> dict[str, Any]:
        try:
            return {"result": self._fn(inputs["x"])}
        except (ValueError, ZeroDivisionError):
            return {"result": float("nan")}

    def to_dict(self) -> dict:
        d = super().to_dict()
        d["params"] = {"op": self.op}
        return d


class Clamp(Node):
    """Clamp a value to [lo, hi]. lo and hi can be wired or fixed."""

    CATEGORY = "math"
    INPUT_PORTS = [
        input_port("x",  DType.FLOAT),
        input_port("lo", DType.FLOAT, required=False, default=0.0),
        input_port("hi", DType.FLOAT, required=False, default=1.0),
    ]
    OUTPUT_PORTS = [output_port("result", DType.FLOAT)]

    def _compute(self, ctx: Context, inputs: dict[str, Any]) -> dict[str, Any]:
        return {"result": max(inputs["lo"], min(inputs["hi"], inputs["x"]))}


class Select(Node):
    """
    Multiplexer: outputs ``a`` when ``condition`` is True, else ``b``.
    Float variant - for routing control flow use BranchGate.
    """

    CATEGORY = "math"
    INPUT_PORTS = [
        input_port("condition", DType.BOOL),
        input_port("a",         DType.FLOAT, doc="Value when True"),
        input_port("b",         DType.FLOAT, doc="Value when False"),
    ]
    OUTPUT_PORTS = [output_port("result", DType.FLOAT)]

    def _compute(self, ctx: Context, inputs: dict[str, Any]) -> dict[str, Any]:
        return {"result": inputs["a"] if inputs["condition"] else inputs["b"]}


# ===========================================================================
# Condition nodes - boolean predicates
# ===========================================================================

_CMP_OPS = {
    "gt": operator.gt,
    "ge": operator.ge,
    "lt": operator.lt,
    "le": operator.le,
    "eq": operator.eq,
    "ne": operator.ne,
}


class Compare(Node):
    """
    Compare a to b using: gt | ge | lt | le | eq | ne
    Produces a bool result.
    """

    CATEGORY = "condition"
    INPUT_PORTS = [
        input_port("a",  DType.FLOAT),
        input_port("b",  DType.FLOAT),
    ]
    OUTPUT_PORTS = [output_port("result", DType.BOOL)]

    def __init__(self, op: str = "gt", **kwargs) -> None:
        if op not in _CMP_OPS:
            raise ValueError(f"Unknown comparison {op!r}")
        super().__init__(**kwargs)
        self.op  = op
        self._fn = _CMP_OPS[op]

    def _compute(self, ctx: Context, inputs: dict[str, Any]) -> dict[str, Any]:
        return {"result": self._fn(inputs["a"], inputs["b"])}

    def to_dict(self) -> dict:
        d = super().to_dict()
        d["params"] = {"op": self.op}
        return d


class Threshold(Node):
    """
    Hysteresis threshold with separate entry/exit levels.
    Avoids rapid flip-flopping around a single threshold.

    State machine:
      BELOW → ABOVE when value > upper_threshold
      ABOVE → BELOW when value < lower_threshold
    """

    CATEGORY = "condition"
    INPUT_PORTS = [
        input_port("value",           DType.FLOAT),
        input_port("upper_threshold", DType.FLOAT, required=False, default=1.0),
        input_port("lower_threshold", DType.FLOAT, required=False, default=0.0),
    ]
    OUTPUT_PORTS = [
        output_port("above",      DType.BOOL, doc="Currently above upper threshold"),
        output_port("crossed_up", DType.BOOL, doc="Crossed upward this tick"),
        output_port("crossed_dn", DType.BOOL, doc="Crossed downward this tick"),
    ]

    def _compute(self, ctx: Context, inputs: dict[str, Any]) -> dict[str, Any]:
        prev_above = ctx.get_var(self.node_id, False)
        v   = inputs["value"]
        hi  = inputs["upper_threshold"]
        lo  = inputs["lower_threshold"]

        if prev_above:
            above = v >= lo          # stay above until we fall below lo
        else:
            above = v > hi           # only cross up if we clear hi

        ctx.set_var(self.node_id, above)
        return {
            "above":      above,
            "crossed_up": above and not prev_above,
            "crossed_dn": not above and prev_above,
        }


class InRange(Node):
    """True when lo <= value <= hi (inclusive, both ends wirable)."""

    CATEGORY = "condition"
    INPUT_PORTS = [
        input_port("value", DType.FLOAT),
        input_port("lo",    DType.FLOAT, required=False, default=0.0),
        input_port("hi",    DType.FLOAT, required=False, default=1.0),
    ]
    OUTPUT_PORTS = [output_port("result", DType.BOOL)]

    def _compute(self, ctx: Context, inputs: dict[str, Any]) -> dict[str, Any]:
        return {"result": inputs["lo"] <= inputs["value"] <= inputs["hi"]}


class LogicGate(Node):
    """
    N-ary boolean logic: and | or | xor | nand | nor | not
    'not' only uses input ``a`` (``b`` ignored).
    """

    CATEGORY = "condition"
    INPUT_PORTS = [
        input_port("a", DType.BOOL),
        input_port("b", DType.BOOL, required=False, default=False),
    ]
    OUTPUT_PORTS = [output_port("result", DType.BOOL)]

    _OPS = {
        "and":  lambda a, b: a and b,
        "or":   lambda a, b: a or b,
        "xor":  lambda a, b: a ^ b,
        "nand": lambda a, b: not (a and b),
        "nor":  lambda a, b: not (a or b),
        "not":  lambda a, b: not a,
    }

    def __init__(self, op: str = "and", **kwargs) -> None:
        if op not in self._OPS:
            raise ValueError(f"Unknown logic op {op!r}")
        super().__init__(**kwargs)
        self.op  = op
        self._fn = self._OPS[op]

    def _compute(self, ctx: Context, inputs: dict[str, Any]) -> dict[str, Any]:
        return {"result": self._fn(inputs["a"], inputs["b"])}

    def to_dict(self) -> dict:
        d = super().to_dict()
        d["params"] = {"op": self.op}
        return d


class Cooldown(Node):
    """
    Edge-triggered one-shot with cooldown timer.
    When ``trigger`` rises, emits ``fire`` = True for one tick, then
    blocks further fires for ``n_ticks`` ticks.

    Useful for preventing order storms: "fire at most once every N ticks."
    """

    CATEGORY = "condition"
    INPUT_PORTS = [
        input_port("trigger",  DType.BOOL),
    ]
    OUTPUT_PORTS = [
        output_port("fire",       DType.BOOL, doc="True on the single fire tick"),
        output_port("in_cooldown", DType.BOOL),
        output_port("ticks_left", DType.FLOAT),
    ]

    def __init__(self, n_ticks: int = 10, **kwargs) -> None:
        super().__init__(**kwargs)
        self.n_ticks = n_ticks
        self._rem_key = f"{self.node_id}__rem"

    def _compute(self, ctx: Context, inputs: dict[str, Any]) -> dict[str, Any]:
        rem = ctx.get_var(self._rem_key, 0)

        if rem > 0:
            ctx.set_var(self._rem_key, rem - 1)
            return {"fire": False, "in_cooldown": True, "ticks_left": float(rem)}

        if inputs["trigger"]:
            ctx.set_var(self._rem_key, self.n_ticks)
            return {"fire": True, "in_cooldown": False, "ticks_left": 0.0}

        return {"fire": False, "in_cooldown": False, "ticks_left": 0.0}

    def to_dict(self) -> dict:
        d = super().to_dict()
        d["params"] = {"n_ticks": self.n_ticks}
        return d


# ===========================================================================
# Branch nodes - conditional signal routing
# ===========================================================================

class BranchGate(Node):
    """
    Routes a float value through only when ``gate`` is True.
    When False, emits the ``default_value`` on the ``true_out`` port
    and the live value on ``false_out``.

    Think of it as: if gate → pass value on true_out, else on false_out.
    Downstream nodes wire to whichever output they care about.
    """

    CATEGORY = "branch"
    INPUT_PORTS = [
        input_port("value",         DType.FLOAT),
        input_port("gate",          DType.BOOL),
        input_port("default_value", DType.FLOAT, required=False, default=float("nan")),
    ]
    OUTPUT_PORTS = [
        output_port("true_out",  DType.FLOAT, doc="Value when gate is True"),
        output_port("false_out", DType.FLOAT, doc="Value when gate is False"),
        output_port("active",    DType.BOOL,  doc="Mirrors gate"),
    ]

    def _compute(self, ctx: Context, inputs: dict[str, Any]) -> dict[str, Any]:
        v   = inputs["value"]
        g   = inputs["gate"]
        dflt = inputs["default_value"]
        return {
            "true_out":  v    if g else dflt,
            "false_out": dflt if g else v,
            "active":    g,
        }


class StateMachine(Node):
    """
    Simple N-state machine driven by boolean transition signals.

    States are integers 0..n_states-1.
    ``transitions`` is a list of (from_state, to_state) pairs; the
    corresponding input ports are named "t0", "t1", … Each port accepts
    a bool; when True this tick, the machine transitions.

    Multiple transitions can be True in the same tick - first match wins
    (transitions evaluated in order).

    Outputs:
      state       - current int state (0-based)
      state_*     - one bool port per state, True when in that state
      changed     - True on the tick the state changed
    """

    CATEGORY = "branch"

    def __init__(
        self,
        n_states: int = 2,
        transitions: Optional[list[tuple[int, int]]] = None,
        initial_state: int = 0,
        state_labels: Optional[list[str]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.n_states      = n_states
        self.transitions   = transitions or []
        self.initial_state = initial_state
        self.state_labels  = state_labels or [f"s{i}" for i in range(n_states)]

        self.INPUT_PORTS = [
            input_port(f"t{i}", DType.BOOL, required=False, default=False,
                       doc=f"Transition {fr}→{to}")
            for i, (fr, to) in enumerate(self.transitions)
        ]
        self.OUTPUT_PORTS = [
            output_port("state",   DType.INT),
            output_port("changed", DType.BOOL),
        ] + [
            output_port(f"in_{lbl}", DType.BOOL, doc=f"Currently in state {lbl}")
            for lbl in self.state_labels
        ]

    def _compute(self, ctx: Context, inputs: dict[str, Any]) -> dict[str, Any]:
        current = ctx.get_var(self.node_id, self.initial_state)
        next_s  = current

        for i, (fr, to) in enumerate(self.transitions):
            if inputs.get(f"t{i}", False) and current == fr:
                next_s = to
                break

        ctx.set_var(self.node_id, next_s)

        result: dict[str, Any] = {
            "state":   next_s,
            "changed": next_s != current,
        }
        for i, lbl in enumerate(self.state_labels):
            result[f"in_{lbl}"] = (next_s == i)
        return result

    def to_dict(self) -> dict:
        d = super().to_dict()
        d["params"] = {
            "n_states":      self.n_states,
            "transitions":   self.transitions,
            "initial_state": self.initial_state,
            "state_labels":  self.state_labels,
        }
        return d


# ===========================================================================
# Aggregator nodes - rolling window statistics
# ===========================================================================

class RollingWindow(Node):
    """
    Maintains a fixed-length FIFO buffer of float values.
    Emits SMA, standard deviation, min, max, and the raw latest value.

    The window fills with the initial value on the first ``window`` ticks.
    """

    CATEGORY = "aggregator"
    INPUT_PORTS  = [input_port("value", DType.FLOAT)]
    OUTPUT_PORTS = [
        output_port("sma",   DType.FLOAT, doc="Simple moving average"),
        output_port("std",   DType.FLOAT, doc="Sample standard deviation"),
        output_port("min",   DType.FLOAT),
        output_port("max",   DType.FLOAT),
        output_port("value", DType.FLOAT, doc="Passthrough of current value"),
        output_port("full",  DType.BOOL,  doc="True once window is fully populated"),
    ]

    def __init__(self, window: int = 20, **kwargs) -> None:
        super().__init__(**kwargs)
        self.window   = window
        self._buf_key = f"{self.node_id}__buf"

    def _compute(self, ctx: Context, inputs: dict[str, Any]) -> dict[str, Any]:
        buf: deque = ctx.get_var(self._buf_key, None)
        if buf is None:
            buf = deque(maxlen=self.window)
            ctx.set_var(self._buf_key, buf)

        v = inputs["value"]
        if v is not None and not (isinstance(v, float) and math.isnan(v)):
            buf.append(v)

        n = len(buf)
        if n == 0:
            return {"sma": float("nan"), "std": float("nan"),
                    "min": float("nan"), "max": float("nan"),
                    "value": v, "full": False}

        sma = sum(buf) / n
        std = math.sqrt(sum((x - sma) ** 2 for x in buf) / max(n - 1, 1))
        return {
            "sma":   sma,
            "std":   std,
            "min":   min(buf),
            "max":   max(buf),
            "value": v,
            "full":  n == self.window,
        }

    def to_dict(self) -> dict:
        d = super().to_dict()
        d["params"] = {"window": self.window}
        return d


class EWMA(Node):
    """
    Exponentially weighted moving average.
    alpha = 2 / (span + 1) following pandas convention.
    Alternatively, supply alpha directly.
    """

    CATEGORY = "aggregator"
    INPUT_PORTS  = [input_port("value", DType.FLOAT)]
    OUTPUT_PORTS = [
        output_port("ewma",   DType.FLOAT),
        output_port("delta",  DType.FLOAT, doc="ewma - prev_ewma"),
        output_port("initialized", DType.BOOL),
    ]

    def __init__(
        self,
        span:  Optional[int]   = 20,
        alpha: Optional[float] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if alpha is not None:
            self.alpha = alpha
        elif span is not None:
            self.alpha = 2.0 / (span + 1)
        else:
            raise ValueError("Provide either span or alpha")
        self._prev_key = f"{self.node_id}__prev"
        self._init_key = f"{self.node_id}__init"

    def _compute(self, ctx: Context, inputs: dict[str, Any]) -> dict[str, Any]:
        v    = inputs["value"]
        prev = ctx.get_var(self._prev_key, None)
        init = ctx.get_var(self._init_key, False)

        if prev is None:
            new_ewma = v
            init     = True
        else:
            new_ewma = self.alpha * v + (1.0 - self.alpha) * prev

        delta = new_ewma - (prev or new_ewma)
        ctx.set_var(self._prev_key, new_ewma)
        ctx.set_var(self._init_key, True)

        return {"ewma": new_ewma, "delta": delta, "initialized": init}

    def to_dict(self) -> dict:
        d = super().to_dict()
        d["params"] = {"alpha": self.alpha}
        return d


class Crossover(Node):
    """
    Detects when signal ``a`` crosses signal ``b``.
    Classic use: fast MA crosses slow MA.
    """

    CATEGORY = "aggregator"
    INPUT_PORTS = [
        input_port("a", DType.FLOAT, doc="Fast/primary signal"),
        input_port("b", DType.FLOAT, doc="Slow/reference signal"),
    ]
    OUTPUT_PORTS = [
        output_port("cross_up",   DType.BOOL, doc="a crossed above b this tick"),
        output_port("cross_down", DType.BOOL, doc="a crossed below b this tick"),
        output_port("a_above_b",  DType.BOOL, doc="a is currently above b"),
    ]

    def _compute(self, ctx: Context, inputs: dict[str, Any]) -> dict[str, Any]:
        a, b = inputs["a"], inputs["b"]
        prev_above = ctx.get_var(self.node_id, None)
        above = a > b

        cross_up   = above and (prev_above is False)
        cross_down = (not above) and (prev_above is True)

        ctx.set_var(self.node_id, above)
        return {"cross_up": cross_up, "cross_down": cross_down, "a_above_b": above}


# ===========================================================================
# OrderGen nodes - sinks that emit orders
# ===========================================================================

class LimitOrderGen(Node):
    """
    Emits a limit order when ``trigger`` is True.

    Price and size can be wired dynamically or set as fixed params.
    A BoolVariable tracking 'position_open' is the canonical way to
    prevent duplicate orders - connect its value to the ``enabled`` port.
    """

    CATEGORY = "order"
    INPUT_PORTS = [
        input_port("trigger",  DType.BOOL),
        input_port("price",    DType.FLOAT),
        input_port("size",     DType.FLOAT),
        input_port("enabled",  DType.BOOL,  required=False, default=True,
                   doc="Additional gate. Order only emits when both trigger and enabled are True."),
    ]
    OUTPUT_PORTS = [
        output_port("fired", DType.BOOL, doc="True on the tick an order was emitted"),
    ]

    def __init__(self, side: str = "buy", symbol: str = "SIM", tag: Optional[str] = None, **kwargs) -> None:
        super().__init__(**kwargs)
        assert side in ("buy", "sell")
        self.side   = side
        self.symbol = symbol
        self.tag    = tag

    def _compute(self, ctx: Context, inputs: dict[str, Any]) -> dict[str, Any]:
        if inputs["trigger"] and inputs["enabled"]:
            order = Order(
                symbol     = self.symbol,
                side       = OrderSide(self.side),
                order_type = OrderType.LIMIT,
                price      = inputs["price"],
                size       = inputs["size"],
                tag        = self.tag or self.label,
            )
            ctx.emit_order(order)
            return {"fired": True}
        return {"fired": False}

    def to_dict(self) -> dict:
        d = super().to_dict()
        d["params"] = {"side": self.side, "symbol": self.symbol, "tag": self.tag}
        return d


class MarketOrderGen(Node):
    """Emits a market order (no price) when trigger fires."""

    CATEGORY = "order"
    INPUT_PORTS = [
        input_port("trigger", DType.BOOL),
        input_port("size",    DType.FLOAT),
        input_port("enabled", DType.BOOL, required=False, default=True),
    ]
    OUTPUT_PORTS = [output_port("fired", DType.BOOL)]

    def __init__(self, side: str = "buy", symbol: str = "SIM", **kwargs) -> None:
        super().__init__(**kwargs)
        assert side in ("buy", "sell")
        self.side   = side
        self.symbol = symbol

    def _compute(self, ctx: Context, inputs: dict[str, Any]) -> dict[str, Any]:
        if inputs["trigger"] and inputs["enabled"]:
            ctx.emit_order(Order(
                symbol     = self.symbol,
                side       = OrderSide(self.side),
                order_type = OrderType.MARKET,
                size       = inputs["size"],
                tag        = self.label,
            ))
            return {"fired": True}
        return {"fired": False}

    def to_dict(self) -> dict:
        d = super().to_dict()
        d["params"] = {"side": self.side, "symbol": self.symbol}
        return d


class CancelOrderGen(Node):
    """
    Emits a cancel instruction for a given order_id when trigger fires.
    order_id is read from a Variable node (written by the exchange on fill callback).
    """

    CATEGORY = "order"
    INPUT_PORTS = [
        input_port("trigger",  DType.BOOL),
        input_port("order_id", DType.FLOAT, doc="Order ID to cancel (float-encoded int)"),
        input_port("enabled",  DType.BOOL, required=False, default=True),
    ]
    OUTPUT_PORTS = [output_port("fired", DType.BOOL)]

    def __init__(self, symbol: str = "SIM", **kwargs) -> None:
        super().__init__(**kwargs)
        self.symbol = symbol

    def _compute(self, ctx: Context, inputs: dict[str, Any]) -> dict[str, Any]:
        if inputs["trigger"] and inputs["enabled"]:
            ctx.emit_order(Order(
                symbol     = self.symbol,
                side       = OrderSide.BUY,   # irrelevant for cancel
                order_type = OrderType.CANCEL,
                size       = 0.0,
                order_id   = str(int(inputs["order_id"])),
                tag        = self.label,
            ))
            return {"fired": True}
        return {"fired": False}


class QuoteGen(Node):
    """
    Market-making convenience node.
    Emits a bid and an ask simultaneously, offset from mid by a half-spread.

    bid_price = mid - half_spread
    ask_price = mid + half_spread
    """

    CATEGORY = "order"
    INPUT_PORTS = [
        input_port("trigger",     DType.BOOL),
        input_port("mid",         DType.FLOAT),
        input_port("half_spread", DType.FLOAT),
        input_port("size",        DType.FLOAT),
        input_port("enabled",     DType.BOOL, required=False, default=True),
    ]
    OUTPUT_PORTS = [
        output_port("fired",     DType.BOOL),
        output_port("bid_price", DType.FLOAT),
        output_port("ask_price", DType.FLOAT),
    ]

    def __init__(self, symbol: str = "SIM", **kwargs) -> None:
        super().__init__(**kwargs)
        self.symbol = symbol

    def _compute(self, ctx: Context, inputs: dict[str, Any]) -> dict[str, Any]:
        mid  = inputs["mid"]
        half = inputs["half_spread"]
        bid  = mid - half
        ask  = mid + half

        if inputs["trigger"] and inputs["enabled"]:
            ctx.emit_order(Order(self.symbol, OrderSide.BUY,  OrderType.LIMIT, inputs["size"], bid,  tag=f"{self.label}_bid"))
            ctx.emit_order(Order(self.symbol, OrderSide.SELL, OrderType.LIMIT, inputs["size"], ask,  tag=f"{self.label}_ask"))
            return {"fired": True, "bid_price": bid, "ask_price": ask}

        return {"fired": False, "bid_price": bid, "ask_price": ask}

    def to_dict(self) -> dict:
        d = super().to_dict()
        d["params"] = {"symbol": self.symbol}
        return d
