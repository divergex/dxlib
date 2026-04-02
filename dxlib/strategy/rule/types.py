"""
Primitive types and port descriptors for the strategy rule engine.

A Port is a typed, directional connector between nodes. Edges in the graph
connect one output Port to one input Port of a compatible dtype.

Supported dtypes mirror what the LOB and order system natively produce:
  - float       : prices, sizes, ratios, indicators
  - bool        : condition results, gate signals
  - int         : integer counts, order IDs
  - str         : symbol, side literals
  - LOBSnapshot : full order book state at a tick
  - Order       : a generated order (sink input only)
  - Any         : untyped passthrough (use sparingly)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Optional, Type, Union
import time


# ---------------------------------------------------------------------------
# Dtype registry
# ---------------------------------------------------------------------------

class DType(Enum):
    FLOAT       = auto()
    BOOL        = auto()
    INT         = auto()
    STR         = auto()
    LOB         = auto()   # LOBSnapshot
    ORDER       = auto()   # Order struct (sink only)
    ANY         = auto()   # untyped passthrough

    def compatible_with(self, other: "DType") -> bool:
        """
        Type compatibility for edge validation.
        ANY is compatible with everything (both directions).
        """
        if self == DType.ANY or other == DType.ANY:
            return True
        return self == other


# ---------------------------------------------------------------------------
# LOB snapshot (exchange-agnostic view)
# ---------------------------------------------------------------------------

@dataclass
class Level:
    price: float
    size: float


@dataclass
class LOBSnapshot:
    """
    Immutable view of the limit order book at a single tick.

    Levels are sorted: bids descending, asks ascending.
    Provides derived accessors so nodes don't need to recompute them.
    """
    symbol:    str
    timestamp: float              # unix seconds (float for sub-second)
    bids:      list[Level]        # best bid first
    asks:      list[Level]        # best ask first
    last_trade_price: Optional[float] = None
    last_trade_size:  Optional[float] = None

    # ------------------------------------------------------------------
    # Derived accessors
    # ------------------------------------------------------------------

    @property
    def best_bid(self) -> Optional[float]:
        return self.bids[0].price if self.bids else None

    @property
    def best_ask(self) -> Optional[float]:
        return self.asks[0].price if self.asks else None

    @property
    def mid(self) -> Optional[float]:
        if self.best_bid is None or self.best_ask is None:
            return None
        return (self.best_bid + self.best_ask) / 2.0

    @property
    def spread(self) -> Optional[float]:
        if self.best_bid is None or self.best_ask is None:
            return None
        return self.best_ask - self.best_bid

    @property
    def bid_depth(self) -> float:
        """Total size across all bid levels."""
        return sum(l.size for l in self.bids)

    @property
    def ask_depth(self) -> float:
        return sum(l.size for l in self.asks)

    @property
    def imbalance(self) -> Optional[float]:
        """
        Order book imbalance: (bid_depth - ask_depth) / (bid_depth + ask_depth).
        Range [-1, 1]. Positive = more buying pressure.
        """
        total = self.bid_depth + self.ask_depth
        if total == 0:
            return None
        return (self.bid_depth - self.ask_depth) / total

    def bid_at(self, level: int) -> Optional[Level]:
        return self.bids[level] if level < len(self.bids) else None

    def ask_at(self, level: int) -> Optional[Level]:
        return self.asks[level] if level < len(self.asks) else None

    def vwap(self, n_levels: int = 5) -> Optional[float]:
        """Volume-weighted average price across top n levels, both sides."""
        levels = self.bids[:n_levels] + self.asks[:n_levels]
        total_size = sum(l.size for l in levels)
        if total_size == 0:
            return None
        return sum(l.price * l.size for l in levels) / total_size


# ---------------------------------------------------------------------------
# Order (emitted by OrderGen nodes)
# ---------------------------------------------------------------------------

class OrderSide(str, Enum):
    BUY  = "buy"
    SELL = "sell"

class OrderType(str, Enum):
    LIMIT  = "limit"
    MARKET = "market"
    CANCEL = "cancel"

@dataclass
class Order:
    """
    Order instruction emitted by a sink node at evaluation time.

    The exchange simulation layer is responsible for assigning order IDs
    and handling fill logic — we just describe intent here.
    """
    symbol:     str
    side:       OrderSide
    order_type: OrderType
    size:       float
    price:      Optional[float] = None   # None for MARKET / CANCEL
    order_id:   Optional[str]  = None   # set by exchange on CANCEL
    tag:        Optional[str]  = None   # user-defined label for debugging


# ---------------------------------------------------------------------------
# Port descriptor
# ---------------------------------------------------------------------------

class PortDirection(Enum):
    INPUT  = "input"
    OUTPUT = "output"


@dataclass
class Port:
    """
    A single typed connector on a node.

    Ports are identified by (node_id, name).  The graph connects
    output ports to input ports of compatible dtype.

    `required` only applies to input ports — optional inputs that are
    not connected return their `default` during evaluation.
    """
    name:      str
    dtype:     DType
    direction: PortDirection
    required:  bool           = True
    default:   Any            = None
    doc:       str            = ""

    def __repr__(self) -> str:
        req = "" if self.required else "?"
        return f"Port({self.direction.value} {self.name}{req}: {self.dtype.name})"
