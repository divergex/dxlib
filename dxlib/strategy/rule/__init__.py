"""
rule — pull-based graph for creating rule-based trading strategies.

Public API
──────────
from rule import (
    # Types
    LOBSnapshot, Level, Order, OrderSide, OrderType, DType,

    # Base
    Node, Context, Graph, StrategyRunner,

    # Source nodes
    LOBSource, LevelExtractor, Constant, TickCounter,

    # Variable nodes
    Variable, BoolVariable,

    # Math nodes
    BinaryOp, UnaryOp, Clamp, Select,

    # Condition nodes
    Compare, Threshold, InRange, LogicGate, Cooldown,

    # Branch nodes
    BranchGate, StateMachine,

    # Aggregator nodes
    RollingWindow, EWMA, Crossover,

    # Order nodes
    LimitOrderGen, MarketOrderGen, CancelOrderGen, QuoteGen,
)
"""

from .types import (
    DType, Level, LOBSnapshot,
    Order, OrderSide, OrderType,
    Port, PortDirection,
)
from .node import Node, Context, input_port, output_port
from .nodes import (
    LOBSource, LevelExtractor, Constant, TickCounter,
    Variable, BoolVariable,
    BinaryOp, UnaryOp, Clamp, Select,
    Compare, Threshold, InRange, LogicGate, Cooldown,
    BranchGate, StateMachine,
    RollingWindow, EWMA, Crossover,
    LimitOrderGen, MarketOrderGen, CancelOrderGen, QuoteGen,
)
from .graph import Edge, Graph, StrategyRunner

__all__ = [
    # Types
    "DType", "Level", "LOBSnapshot",
    "Order", "OrderSide", "OrderType",
    "Port", "PortDirection",
    # Base
    "Node", "Context", "Graph", "Edge", "StrategyRunner",
    "input_port", "output_port",
    # Nodes
    "LOBSource", "LevelExtractor", "Constant", "TickCounter",
    "Variable", "BoolVariable",
    "BinaryOp", "UnaryOp", "Clamp", "Select",
    "Compare", "Threshold", "InRange", "LogicGate", "Cooldown",
    "BranchGate", "StateMachine",
    "RollingWindow", "EWMA", "Crossover",
    "LimitOrderGen", "MarketOrderGen", "CancelOrderGen", "QuoteGen",
]
