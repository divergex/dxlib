"""
Example strategies demonstrating the rule engine API.
Each function returns a StrategyRunner ready for the exchange simulation loop.

Strategies
──────────
1. momentum_crossover   - fast/slow EWMA crossover, long only
2. mean_reversion       - trade back to mid when spread is wide
3. market_maker         - continuous two-sided quoting with spread control
4. imbalance_momentum   - enter when LOB imbalance exceeds threshold
5. state_machine_mm     - market maker with explicit FLAT/LONG/SHORT states
"""
from dxlib.strategy.rule import (
    Graph, StrategyRunner,
    LOBSource, Constant,
     BoolVariable,
    BinaryOp,
    Compare, LogicGate, Cooldown,
    StateMachine,
    EWMA, RollingWindow, Crossover,
    LimitOrderGen, MarketOrderGen, QuoteGen,
    LOBSnapshot, Level
)

import logging

logging.basicConfig(level=logging.DEBUG)  # ensures a handler exists

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# ---------------------------------------------------------------------------
# 1. Momentum Crossover
#    Buy when fast EWMA crosses above slow EWMA (confirmed by positive mid delta).
#    Sell (market) when fast crosses back below slow.
#    One position at a time guarded by BoolVariable.
# ---------------------------------------------------------------------------

def momentum_crossover(symbol: str = "SIM") -> StrategyRunner:
    g = Graph(logger)

    src   = g.add(LOBSource(label="market"))
    fast  = g.add(EWMA(span=5,  label="fast_ewma"))
    slow  = g.add(EWMA(span=20, label="slow_ewma"))
    cross = g.add(Crossover(label="crossover"))

    # Gate: don't buy if already long; don't sell if already flat
    in_pos = g.add(BoolVariable(initial=False, label="in_position"))

    # Cooldown: no back-to-back orders within 5 ticks
    buy_cd  = g.add(Cooldown(n_ticks=5,  label="buy_cooldown"))
    sell_cd = g.add(Cooldown(n_ticks=5,  label="sell_cooldown"))

    # Invert in_position for buy gate
    not_in  = g.add(LogicGate(op="not", label="not_in_pos"))

    buy_ord  = g.add(LimitOrderGen(side="buy",  symbol=symbol, label="buy"))
    sell_ord = g.add(MarketOrderGen(side="sell", symbol=symbol, label="sell"))

    # Wiring
    g.connect(src,   "mid",        fast,    "value")
    g.connect(src,   "mid",        slow,    "value")
    g.connect(fast,  "ewma",       cross,   "a")
    g.connect(slow,  "ewma",       cross,   "b")

    # Buy trigger: cross_up AND not in position → cooldown gate
    g.connect(cross,  "cross_up",   buy_cd,  "trigger")
    g.connect(buy_cd, "fire",        buy_ord, "trigger")
    g.connect(in_pos, "value",       not_in,  "a")
    g.connect(not_in, "result",      buy_ord, "enabled")
    g.connect(src,    "best_ask",    buy_ord, "price")
    g.connect(Constant(1.0, label="buy_size"), "value", buy_ord, "size")

    # Sell trigger: cross_down AND in position
    g.connect(cross,  "cross_down",  sell_cd, "trigger")
    g.connect(sell_cd,"fire",        sell_ord,"trigger")
    g.connect(in_pos, "value",       sell_ord,"enabled")
    g.connect(Constant(1.0, label="sell_size"), "value", sell_ord, "size")

    # Position tracking: set on buy fire, clear on sell fire
    g.connect(buy_ord, "fired",  in_pos, "set")
    g.connect(sell_ord,"fired",  in_pos, "clear")

    # Register all Constant nodes that were created standalone
    for n in [
        Constant(1.0, label="buy_size"),
        Constant(1.0, label="sell_size"),
    ]:
        pass  # already added via connect above - Graph.connect adds them if missing

    return StrategyRunner(g, symbol=symbol, validate=True)


# ---------------------------------------------------------------------------
# 2. Mean Reversion
#    When spread widens beyond 2× its rolling average, fade the move:
#      - buy at best_bid when price is below rolling mean
#      - sell at best_ask when price is above rolling mean
# ---------------------------------------------------------------------------

def mean_reversion(symbol: str = "SIM", window: int = 30) -> StrategyRunner:
    g = Graph(logger)

    src    = g.add(LOBSource(label="market"))
    roll   = g.add(RollingWindow(window=window, label=f"roll_{window}"))
    spread_roll = g.add(RollingWindow(window=window, label="spread_roll"))

    # price below mean → buy signal
    below_mean = g.add(Compare(op="lt", label="price_below_mean"))
    # price above mean → sell signal
    above_mean = g.add(Compare(op="gt", label="price_above_mean"))

    # spread > 2 × mean spread → spread is wide
    two = g.add(Constant(2.0, label="two"))
    wide_thresh = g.add(BinaryOp(op="mul", label="2x_spread"))
    spread_wide = g.add(Compare(op="gt", label="spread_wide"))

    # Combine with wide spread condition
    buy_sig  = g.add(LogicGate(op="and", label="buy_signal"))
    sell_sig = g.add(LogicGate(op="and", label="sell_signal"))

    # Cooldowns
    buy_cd  = g.add(Cooldown(n_ticks=10, label="buy_cd"))
    sell_cd = g.add(Cooldown(n_ticks=10, label="sell_cd"))

    buy_ord  = g.add(LimitOrderGen(side="buy",  symbol=symbol))
    sell_ord = g.add(LimitOrderGen(side="sell", symbol=symbol))

    # Wiring
    g.connect(src,         "mid",    roll,        "value")
    g.connect(src,         "spread", spread_roll, "value")

    g.connect(src,         "mid",    below_mean,  "a")
    g.connect(roll,        "sma",    below_mean,  "b")

    g.connect(src,         "mid",    above_mean,  "a")
    g.connect(roll,        "sma",    above_mean,  "b")

    g.connect(spread_roll, "sma",    wide_thresh, "a")
    g.connect(two,         "value",  wide_thresh, "b")
    g.connect(src,         "spread", spread_wide, "a")
    g.connect(wide_thresh, "result", spread_wide, "b")

    g.connect(below_mean,  "result", buy_sig,     "a")
    g.connect(spread_wide, "result", buy_sig,     "b")
    g.connect(above_mean,  "result", sell_sig,    "a")
    g.connect(spread_wide, "result", sell_sig,    "b")

    g.connect(buy_sig,     "result", buy_cd,      "trigger")
    g.connect(sell_sig,    "result", sell_cd,     "trigger")

    g.connect(buy_cd,      "fire",   buy_ord,     "trigger")
    g.connect(sell_cd,     "fire",   sell_ord,    "trigger")

    g.connect(src,         "best_bid", buy_ord,   "price")
    g.connect(src,         "best_ask", sell_ord,  "price")

    size = g.add(Constant(1.0, label="size"))
    g.connect(size, "value", buy_ord,  "size")
    g.connect(size, "value", sell_ord, "size")

    return StrategyRunner(g, symbol=symbol)


# ---------------------------------------------------------------------------
# 3. Market Maker
#    Continuous two-sided quoting with dynamic spread derived from
#    rolling volatility (std of mid prices).
#    Quotes refreshed every tick.
# ---------------------------------------------------------------------------

def market_maker(
    symbol:      str   = "SIM",
    base_spread: float = 0.5,
    vol_window:  int   = 20,
    size:        float = 1.0,
) -> StrategyRunner:
    g = Graph(logger)

    src     = g.add(LOBSource(label="market"))
    vol     = g.add(RollingWindow(window=vol_window, label="vol"))
    base    = g.add(Constant(base_spread, label="base_spread"))
    sz      = g.add(Constant(size,        label="size"))
    always  = g.add(Constant(1.0,         label="always"))   # trigger hack

    # half_spread = max(base_spread, vol.std)
    half_spread = g.add(BinaryOp(op="max",  label="half_spread"))
    trigger     = g.add(Compare(op="ge",    label="always_true"))  # 1 >= 0

    zero = g.add(Constant(0.0, label="zero"))
    quot = g.add(QuoteGen(symbol=symbol, label="quotes"))

    g.connect(src,         "mid",    vol,         "value")
    g.connect(vol,         "std",    half_spread, "a")
    g.connect(base,        "value",  half_spread, "b")

    g.connect(always,      "value",  trigger,     "a")
    g.connect(zero,        "value",  trigger,     "b")

    g.connect(trigger,     "result", quot,        "trigger")
    g.connect(src,         "mid",    quot,        "mid")
    g.connect(half_spread, "result", quot,        "half_spread")
    g.connect(sz,          "value",  quot,        "size")

    return StrategyRunner(g, symbol=symbol)


# ---------------------------------------------------------------------------
# 4. State-machine market maker
#    Explicit states: FLAT(0) / LONG(1) / SHORT(2)
#    Transitions driven by imbalance and EWMA signals.
# ---------------------------------------------------------------------------

def state_machine_mm(symbol: str = "SIM") -> StrategyRunner:
    g = Graph(logger)

    src  = g.add(LOBSource(label="market"))
    ewma = g.add(EWMA(span=10, label="ewma"))

    # Signals
    price_rising = g.add(Compare(op="gt", label="price_rising"))
    price_fall   = g.add(Compare(op="lt", label="price_falling"))
    imb_pos      = g.add(Compare(op="gt", label="imb_positive"))
    imb_neg      = g.add(Compare(op="lt", label="imb_negative"))
    zero         = g.add(Constant(0.0, label="zero"))

    buy_sig  = g.add(LogicGate(op="and", label="buy_signal"))
    sell_sig = g.add(LogicGate(op="and", label="sell_signal"))

    # State machine: FLAT(0) → LONG(1) on buy_sig, FLAT(0) → SHORT(2) on sell_sig
    #                LONG(1) → FLAT on sell_sig, SHORT(2) → FLAT on buy_sig
    sm = g.add(StateMachine(
        n_states      = 3,
        transitions   = [(0,1),(0,2),(1,0),(2,0)],
        initial_state = 0,
        state_labels  = ["flat","long","short"],
        label         = "position_state",
    ))

    buy_cd  = g.add(Cooldown(n_ticks=5, label="buy_cd"))
    sell_cd = g.add(Cooldown(n_ticks=5, label="sell_cd"))
    buy     = g.add(LimitOrderGen(side="buy",  symbol=symbol))
    sell    = g.add(LimitOrderGen(side="sell", symbol=symbol))
    sz      = g.add(Constant(1.0, label="size"))

    # Signal wiring
    g.connect(src,   "mid",      ewma,        "value")
    g.connect(src,   "mid",      price_rising,"a")
    g.connect(ewma,  "ewma",     price_rising,"b")
    g.connect(src,   "mid",      price_fall,  "a")
    g.connect(ewma,  "ewma",     price_fall,  "b")
    g.connect(src,   "imbalance",imb_pos,     "a")
    g.connect(zero,  "value",    imb_pos,     "b")
    g.connect(src,   "imbalance",imb_neg,     "a")
    g.connect(zero,  "value",    imb_neg,     "b")

    g.connect(price_rising,"result",  buy_sig, "a")
    g.connect(imb_pos,     "result",  buy_sig, "b")
    g.connect(price_fall,  "result",  sell_sig,"a")
    g.connect(imb_neg,     "result",  sell_sig,"b")

    # Transitions
    g.connect(buy_sig, "result", sm, "t0")   # FLAT → LONG
    g.connect(sell_sig,"result", sm, "t1")   # FLAT → SHORT
    g.connect(sell_sig,"result", sm, "t2")   # LONG → FLAT
    g.connect(buy_sig, "result", sm, "t3")   # SHORT → FLAT

    # Orders fire on state transitions (sm.changed + in state)
    long_entry  = g.add(LogicGate(op="and", label="long_entry_gate"))
    short_entry = g.add(LogicGate(op="and", label="short_entry_gate"))

    g.connect(sm, "changed",  long_entry,  "a")
    g.connect(sm, "in_long",  long_entry,  "b")
    g.connect(sm, "changed",  short_entry, "a")
    g.connect(sm, "in_short", short_entry, "b")

    g.connect(long_entry,  "result", buy_cd,  "trigger")
    g.connect(short_entry, "result", sell_cd, "trigger")
    g.connect(buy_cd,  "fire",  buy,  "trigger")
    g.connect(sell_cd, "fire",  sell, "trigger")

    g.connect(src,  "best_ask", buy,  "price")
    g.connect(src,  "best_bid", sell, "price")
    g.connect(sz,   "value",    buy,  "size")
    g.connect(sz,   "value",    sell, "size")

    return StrategyRunner(g, symbol=symbol)


# ---------------------------------------------------------------------------
# Quick smoke test
# ---------------------------------------------------------------------------
def test_rule_strategy():
    import random, math


    def fake_lob(t: int) -> "LOBSnapshot":
        mid = 100.0 + 2 * math.sin(t / 20) + random.gauss(0, 0.1)
        spread = 0.2 + abs(random.gauss(0, 0.05))
        return LOBSnapshot(
            symbol    = "SIM",
            timestamp = float(t),
            bids = [Level(mid - spread/2 - i*0.1, 10 - i) for i in range(5)],
            asks = [Level(mid + spread/2 + i*0.1, 10 - i) for i in range(5)],
        )

    strategies = {
        "mean_reversion":       mean_reversion(),
        "market_maker":         market_maker(),
        "state_machine":        state_machine_mm()
    }

    print("Running 100 ticks per strategy...\n")
    for name, runner in strategies.items():
        total_orders = 0
        for t in range(100):
            orders = runner.step(fake_lob(t))
            total_orders += len(orders)
        print(f"  {name:25s} - {total_orders:3d} orders over 100 ticks")
