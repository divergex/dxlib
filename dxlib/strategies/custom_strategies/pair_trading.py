import numpy as np
import pandas as pd
from pandas import MultiIndex

from ...core import History, Strategy, Inventory, Signal, Side, SchemaLevel


class PairTradingStrategy(Strategy):
    """
    A strategy that generates buy/sell signals based on the spread between two correlated securities.

    Parameters:
    - window (int): Number of days to calculate the rolling spread.
    - entry_z (float): Z-score threshold to enter a trade.
    - exit_z (float): Z-score threshold to exit a trade.

    Methods:
    - fit(history): Calculate the spread and identify trading signals.
    - execute(row, idx, history) -> dict: Generate trading signals based on the spread.
    """

    def __init__(self,
                 pairs: tuple,
                 field="close",
                 window=20,
                 entry_z=2.0,
                 exit_z=0.5,
                 ):
        super().__init__()
        self.security1, self.security2 = pairs
        self.field = field
        self.window = window
        self.entry_z = entry_z
        self.exit_z = exit_z
        self.quantity = 1

    def set(self, quantity: int):
        self.quantity = quantity

    def execute(
            self, observation: any, history: History, position: Inventory = None,
    ) -> pd.Series:  # pd.Series[TradeSignal]
        """
        Generate trading signals based on the spread between two securities.

        Args:
        - row (pd.Series): Latest row of equity prices.
        - idx (int): Index of the current row.
        - history (pd.DataFrame): Historical price data of multiple equities.

        Returns:
        dict: Trading signals for each equity.
        """
        levels = history.levels_unique()
        idx, _ = observation
        signals = pd.Series(
            Signal(Side.WAIT), index=MultiIndex.from_tuples([idx], names=levels.keys())
        )

        # Get historical data for the two securities
        df1 = history.get_df({SchemaLevel.SECURITY: [self.security1]}, [self.field])
        df2 = history.get_df({SchemaLevel.SECURITY: [self.security2]}, [self.field])

        if len(df1) > self.window and len(df2) > self.window:
            # Calculate the spread, cant just subtract since indices might not match
            n = np.minimum(len(df1), len(df2))
            spread = pd.DataFrame(df1.iloc[-n:][self.field].values, df2.iloc[-n:][self.field].values)

            if len(spread) < self.window:
                return signals

            mean_spread = spread.rolling(self.window).mean().iloc[-1]
            std_spread = spread.rolling(self.window).std().fillna(1).iloc[-1]

            # Calculate the z-score of the current spread
            current_spread = spread.iloc[-1]
            z_score = (current_spread - mean_spread) / (std_spread + 1e-6)

            # Generate signals based on z-score
            if (z_score > self.entry_z).all():
                signals.loc[idx] = Signal(Side.SELL, self.quantity)
                signals.loc[idx, self.security2] = Signal(Side.BUY, self.quantity)
            elif (z_score < -self.entry_z).all():
                signals.loc[idx] = Signal(Side.BUY, self.quantity)
                signals.loc[idx, self.security2] = Signal(Side.SELL, self.quantity)
            elif (abs(z_score) < self.exit_z).all():
                pass
        return signals
