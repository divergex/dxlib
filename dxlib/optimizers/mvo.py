from abc import ABC, abstractmethod
from typing import Literal, Tuple

import numpy as np
import numpy.linalg as linalg
import pandas as pd


class PortfolioOptimizer(ABC):
    @abstractmethod
    def optimize(self, *args, **kwargs):
        pass

    def __call__(self, *args, **kwargs) -> Tuple:
        return self.optimize(*args, **kwargs)


class Mvo(PortfolioOptimizer):
    def __init__(self, target: Literal["risk_aversion", "target_return"] = "risk_aversion"):
        if target == "risk_aversion":
            self._optim = self._optim_target_return
        else:
            self._optim = self._optim_risk_aversion

    def optimize(self, returns: pd.Series, covariance: np.array, param: float):
        weights, params = self._optim(returns.to_numpy(), covariance, param)
        return pd.Series({
            returns.index[i]: weights[i] for i in range(len(covariance))
        }), params

    def _optim_risk_aversion(self, returns: np.array, covariance: np.array, gamma: float) -> Tuple[np.array, Tuple]:
        n = len(returns)
        mat = np.vstack([
            np.hstack([gamma * covariance, np.ones((n, 1))]),
            np.hstack([np.ones((1, n)), np.array([[0]])])
        ])
        rhs = np.concatenate([returns, [1]])
        solution = linalg.solve(mat, rhs)
        w, lmbda = solution[:n], solution[n]
        return w, (lmbda, None)

    ### Portf
    def _optim_target_return(self, returns: np.array, covariance: np.array, mu):
        """
        Optimizes a portfolio using return estimates and covariance matrix.
        """
        n = len(returns)
        mat = np.vstack([
            np.hstack([2 * covariance, -returns.reshape(-1, 1), np.ones((n, 1))]),
            np.hstack([returns.reshape(1, -1), np.array([[0, 0]])]),
            np.hstack([np.ones((1, n)), np.array([[0, 0]])])
        ])
        rhs = np.concatenate([np.zeros(n), [mu], [1]])
        solution = linalg.solve(mat, rhs)
        w, lmbda, vega = solution[:n], solution[n], solution[n + 1]
        return w, (lmbda, vega)


if __name__ == '__main__':
    n = 3
    r = np.array(
        [0.1, 0.17, 0.15]
    )
    np.random.seed(693)
    A = np.random.randn(n, n)
    cov = A.T @ A
    eps = 1e-3
    cov += eps * np.eye(n)
    optim = Mvo()
    w, lmbda, vega = optim(r, cov, 0.2)
    print(w)
