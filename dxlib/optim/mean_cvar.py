import numpy as np
import pandas as pd
from scipy.optimize import minimize


def portfolio_returns(weights, returns):
    return np.dot(weights, returns.mean(axis=0))


def portfolio_losses(weights, returns, alpha):
    portfolio_return = np.dot(weights, returns.T)
    losses = np.percentile(portfolio_return, 100 * alpha)
    return -np.mean(np.where(portfolio_return < losses, portfolio_return - losses, 0))


def mean_cvar_optimization(returns, alpha=0.05):
    """
    Perform mean-CVaR portfolio optimization. Minimize the negative CVaR (expected shortfall) of the portfolio.
    This is a convex optimization problem, and an extent to the Markowitz model.

    Args:
        returns: (pd.DataFrame) Expected returns of the assets
        alpha: (float) Confidence level for CVaR

    Returns:
        Vector of optimal weights
    """
    num_assets = returns.shape[1]

    def objective(weights):
        return -portfolio_returns(weights, returns) + portfolio_losses(weights, returns, alpha)

    constraints = ({'type': 'eq', 'fun': lambda weights: np.sum(weights) - 1})
    bounds = [(0, 1) for _ in range(num_assets)]
    initial_guess = np.array(num_assets * [1. / num_assets])

    result = minimize(objective, initial_guess, method='SLSQP', bounds=bounds, constraints=constraints)

    return result.x


# Example data
np.random.seed(42)
num_assets = 5
num_periods = 1000
# Simulated returns data (usar valores reais, pós inserção do à posteriori do black_litterman.py)
returns = np.random.randn(num_periods, num_assets) / 100

# Optimize portfolio
alpha = 0.05  # 5% level for CVaR <- esse valor é arbitrário e significa que estamos otimizando para o pior 5% dos casos
optimal_weights = mean_cvar_optimization(pd.DataFrame(returns), alpha)
print("Optimal Weights:", optimal_weights)
