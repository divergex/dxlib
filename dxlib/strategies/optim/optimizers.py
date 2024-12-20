import numpy as np
import pandas as pd

from scipy.optimize import minimize
from scipy import stats


def mean_variance_optim(mu, sigma, gamma=1, max_stocks=None, var_bound=None, conf=0.95):
    """
    Perform mean-variance optimization without cvxpy.

    Args:
        mu (Series): Expected returns
        sigma (DataFrame): Covariance matrix of assets
        gamma (float): Risk aversion parameter
        max_stocks (int): Maximum number of stocks in the portfolio
        var_bound (float): Variance bound
        conf (float): Confidence level for the variance bound

    Returns:
        Series: Optimal weights
    """
    n = len(mu)

    # Function to minimize (negative Sharpe ratio)
    def objective(weights):
        return -((weights @ mu) - (gamma / 2) * (weights @ sigma @ weights))

    # Constraints: weights sum to 1
    constraints = [{'type': 'eq', 'fun': lambda x: np.sum(x) - 1}]  # Sum of weights = 1

    # Variance constraint if provided
    if var_bound is not None:
        constraints.append({
            'type': 'ineq',
            'fun': lambda x: var_bound - (x @ sigma @ x)  # Variance must be less than or equal to var_bound
        })

    # Boundaries for weights (between 0 and 1)
    bounds = [(0, 1) for _ in range(n)]

    # Initial guess
    initial_weights = np.ones(n) / n

    # Adding a constraint for maximum number of stocks
    if max_stocks is not None:
        def max_stocks_constraint(weights):
            return max_stocks - np.count_nonzero(weights)  # Count of non-zero weights must be less than or equal to max_stocks
        constraints.append({
            'type': 'ineq',
            'fun': max_stocks_constraint
        })

    # Perform optimization
    result = minimize(objective, initial_weights, method='SLSQP', bounds=bounds, constraints=constraints)

    # Check if optimization was successful
    if not result.success:
        raise ValueError("Optimization failed: " + result.message)

    return pd.Series(result.x, index=mu.index)


def dirichlet_sampling(alpha, columns, num_samples=20000):
    """
    Sample random portfolios from a Dirichlet distribution.

    Args:
        alpha (array-like): Concentration parameters of the Dirichlet distribution.
        columns (list): Names of the stocks in the portfolio.
        num_samples (int): Number of random portfolios to sample.

    Returns:
        DataFrame: Sampled portfolios where each row is a portfolio and each column is a stock.
    """
    # Generate samples from the Dirichlet distribution
    samples = np.random.dirichlet(alpha, num_samples)

    # Convert to a DataFrame for easier handling
    return pd.DataFrame(samples, columns=columns)


def dirichlet_weights(mu, sigma, alpha,
                      optimizer=mean_variance_optim,
                      num_samples=20000,
                      ) -> pd.Series:
    """
    Estimate the mean and covariance of portfolios sampled from a Dirichlet distribution.
    Find the portfolio closest to the optimal (as given by mean-variance optim).

    Args:
        mu (Series): Expected returns of assets.
        sigma (DataFrame): Covariance matrix of assets.
        alpha (array-like): Concentration parameters for the Dirichlet distribution.
        optimizer (function): Function to optimize the portfolio.
        num_samples (int): Number of portfolios to sample.

    Returns:
        Series: Portfolio weights closest to the optimized portfolio.
    """
    # Sample portfolios from the Dirichlet distribution
    df = dirichlet_sampling(alpha, mu.index, num_samples)

    # Calculate mean and covariance of each sampled portfolio
    sigma_np = sigma.values
    means = df.dot(mu)
    # sigma is n x n
    # df is num_samples x n
    covariances = df.apply(lambda x: x.dot(sigma_np).dot(x), axis=1)

    # Find the portfolio closest to the optimal
    optimal_weights = optimizer(mu, sigma)
    optimal_mean = optimal_weights.dot(mu)
    optimal_covariance = optimal_weights.dot(sigma).dot(optimal_weights)

    # Calculate distance to optimal portfolio
    distances = np.abs(means - optimal_mean) + np.abs(covariances - optimal_covariance)

    # Find the portfolio with the minimum distance that satisfies max stocks and VaR constraints
    min_index = distances.idxmin()
    return df.loc[min_index]


def black_litterman(P, q, Omega, pi, sigma, tau=0.025):
    """
    Perform the Black-Litterman model to generate expected returns.
    Accepts two sets of inputs: the market-implied returns and the investor's views.

    Args:
        P (DataFrame): Pick matrix
        q (DataFrame): Views
        Omega (DataFrame): Views uncertainty
        pi (Series): Equilibrium excess returns
        sigma (DataFrame): Covariance matrix of assets
        tau (float): Scaling factor

    Returns:
        Series: Expected returns
    """
    # Step 1: Adjust the covariance matrix with the scaling factor tau
    tau_sigma = tau * sigma

    # Step 2: Calculate the posterior estimate of the mean
    # First, calculate the middle term for the posterior distribution
    middle_term = np.linalg.inv(np.dot(np.dot(P.T, np.linalg.inv(Omega)), P) + np.linalg.inv(tau_sigma))

    # Then, calculate the posterior expected returns
    term1 = np.dot(np.dot(middle_term, P.T), np.dot(np.linalg.inv(Omega), q))
    term2 = np.dot(middle_term, np.dot(np.linalg.inv(tau_sigma), pi))

    # Final estimate of the mean
    theta = term1 + term2

    return pd.Series(theta, index=pi.index)