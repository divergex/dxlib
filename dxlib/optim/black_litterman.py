import numpy as np
from scipy.linalg import inv


def black_litterman(expected_market_returns, tau, P, Q, omega, cov_matrix, delta=2.5):
    """
    Black-Litterman Model implementation, which combines the Markowitz model with the views of an investor.
    The views can be absolute or relative, and the model adjusts the expected returns based on the uncertainty
    Absolute views are expressed as expected returns, while relative views are expressed as expected return differences.

    Args:
        expected_market_returns: Market equilibrium returns (vector)
        tau: Scalar indicating uncertainty in the prior (usually small)
        P: View matrix (views on returns)
        Q: View vector (expected values of views)
        omega: Diagonal matrix (uncertainty in views)
        cov_matrix: Covariance matrix of the assets
        delta: Risk aversion coefficient
    Returns:
        Adjusted expected returns
    """
    # Calculate the Black-Litterman expected returns
    M_inverse = inv(tau * cov_matrix) + np.dot(np.dot(P.T, inv(omega)), P)
    M = inv(M_inverse)
    adj_returns = np.dot(M, np.dot(tau * inv(cov_matrix), expected_market_returns) +
                         np.dot(np.dot(P.T, inv(omega)), Q))
    return adj_returns


# Example data
expected_market_returns = np.array([0.04, 0.06, 0.05])  # Market equilibrium returns, por exemplo: ["PETR4", "CPFL", "TSLA"]
tau = 0.025  # Scalar indicating uncertainty in the prior
P = np.array([[1, -1, 0], [0, 1, -1]])  # View matrix
Q = np.array([0.01, 0.02])  # View vector
omega = np.diag([0.0001, 0.0001])  # Uncertainty in views
cov_matrix = np.array([[0.0004, 0.0002, 0.0001],
                       [0.0002, 0.0003, 0.00015],
                       [0.0001, 0.00015, 0.00025]])  # Covariance matrix

# Compute adjusted returns
adjusted_returns = black_litterman(expected_market_returns, tau, P, Q, omega, cov_matrix)
print("Adjusted Expected Returns:", adjusted_returns)
# Output: Adjusted Expected Returns: [0.041 0.059 0.054] -> This is input to the portfolio optimization step (arquivo mean_cvar.py)
