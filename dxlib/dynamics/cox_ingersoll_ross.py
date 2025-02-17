import numpy as np

from .stochastic_process import StochasticProcess


class CoxIngersollRoss(StochasticProcess):
    def __init__(self, mean_reversion=0.5, long_term_mean=1.0, volatility=0.1):
        """
        Stochastic process that models the evolution of the Cox-Ingersoll-Ross (CIR) process.
        Useful for modeling interest rates.

        Args:
            mean_reversion (float): Speed of mean reversion (kappa).
            long_term_mean (float): Long-term mean level (theta).
            volatility (float): Volatility parameter (sigma).
        """
        super().__init__()
        if mean_reversion <= 0:
            raise ValueError("Mean reversion rate must be positive.")
        if long_term_mean < 0:
            raise ValueError("Long-term mean must be positive.")
        if volatility < 0:
            raise ValueError("Volatility must be positive.")

        self.mean_reversion = mean_reversion
        self.long_term_mean = long_term_mean
        self.volatility = volatility

    def sample(self, x, dt, size=None, *args, **kwargs) -> np.ndarray | float:
        """
        Sample the Cox-Ingersoll-Ross process at a given point in time.

        Args:
            x (float | np.ndarray): Current value of the process.
            dt (float): Time step.
            size (int): Number of samples to generate.

        Returns:
            np.ndarray: Value of the process at the next time step.
        """
        assert (isinstance(x, (int, float)) and x >= 0) or (isinstance(x, np.ndarray) and np.all(x >= 0)), \
            "Argument x must be a non-negative number or a NumPy array of non-negative values."

        mean = x + self.mean_reversion * (self.long_term_mean - x) * dt
        variance = self.volatility ** 2 * x * dt

        if isinstance(x, (int, float)):
            return np.maximum(0, np.random.normal(mean, np.sqrt(variance), size))
        elif isinstance(x, np.ndarray):
            assert x.ndim == 1, "x must be a 1D array."
            if size is not None:
                assert x.shape[0] == size, "x must have the same size as the number of samples."
            return np.maximum(0, np.random.normal(mean, np.sqrt(variance), size))

    def simulate(self, x, dt, t, size=None, *args, **kwargs) -> np.ndarray:
        """
        Simulate the Cox-Ingersoll-Ross process over a given time period.

        Args:
            x (float | np.ndarray): Current value of the process.
            dt (float): Time step.
            t (float): Total simulation time.
            size (int): Number of samples to generate.

        Returns:
            np.ndarray: Simulated trajectory of the process over time.
        """
        assert (isinstance(x, (int, float)) and x >= 0) or (isinstance(x, np.ndarray) and np.all(x >= 0)), \
            "Argument x must be a non-negative number or a NumPy array of non-negative values."

        num_steps = int(t / dt)
        trajectory = np.zeros((num_steps, size)) if size is not None else np.zeros(num_steps)

        if isinstance(x, (int, float)):
            trajectory[0] = x
        elif isinstance(x, np.ndarray):
            assert x.ndim == 1, "x must be a 1D array."
            if size is not None:
                assert x.shape[0] == size, "x must have the same size as the number of samples."
            trajectory[0] = x

        for i in range(1, num_steps):
            trajectory[i] = self.sample(trajectory[i - 1], dt, size)

        return trajectory
