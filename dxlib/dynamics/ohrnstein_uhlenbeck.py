import numpy as np

from .stochastic_process import StochasticProcess


class OrnsteinUhlenbeck(StochasticProcess):
    def __init__(self, mean_reversion=0.5, long_term_mean=0.0, volatility=0.1):
        """
        Stochastic process that models the evolution of an Ornstein-Uhlenbeck process.

        Args:
            mean_reversion (float): Speed of mean reversion (kappa).
            long_term_mean (float): Long-term mean level (theta).
            volatility (float): Volatility parameter (sigma).
        """
        super().__init__()
        if mean_reversion <= 0:
            raise ValueError("Mean reversion rate must be positive.")
        if volatility < 0:
            raise ValueError("Volatility must be positive.")

        self.mean_reversion = mean_reversion
        self.long_term_mean = long_term_mean
        self.volatility = volatility

    def sample(self, x, dt, size=None, *args, **kwargs) -> np.ndarray | float:
        """
        Sample the Ornstein-Uhlenbeck process at a given point in time.

        Args:
            x (float | np.ndarray): Current value of the process.
            dt (float): Time step.
            size (int): Number of samples to generate.

        Returns:
            np.ndarray: Value of the process at the next time step.
        """
        assert isinstance(x, (int, float, np.ndarray)), "Argument x must be a number or a NumPy array."

        mean = x + self.mean_reversion * (self.long_term_mean - x) * dt
        std_dev = self.volatility * np.sqrt(dt)

        if isinstance(x, (int, float)):
            return np.random.normal(mean, std_dev, size)
        elif isinstance(x, np.ndarray):
            assert x.ndim == 1, "x must be a 1D array."
            if size is not None:
                assert x.shape[0] == size, "x must have the same size as the number of samples."
            return np.random.normal(mean, std_dev, size)

    def simulate(self, x, dt, t, size=None, *args, **kwargs) -> np.ndarray:
        """
        Simulate the Ornstein-Uhlenbeck process over a given time period.

        Args:
            x (float | np.ndarray): Current value of the process.
            dt (float): Time step.
            t (float): Total simulation time.
            size (int): Number of samples to generate.

        Returns:
            np.ndarray: Simulated trajectory of the process over time.
        """
        assert isinstance(x, (int, float, np.ndarray)), "Argument x must be a number or a NumPy array."

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
