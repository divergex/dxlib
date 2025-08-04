from typing import Iterator, Tuple

import numpy as np

from .stochastic_process import StochasticProcess


class GeometricBrownianMotion(StochasticProcess):
    def __init__(self, mean=0.0, vol=1.0):
        """
        Stochastic process that models the evolution of a geometric brownian motion.

        Args:
            mean (float): Mean of the process.
            vol (float): Standard deviation of the process
        """
        super().__init__()
        self.mean = mean
        self.vol = vol

    def sample(self, x, dt, size=None, *args, **kwargs) -> np.ndarray:
        """
        Sample the stochastic process at a given point in time.

        Args:
            x (float | np.ndarray): Current value of the process.
            dt (float): Time step.
            size (int): Number of samples to generate.
        Returns:
            float: Value of the process at a given point in time.
        """
        assert (isinstance(x, (int, float)) and not isinstance(x, complex)) or isinstance(x, np.ndarray), \
            "Argument must be a non-complex number or a NumPy array"
        if isinstance(x, (int, float)) and not isinstance(x, complex):
            return x * np.exp((self.mean - 0.5 * self.vol ** 2) * dt
                              + self.vol * np.random.normal(0, np.sqrt(dt), size))
        elif isinstance(x, np.ndarray) and size is not None:
            assert x.ndim == 1, "x must be a 1D array."
            assert x.shape[0] == size, "x must have the same size as the number of samples."
            return x * np.exp((self.mean - 0.5 * self.vol ** 2) * dt
                              + self.vol * np.random.normal(0, np.sqrt(dt), size))

        else:
            return x * np.exp((self.mean - 0.5 * self.vol ** 2) * dt) + self.vol * np.random.normal(0, np.sqrt(dt))

    def simulate(self, x, dt, t, size=None, *args, **kwargs) -> Iterator[Tuple[np.ndarray, float]]:
        """
        Simulate the stochastic process over a given time period.

        Args:
            x (float | np.ndarray): Current value of the process.
            dt (float): Time step.
            t (float): Total simulation time.
            size (int): Number of samples to generate.
        Returns:
            np.ndarray: Simulated trajectory of the process over time.
        """
        assert ((isinstance(x, (int, float)) and not isinstance(x, complex)) or isinstance(x, np.ndarray)), "Argument must be a non-complex number or a NumPy array"

        num_steps = int(t / dt)

        if isinstance(x, (int, float)) and not isinstance(x, complex):
            sample = x
        elif isinstance(x, np.ndarray) and size is not None:
            assert x.ndim == 1, "x must be a 1D array."
            assert x.shape[0] == size, "x must have the same size as the number of samples."
            sample = x
        elif isinstance(x, np.ndarray) and len(x) > 1 and size is None:
            raise ValueError(
                "Argument 'size' must be provided when 'x' is a NumPy array.")
        else:
            sample = np.zeros_like(x)

        for i in range(0, num_steps):
            yield sample, i * dt
            sample = self.sample(sample, dt, size)
        return None
