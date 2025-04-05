from abc import ABC, abstractmethod

import numpy as np


class StochasticProcess(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def sample(self, x, dt, size, *args, **kwargs) -> np.ndarray:
        """
        Sample the stochastic process at a given point in time.

        Args:
            x (float): Current value of the process.
            dt (float): Measure of the time step.
            size (int): Number of samples to generate.
        """
        pass

    @abstractmethod
    def simulate(self, x, dt, t, size, *args, **kwargs) -> np.ndarray:
        """
        Simulate the stochastic process over a given time period.

        Args:
            x (float): Current value of the process.
            dt (float): Measure of the time step.
            t (float): Total simulation time.
            size (int): Number of samples to generate.
        """
        pass
