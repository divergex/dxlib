import numpy as np

from .stochastic_process import StochasticProcess


class Hawkes(StochasticProcess):
    def __init__(self, base_intensity=.5, branching_ratio=.5, decay=.1):
        """
        Stochastic process that models the evolution of a Hawkes process.

        Args:
            base_intensity (float): The baseline intensity (lambda). The smaller the value, less frequent the events.
            branching_ratio (float): The branching ratio (alpha), must satisfy 0 <= alpha < 1. The larger the value, the more frequent the events.
            decay (float): The decay rate (beta) for the exponential kernel. The larger the value, the faster the decay.
        """
        super().__init__()
        if not (0 <= branching_ratio < 1):
            raise ValueError("Branching ratio must satisfy 0 <= branching_ratio < 1.")

        self.base_intensity = base_intensity
        self.branching_ratio = branching_ratio
        self.decay = decay

    def intensity(self, x, dt, events) -> np.ndarray:
        """
        Compute the intensity of the Hawkes process at a given point in time.

        Args:
            x (float | np.ndarray): Current value of the process (typically the number of events).
            events (np.ndarray): Array of event times.

        Returns:
            np.ndarray: Intensity of the Hawkes process at a given point in time.
        """
        return (self.base_intensity
                + self.branching_ratio * np.sum(np.exp(-self.decay * (x - events) / dt), axis=0))

    def sample(self, x, dt, events=None, size=None, *args, **kwargs) -> np.ndarray | float:
        """
        Sample the Hawkes process at a given point in time.

        Args:
            x (float | np.ndarray): Current value of the process (typically the number of events).
            dt (float): Time step.
            events (np.ndarray): Array of event times.
            size (int): Number of samples to generate.
        Returns:
            np.ndarray: Samples drawn from the Hawkes process.
        """
        assert (isinstance(x, (int, float)) and not isinstance(x, complex)) or isinstance(x, np.ndarray), \
            "Argument must be a non-complex number or a NumPy array"
        events = np.array(events) if events is not None else np.array([])
        if isinstance(x, (int, float)) and not isinstance(x, complex):
            return x + np.random.exponential(dt / self.intensity(x, dt, events), size)
        elif isinstance(x, np.ndarray) and size is not None:
            assert x.ndim == 1, "x must be a 1D array."
            assert x.shape[0] == size, "x must have the same size as the number of samples."
            assert x.shape[0] == events.shape[-1], "x and events must have the same size."
            return x + np.random.exponential(dt / self.intensity(x, dt, events), size)

    def simulate(self, x, dt, t, size=None, *args, **kwargs) -> np.ndarray:
        """
        Simulate the Hawkes process over a given time period.

        Args:
            x (float): Current value of the process (typically the number of events).
            dt (float): Time step.
            t (float): Total simulation time.
            size (int): Number of samples to generate.

        Returns:
            np.ndarray: Simulated trajectory of the Hawkes process over time.
        """
        assert (isinstance(x, (int, float)) and not isinstance(x, complex)) or isinstance(x, np.ndarray), \
            "Argument must be a non-complex number or a NumPy array"
        if isinstance(x, np.ndarray) and size is None:
            raise ValueError("Argument 'size' must be provided when 'x' is a NumPy array.")

        num_steps = int(t / dt)

        if isinstance(x, (int, float)) and not isinstance(x, complex) and size is None:
            trajectory = np.zeros(num_steps)
            trajectory[0] = x

            for i in range(1, num_steps):
                trajectory[i] = self.sample(trajectory[i - 1], dt)

            return trajectory

        elif isinstance(x, (int, float)) and not isinstance(x, complex) and size is not None:
            trajectory = np.zeros((num_steps, size))
            trajectory[0] = x

            for i in range(1, num_steps):
                events = trajectory[:i]
                trajectory[i] = self.sample(trajectory[i - 1], dt, events, size)

            return trajectory

        elif isinstance(x, np.ndarray) and size is not None:
            assert x.ndim == 1, "x must be a 1D array."
            assert x.shape[0] == size, "x must have the same size as the number of samples."
            trajectory = np.zeros((num_steps, size))
            trajectory[0] = x

            for i in range(1, num_steps):
                events = trajectory[:i]
                trajectory[i] = self.sample(trajectory[i - 1], dt, events, size)

            return trajectory
        elif isinstance(x, np.ndarray) and size is None:
            raise ValueError("Argument 'size' must be provided when 'x' is a NumPy array.")
        else:
            raise ValueError("Invalid input arguments.")
