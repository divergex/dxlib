import unittest

import matplotlib.pyplot as plt
import numpy as np

from dxlib.core.dynamics import OrnsteinUhlenbeck, CoxIngersollRoss, GeometricBrownianMotion, Hawkes


class TestGBM(unittest.TestCase):
    def setUp(self):
        self.annual_mean = 0.05
        self.annual_volatility = 0.2

        self.trading_days = 252
        self.trading_hours = 6.5
        self.minutes_in_year = self.trading_days * self.trading_hours * 60

        self.mean = self.annual_mean / self.minutes_in_year
        self.std = self.annual_volatility / np.sqrt(self.minutes_in_year)

    def test_sample(self):
        gbm = GeometricBrownianMotion(self.mean, self.std)

        x = 100
        sample = gbm.sample(x, 1)
        self.assertTrue(isinstance(sample, float))
        print(sample)

    def test_simulate(self):
        x = 100
        num_samples = 10
        t = self.trading_hours * 60
        gbm = GeometricBrownianMotion(self.mean, self.std)

        trajectory = np.array(list(gbm.simulate(x, 1, t, num_samples)))
        self.assertTrue(isinstance(trajectory, np.ndarray))
        self.assertEqual((int(t), num_samples), trajectory.shape)
        print(trajectory)

        plt.plot(trajectory)
        plt.title("Simulated trajectories")
        plt.show()

    def test_simulate_returns(self):
        x = 100
        num_samples = 1000
        t = self.trading_hours * 60
        gbm = GeometricBrownianMotion(self.mean, self.std)

        trajectory = np.array(list(gbm.simulate(x, 1, t, num_samples)))
        returns = np.mean(np.diff(trajectory, axis=0) / trajectory[:-1], axis=1)

        plt.plot(returns, color='black', linestyle='--')
        plt.title("Mean returns")
        plt.show()
        self.assertTrue(np.allclose(np.mean(returns, axis=0), 0, atol=0.1))

    def test_distribution(self):
        # plot prices to see if normality holds
        gbm = GeometricBrownianMotion(0.1, self.std)

        prices = gbm.sample(100, 1, 1000)
        plt.hist(prices, bins=50)
        # plot mean as vertical lnie
        plt.axvline(float(np.mean(prices)), color='black', linestyle='--')
        plt.axvline(100 * np.exp(0.1), color='black')
        plt.title("Price distribution")
        plt.show()

        self.assertAlmostEqual(np.mean(prices), 100 * np.exp(0.1), delta=0.1)

class TestHawkes(unittest.TestCase):
    def setUp(self):
        self.base_intensity = .5
        self.branching_ratio = 0.5
        self.decay = 1

    def test_sample(self):
        hawkes = Hawkes(self.base_intensity, self.branching_ratio, self.decay)

        x = 0
        sample = hawkes.sample(x, 1)

        self.assertIsInstance(sample, float)
        self.assertGreaterEqual(sample, x)
        print(sample)

    def test_simulate(self):
        hawkes = Hawkes(self.base_intensity, self.branching_ratio, self.decay)

        x = 0

        num_samples = 10
        t = 15
        trajectory = hawkes.simulate(x, 1, t, num_samples)
        self.assertTrue(isinstance(trajectory, np.ndarray))
        self.assertEqual(trajectory.shape, (int(t), num_samples))

        print(trajectory)

        plt.step(np.arange(0, t, 1), trajectory)
        plt.title("Simulated trajectories")
        plt.show()

        num_samples = 1000
        t = 15
        trajectory = hawkes.simulate(x, 1, t, num_samples)
        self.assertTrue(isinstance(trajectory, np.ndarray))
        self.assertEqual(trajectory.shape, (int(t), num_samples))

        plt.plot(np.arange(0, t, 1), np.mean(trajectory, axis=1), color='black', linestyle='--')
        plt.plot(np.arange(0, t, 1), np.arange(0, t, 1), color='black')
        # should be lower than the diagonal because more events reduce future waiting times proportionally to branching ratio
        plt.title("Mean time of events")
        plt.show()


class TestCIR(unittest.TestCase):
    def setUp(self):
        self.mean_reversion = 0.1
        self.long_term_mean = 1
        self.volatility = 0.1

    def test_sample(self):
        cir = CoxIngersollRoss(self.mean_reversion, self.long_term_mean, self.volatility)

        x = 1
        sample = cir.sample(x, 1)
        self.assertTrue(isinstance(sample, float))
        print(sample)

    def test_simulate(self):
        cir = CoxIngersollRoss(self.mean_reversion, self.long_term_mean, self.volatility)

        x = 1

        num_samples = 10
        t = 15
        trajectory = cir.simulate(x, 1, t, num_samples)
        self.assertTrue(isinstance(trajectory, np.ndarray))
        self.assertEqual(trajectory.shape, (int(t), num_samples))

        print(trajectory)

        plt.plot(trajectory)
        plt.title("Simulated trajectories")
        plt.show()

        num_samples = 1000
        t = 15
        trajectory = cir.simulate(x, 1, t, num_samples)

        plt.plot(np.arange(0, t, 1), np.mean(trajectory, axis=1), color='black', linestyle='--')
        plt.plot(np.arange(0, t, 1), np.ones(int(t)), color='black')
        plt.title("Mean of the process")
        plt.show()


class TestMeanReversion(unittest.TestCase):
    def setUp(self):
        self.mean_reversion = 1e-2
        self.volatility = 0.1

    def test_sample(self):
        x = 100
        ornstein_uhlenbeck = OrnsteinUhlenbeck(
            self.mean_reversion, x, self.volatility
        )

        sample = ornstein_uhlenbeck.sample(105, 1)

        self.assertTrue(isinstance(sample, float))
        self.assertLessEqual(float(sample), 105)


    def test_simulate(self):
        mean = 100
        x = 105
        ou = OrnsteinUhlenbeck(
            self.mean_reversion, mean, self.volatility
        )

        num_samples = 5
        t = 100
        trajectory = ou.simulate(x, 1, t, num_samples)
        ou.long_term_mean = 105
        x = trajectory[-1]
        trajectory2 = ou.simulate(x, 1, t, num_samples)
        trajectory = np.concatenate((trajectory, trajectory2), axis=0)

        print(trajectory)

        plt.plot(trajectory)
        plt.title("Simulated trajectories")
        plt.show()

if __name__ == '__main__':
    unittest.main()
