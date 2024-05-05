import numpy as np
from scipy.stats import norm


class BSMModel:
    def __init__(self, **kwargs):
        pass

    def d_plus(self, sigma, S, K, T, r, d):
        return (np.log(S / K) + (r - d + 0.5 * sigma ** 2) * (T)) / (sigma * np.sqrt(T))

    def d_minus(self, d_plus, sigma, T):
        return d_plus - sigma * np.sqrt(T)

    def call(self, S, K, T, r, d, sigma):
        d_plus = self.d_plus(sigma, S, K, T, r, d)
        d_minus = self.d_minus(d_plus, sigma, T)
        return S * np.exp(-d * T) * norm.cdf(d_plus) - K * np.exp(-r * T) * norm.cdf(d_minus)

    def put(self, S, K, T, r, d, sigma):
        d_plus = self.d_plus(sigma, S, K, T, r, d)
        d_minus = self.d_minus(d_plus, sigma, T)
        return K * np.exp(-r * T) * norm.cdf(-d_minus) - S * np.exp(-d * T) * norm.cdf(-d_plus)

    def price(self, S, K, T, r, d, sigma, option_type: str = "call"):
        if option_type == "call":
            return self.call(S, K, T, r, d, sigma)
        elif option_type == "put":
            return self.put(S, K, T, r, d, sigma)
        else:
            raise ValueError("Invalid option type.")

    def delta(self, S, K, T, r, d, sigma, option_type: str = "call"):
        d_plus = self.d_plus(sigma, S, K, T, r, d)
        if option_type == "call":
            return np.exp(-d * T) * norm.cdf(d_plus)
        elif option_type == "put":
            return -np.exp(-d * T) * norm.cdf(-d_plus)
        else:
            raise ValueError("Invalid option type.")

    def gamma(self, S, K, T, r, d, sigma):
        d_plus = self.d_plus(sigma, S, K, T, r, d)
        return np.exp(-d * T) * norm.pdf(d_plus) / (S * sigma * np.sqrt(T))

    def theta(self, S, K, T, r, d, sigma, option_type: str = "call"):
        d_plus = self.d_plus(sigma, S, K, T, r, d)
        d_minus = self.d_minus(d_plus, sigma, T)
        if option_type == "call":
            return -S * np.exp(-d * T) * norm.pdf(d_plus) * sigma / (2 * np.sqrt(T)) - r * K * np.exp(-r * T) * norm.cdf(d_minus) + d * S * np.exp(-d * T) * norm.cdf(d_plus)
        elif option_type == "put":
            return -S * np.exp(-d * T) * norm.pdf(d_plus) * sigma / (2 * np.sqrt(T)) + r * K * np.exp(-r * T) * norm.cdf(-d_minus) - d * S * np.exp(-d * T) * norm.cdf(-d_plus)
        else:
            raise ValueError("Invalid option type.")

    def vega(self, S, K, T, r, d, sigma):
        d_plus = self.d_plus(sigma, S, K, T, r, d)
        return S * np.exp(-d * T) * norm.pdf(d_plus) * np.sqrt(T)

    def rho(self, S, K, T, r, d, sigma, option_type: str = "call"):
        d_minus = self.d_minus(self.d_plus(sigma, S, K, T, r, d), sigma, T)
        if option_type == "call":
            return K * T * np.exp(-r * T) * norm.cdf(d_minus)
        elif option_type == "put":
            return -K * T * np.exp(-r * T) * norm.cdf(-d_minus)
        else:
            raise ValueError("Invalid option type.")
