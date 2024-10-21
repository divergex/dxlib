import time
from enum import Enum
from typing import Dict

from .common import Singleton


class Timer(metaclass=Singleton):
    class Unit(Enum):
        seconds = 1e1
        milliseconds = 1e3
        microseconds = 1e6
        nanoseconds = 1e9

    def __init__(self):
        self.times: Dict[str, float] = {}

    def start(self, name: str, exclude_overhead = True):
        self.times[name] = time.time_ns() - (self.overhead * exclude_overhead)

    def stop(self, name: str, exclude_overhead = True):
        self.times[name] = time.time_ns() - self.times[name] - (self.overhead * exclude_overhead)

    def print(self, unit: Unit, places: int = 4, n: int = 1):
        if n > 1:
            for name, clock_time in self.times.items():
                t = f"{clock_time * unit.value / n:.{places}f} {unit.name}"
                print(f"{name.capitalize()} time: {t}{' * ' + str(n) + ' items' if n > 1 else ''}.")
        else:
            for name, clock_time in self.times.items():
                t = f"{clock_time * unit.value:.{places}f} {unit.name}"
                print(f"{name.capitalize()} time: {t}.")

    @property
    def overhead(self):
        return time.time_ns() - time.time_ns()