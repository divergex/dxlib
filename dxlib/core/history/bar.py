class Bar:
    def __init__(self, open, high, low, close, volume):
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume

    def __str__(self):
        return f"Bar({self.open}, {self.high}, {self.low}, {self.close}, {self.volume})"

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        return (self.open == other.open and
                self.high == other.high and
                self.low == other.low and
                self.close == other.close and
                self.volume == other.volume)

    def __ne__(self, other):
        return not self.__eq__(
            other
        )
