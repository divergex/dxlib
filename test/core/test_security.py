from dxlib import Security

if __name__ == "__main__":
    class MySecurity(Security):
        def __init__(self, symbol: str, price):
            super().__init__(symbol)
            self.price = price

        @property
        def value(self):
            return self.price

        @value.setter
        def value(self, value):
            self.price = value

        def to_dict(self):
            return {"value": self.value}

        def from_dict(self, data):
            self.value = data["value"]

    s = MySecurity("AAPL", 100)
    print(s)

    import json
    print(s.to_dict())
    s_json = json.dumps(s, default=lambda x: x.__json__())
    print(s_json)
