class Endpoint:
    def __init__(self, route, *args, **kwargs):
        self.route = route
        self.method = None

    def __call__(self, *args, **kwargs):
        return self.method(*args, **kwargs)

    def to_dict(self):
        return {
            "route": self.route,
            "method": self.method
        }
