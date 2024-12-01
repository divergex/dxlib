class Endpoint:
    def __init__(self, route, *args, **kwargs):
        self.route = route
        self.func = None

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)
