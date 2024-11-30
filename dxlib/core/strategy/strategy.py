from abc import abstractmethod, ABC


class Strategy(ABC):
    @abstractmethod
    def execute(self, *args, **kwargs):
        raise NotImplementedError