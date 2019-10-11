from abc import ABC, abstractmethod

class IndexAbstract(ABC):
    def __init__(self, aggregator):
        self._aggregator = aggregator
        self._id = self._aggregator.get_id()

    @abstractmethod
    def update(self, group_key, binding, write=True):
        pass

    @abstractmethod
    def list(self, group_key, read=True):
        pass

    @abstractmethod
    def close(self):
        pass
