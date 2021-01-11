# partial_agg.py
# Author: Thomas MINIER - MIT License 2017-2019
from abc import ABC, abstractmethod


class PartialAggregator(ABC):
    """An abstract class for implementing Partial Aggregators"""

    def __init__(self, variable, binds_to='?agg'):
        super(PartialAggregator, self).__init__()
        self._variable = variable
        self._binds_to = binds_to

    @abstractmethod
    def get_type(self):
        """Return the name of the aggregator (used for serialization)"""
        pass

    @abstractmethod
    def update(self, group_key, bindings):
        """Update the aggregator with a new value for a group of bindings"""
        pass

    @abstractmethod
    def done(self, group_key):
        """Complete the aggregation for a group and return the result"""
        pass

    def get_variable(self):
        """Return the variable on which the aggregator is performed (used for serialization)"""
        return self._variable

    def get_binds_to(self):
        """Return the variable on which aggregation's reults are bind to (used for serialization)"""
        return self._binds_to

    def is_distinct(self):
        """Return true if it is a distinct aggregator"""
        return False

    def close(self):
        """should only be called by aggregator using disk"""
        if hasattr(self, '_index'):
            self._index.close()
