# count.py
# Author: Thomas MINIER - MIT License 2017-2019
from sage.query_engine.iterators.aggregates.partial_agg import PartialAggregator


class CountAggregator(PartialAggregator):
    """A CountAggregator evaluates a COUNT aggregation"""

    def __init__(self, variable, binds_to='?c'):
        super(CountAggregator, self).__init__(variable, binds_to)
        self._groups = dict()
        self._size = 0

    def update(self, group_key, bindings):
        """Update the aggregator with a new value for a group of bindings"""
        if self._variable in bindings:
            if group_key not in self._groups:
                self._groups[group_key] = 0
                self._size += 16 # size of the group key in bytes (xxhash64 generates 16 bytes strings)
            self._groups[group_key] += 1

    def done(self, group_key):
        """Complete the aggregation for a group and return the result"""
        return {
            '__type__': 'count', 
            '__value__': self._groups[group_key]
        }

    def get_type(self):
        """Return the name of the aggregator (used for serialization)"""
        return 'count'

    def __repr__(self):
        return "<Aggregator(COUNT({}) AS {})>".format(self._variable, self._binds_to)

    def size(self):
        return self._size