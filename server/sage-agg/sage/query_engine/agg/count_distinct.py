# count.py
# Author: Arnaud GRALL - MIT License 2017-2019
from sage.query_engine.agg.partial_agg import PartialAggregator


class CDAError(Exception):
    """CountDistinctAggregator error"""
    pass


class CountDistinctAggregator(PartialAggregator):
    """A CountDistinctAggregator evaluates a COUNT DISTINCT aggregation"""

    def __init__(self, variable, binds_to='?c'):
        super(CountDistinctAggregator, self).__init__(variable, binds_to)
        self._groups = dict()
        self._current_key = ""
        self._finished = dict()

    def update(self, group_key, bindings):
        """Update the aggregator with a new value for a group of bindings"""
        print("count-distinct update: ", group_key, bindings)
        if self._variable in bindings:
            if group_key not in self._groups:
                self._groups[group_key] = 0
                # move the current into the finished
                self._finished[self._current_key] = self._groups[self._current_key]
                self._groups.pop(self._current_key)
            self._groups[group_key] += 1

    def done(self, group_key):
        """Complete the aggregation for a group and return the result"""
        if group_key in self._finished:
            return '"{}"^^<http://www.w3.org/2001/XMLSchema#integer>'.format(self._finished[group_key])
        else:
            raise CDAError()

    def get_type(self):
        """Return the name of the aggregator (used for serialization)"""
        return 'count-distinct'

    def __repr__(self):
        return "<Aggregator(COUNT(DISTINCT {}) AS {})>".format(self._variable, self._binds_to)
