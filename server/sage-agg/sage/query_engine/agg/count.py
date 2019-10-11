# count.py
# Author: Thomas MINIER - MIT License 2017-2019
from sage.query_engine.agg.partial_agg import PartialAggregator


class CountAggregator(PartialAggregator):
    """A CountAggregator evaluates a COUNT aggregation"""

    def __init__(self, variable, binds_to='?c', ID=None):
        super(CountAggregator, self).__init__(variable, binds_to, ID)
        self._groups = dict()

    def update(self, group_key, bindings):
        """Update the aggregator with a new value for a group of bindings"""
        if self._variable in bindings:
            if group_key not in self._groups:
                self._groups[group_key] = 0
            self._groups[group_key] += 1

    def done(self, group_key):
        """Complete the aggregation for a group and return the result"""
        return '"{}"^^<http://www.w3.org/2001/XMLSchema#integer>'.format(self._groups[group_key])

    def get_type(self):
        """Return the name of the aggregator (used for serialization)"""
        return 'count'

    def __repr__(self):
        return "<Aggregator(COUNT({}) AS {})>".format(self._variable, self._binds_to)
