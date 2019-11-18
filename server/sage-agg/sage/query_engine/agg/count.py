# count.py
# Author: Thomas MINIER - MIT License 2017-2019
from sage.query_engine.agg.partial_agg import PartialAggregator
from sage.query_engine.agg.bb import BoundedBuffer

class CountAggregator(PartialAggregator):
    """A CountAggregator evaluates a COUNT aggregation"""

    def __init__(self, variable, binds_to='?c', query_id=None, ID=None):
        super(CountAggregator, self).__init__(variable, binds_to, query_id=query_id, ID=ID)
        self._groups = dict()
        self._cache = BoundedBuffer()

    def update(self, group_key, bindings):
        """Update the aggregator with a new value for a group of bindings"""
        if self._variable in bindings:
            if group_key not in self._groups:
                self._groups[group_key] = 0
            self._groups[group_key] += 1

    def update_bounded(self, group_key, bindings):
        """Update the aggregator with a new value for a group of bindings"""
        if self._variable in bindings:
            if not self._cache.has(self.get_query_id(), self.get_id(), group_key):
                self._cache.set(self.get_query_id(), self.get_id(), group_key, [1, bindings])
            else:
                v = self._cache.get(self.get_query_id(), self.get_id(), group_key)
                v[0] += 1

    def done(self, group_key):
        """Complete the aggregation for a group and return the result"""
        return '"{}"^^<http://www.w3.org/2001/XMLSchema#integer>'.format(self._groups[group_key])

    def done_bounded(self, group_key, groups):
        """Complete the aggregation for a group and return the result"""
        return '"{}"^^<http://www.w3.org/2001/XMLSchema#integer>'.format(groups[group_key][self.get_id()][0]), groups[group_key][self.get_id()][1]

    def get_type(self):
        """Return the name of the aggregator (used for serialization)"""
        return 'count'

    def __repr__(self):
        return "<Aggregator(COUNT({}) AS {})>".format(self._variable, self._binds_to)
