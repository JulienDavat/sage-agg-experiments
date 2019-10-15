# sum.py
# Author: Thomas MINIER - MIT License 2017-2019
from sage.query_engine.agg.partial_agg import PartialAggregator
from sage.query_engine.agg.utils import to_numeric


class SumAggregator(PartialAggregator):
    """A SumAggregator evaluates a SUM aggregation"""

    def __init__(self, variable, binds_to='?sum', query_id=None, ID=None):
        super(SumAggregator, self).__init__(variable, binds_to, query_id=query_id, ID=ID)
        self._groups = dict()

    def update(self, group_key, bindings):
        """Update the aggregator with a new value for a group of bindings"""
        if self._variable in bindings:
            if group_key not in self._groups:
                self._groups[group_key] = 0
            self._groups[group_key] += to_numeric(bindings[self._variable])

    def done(self, group_key):
        """Complete the aggregation for a group and return the result"""
        v = self._groups[group_key]
        if isinstance(v, int):
            return '"{}"^^<http://www.w3.org/2001/XMLSchema#integer>'.format(v)
        elif isinstance(v, float):
            return '"{}"^^<http://www.w3.org/2001/XMLSchema#float>'.format(v)
        else:
            return '"{}"^^<http://www.w3.org/2001/XMLSchema#number>'.format(v)

    def get_type(self):
        """Return the name of the aggregator (used for serialization)"""
        return 'sum'

    def __repr__(self):
        return "<Aggregator(SUM({}) AS {})>".format(self._variable, self._binds_to)
