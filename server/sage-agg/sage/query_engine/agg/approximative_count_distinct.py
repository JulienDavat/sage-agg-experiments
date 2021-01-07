# count.py
# Author: Julien AIMONIER-DAVAT
import xxhash
from sage.query_engine.agg.bb import BoundedBuffer
from sage.query_engine.agg.partial_agg import PartialAggregator

from hyperloglog import HyperLogLog


class ApproximateCountDistinctAggregator(PartialAggregator):
    """AN ApproximateCountDistinctAggregator evaluates an approximation of a COUNT distinct aggregation"""

    def __init__(self, variable, binds_to='?c', query_id=None, ID=None, error_rate=0.01):
        super(ApproximateCountDistinctAggregator, self).__init__(variable, binds_to, query_id=query_id, ID=ID)
        self._error_rate = error_rate
        self._groups = dict()

    def update(self, group_key, bindings):
        """Update the aggregator with a new value for a group of bindings"""
        if self._variable in bindings:
            if group_key not in self._groups:
                self._groups[group_key] = HyperLogLog(self._error_rate)
            self._groups[group_key].add(bindings[self._variable])
            # try:
            #     hash = xxhash.xxh64_hexdigest(bindings[self._variable])
            #     if not hash in self._groups[group_key]:
            #         self._groups[group_key][0].add(hash)
            # except Exception as err:
            #     print(err)
            #     exit(1)
            #     raise err

    # def done(self, group_key):
    #     """Complete the distinct aggregation for a group and return the result"""
    #     return '"{}"^^<http://www.w3.org/2001/XMLSchema#integer>'.format(int(self._groups[group_key].card()))

    def done(self, group_key):
        """Complete the distinct aggregation for a group and return the result"""
        return {
            '__type__': 'approximative-count-distinct', 
            '__value__': self._groups[group_key].save()  
        }

    def get_type(self):
        """Return the name of the aggregator (used for serialization)"""
        return 'approximative-count-distinct'

    def __repr__(self):
        return "<Aggregator(COUNT(DISTINCT {}) AS {})>".format(self._variable, self._binds_to)

    def is_distinct(self):
        return True