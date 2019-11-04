# count.py
# Author: Thomas MINIER - MIT License 2017-2019
from sage.query_engine.agg.partial_agg import PartialAggregator
import xxhash, json


class CountDistinctAggregator(PartialAggregator):
    """A CountDistinctAggregator evaluates a COUNT distinct aggregation"""

    def __init__(self, variable, binds_to='?c', query_id=None, ID=None):
        super(CountDistinctAggregator, self).__init__(variable, binds_to, query_id=query_id, ID=ID)
        self._groups = dict()

    def update(self, group_key, bindings):
        """Update the aggregator with a new value for a group of bindings"""
        if self._variable in bindings:
            if group_key not in self._groups:
                self._groups[group_key] = dict({
                    'hex': set(),
                    'values': []
                })
            try:
                d = dict()
                d[self._variable] = xxhash.xxh64_hexdigest(bindings[self._variable])
                d[self.get_binds_to()] = '"1"^^<http://www.w3.org/2001/XMLSchema#integer>'
                # create our own serialization because, json.dumps COSTS A LOT
                hash = xxhash.xxh64_hexdigest(group_key + "_" + d[self._variable])
                if not hash in self._groups[group_key]['hex']:
                    self._groups[group_key]['hex'].add(hash)
                    self._groups[group_key]['values'].append(d)
            except Exception as err:
                print(err)
                exit(1)
                raise err


    def done(self, group_key):
        """Return the group for the distinct aggregation given the group key"""
        return self._groups[group_key]['values']

    def get_type(self):
        """Return the name of the aggregator (used for serialization)"""
        return 'count-distinct'

    def __repr__(self):
        return "<Aggregator(COUNT(DISTINCT {}) AS {})>".format(self._variable, self._binds_to)

    def is_distinct(self):
        return True