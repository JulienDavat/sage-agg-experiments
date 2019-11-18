# count.py
# Author: Thomas MINIER - MIT License 2017-2019
from sage.query_engine.agg.partial_agg import PartialAggregator
from sage.query_engine.agg.bb import BoundedBuffer
import xxhash

class CountDistinctAggregator(PartialAggregator):
    """A CountDistinctAggregator evaluates a COUNT distinct aggregation"""

    def __init__(self, variable, binds_to='?c', query_id=None, ID=None):
        super(CountDistinctAggregator, self).__init__(variable, binds_to, query_id=query_id, ID=ID)
        self._groups = dict()
        self._cache = BoundedBuffer()

    def update(self, group_key, bindings):
        """Update the aggregator with a new value for a group of bindings"""
        if self._variable in bindings:
            if group_key not in self._groups:
                self._groups[group_key] = [set(), bindings]
            try:
                hash = xxhash.xxh64_hexdigest(bindings[self._variable])
                if not hash in self._groups[group_key]:
                    self._groups[group_key][0].add(hash)
            except Exception as err:
                print(err)
                exit(1)
                raise err

    def update_bounded(self, group_key, bindings):
        """Update the aggregator with a new value for a group of bindings"""

        if self._variable in bindings:
            if not self._cache.has(self.get_query_id(), self.get_id(), group_key):
                hash = xxhash.xxh64_hexdigest(bindings[self._variable])
                v = [set(), bindings]
                v[0].add(hash)
                try:
                    self._cache.set(self.get_query_id(), self.get_id(), group_key, v)
                except Exception as e:
                    print(e)
                    exit(1)
                    raise e
            else:
                val = self._cache.get(self.get_query_id(), self.get_id(), group_key)
                try:
                    hash = xxhash.xxh64_hexdigest(bindings[self._variable])
                    if not hash in val[0]:
                        val[0].add(hash)
                        # try:
                        #     self._cache.set(self.get_query_id(), self.get_id(), group_key, val)
                        # except Exception as e:
                        #     print(e)
                        #     exit(1)
                        #     raise e
                except Exception as err:
                    print("update error:", err)
                    exit(1)
                    raise err


    def done(self, group_key):
        """Return the group for the distinct aggregation given the group key"""
        return [{
            self._variable: x,
            self.get_binds_to(): '"1"^^<http://www.w3.org/2001/XMLSchema#integer>'
        } for x in self._groups[group_key][0]]

    def done_bounded(self, group_key, groups={}):
        """Return the group for the distinct aggregation given the group key"""
        return [{
            self._variable: x,
            self.get_binds_to(): '"1"^^<http://www.w3.org/2001/XMLSchema#integer>'
        } for x in groups[group_key][self.get_id()][0]], groups[group_key][self.get_id()][1]

    def get_type(self):
        """Return the name of the aggregator (used for serialization)"""
        return 'count-distinct'

    def __repr__(self):
        return "<Aggregator(COUNT(DISTINCT {}) AS {})>".format(self._variable, self._binds_to)

    def is_distinct(self):
        return True