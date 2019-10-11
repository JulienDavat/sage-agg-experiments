# count.py
# Author: Arnaud GRALL - MIT License 2017-2019
from sage.query_engine.agg.partial_agg import PartialAggregator
from sage.query_engine.agg.index.index_disk_simple import IndexSimple
from sage.query_engine.agg.index.index_disk_rocksdb import IndexRocksdb


class CDAError(Exception):
    """CountDistinctAggregator error"""
    pass


class CountDistinctAggregator(PartialAggregator):
    """A CountDistinctAggregator evaluates a COUNT DISTINCT aggregation"""

    def __init__(self, variable, binds_to='?c', ID=None):
        super(CountDistinctAggregator, self).__init__(variable, binds_to, ID)
        self._index = IndexSimple(self)
        self._ended = False

    def update(self, group_key, bindings):
        """Update the aggregator with a new value for a group of bindings"""
        print("count-distinct update: ", group_key, bindings)
        if self._variable in bindings:
            self._index.update(group_key, bindings, write=False)

    def done(self, group_key):
        """Complete the aggregation for a group and return the result"""
        if self._ended:
            return '"{}"^^<http://www.w3.org/2001/XMLSchema#integer>'.format(len(self._index.list(group_key, read=False)))
        else:
            raise CDAError()

    def get_type(self):
        """Return the name of the aggregator (used for serialization)"""
        return 'count-distinct'

    def __repr__(self):
        return "<Aggregator(COUNT(DISTINCT {}) AS {})>".format(self._variable, self._binds_to)

    def is_distinct(self):
        return True

    def end(self):
        self._ended = True

    def close(self):
        self._index.close()