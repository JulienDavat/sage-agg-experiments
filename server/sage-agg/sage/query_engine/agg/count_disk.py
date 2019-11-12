# count.py
# Author: Thomas MINIER - MIT License 2017-2019
from sage.query_engine.agg.partial_agg import PartialAggregator
from sage.query_engine.agg.index_disk_rocksdb import IndexRocksdb

class CountDiskAggregator(PartialAggregator):
    """A CountDiskAggregator evaluates a COUNT aggregation"""

    def __init__(self, variable, binds_to='?c', query_id=None, ID=None):
        super(CountDiskAggregator, self).__init__(variable, binds_to, query_id=query_id, ID=ID)
        self._index = IndexRocksdb()

    def update(self, group_key, bindings):
        """Update the aggregator with a new value for a group of bindings"""
        if self._variable in bindings:
            exist = self._index.has(self._query_id, group_key)
            if not exist[0]:
                elem = dict()
                elem['bindings'] = bindings
                elem['group_key'] = group_key
                elem[self.get_binds_to()] = 1
                self._index.set(query_id=self._query_id, group_key=group_key, value=elem)
            else:
                elem = exist[1].copy()
                try:
                    if self.get_binds_to() in elem:
                        elem[self.get_binds_to()] = elem[self.get_binds_to()] + 1
                    else:
                        elem[self.get_binds_to()] = 1
                    elem['bindings'] = bindings
                    self._index.set(query_id=self._query_id, group_key=group_key, value=elem)
                except Exception as e:
                    print(str(e))

    def done(self, bindings):
        """
            Return the result for this aggregation
            You have to provide the the result of get_first_group_key
        """
        return '"{}"^^<http://www.w3.org/2001/XMLSchema#integer>'.format(bindings[self.get_binds_to()])

    def get_first_group_key(self):
        return self._index.get_first_group_key_for_query(self._query_id)

    def remove_group_key(self, group_key):
        self._index.remove_group_key_for_query(query_id=self.get_query_id(), group_key=group_key)


    def get_type(self):
        """Return the name of the aggregator (used for serialization)"""
        return 'count-disk'

    def __repr__(self):
        return "<AggregatorDisk(COUNT({}) AS {})>".format(self._variable, self._binds_to)
