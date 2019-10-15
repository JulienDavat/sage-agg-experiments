# count.py
# Author: Arnaud GRALL - MIT License 2017-2019
from sage.query_engine.agg.partial_agg import PartialAggregator
from sage.query_engine.agg.index.index_disk_rocksdb import IndexRocksdb


class CDAError(Exception):
    """CountDistinctAggregator error"""
    pass


class CountDistinctAggregator(PartialAggregator):
    """A CountDistinctAggregator evaluates a COUNT DISTINCT aggregation"""

    def __init__(self, variable, binds_to='?c', query_id=None, ID=None):
        super(CountDistinctAggregator, self).__init__(variable, binds_to, query_id=query_id, ID=ID)
        # print('[count-distinct] initialized with id=%s' % self._id)
        self._index = IndexRocksdb()
        self._ended = False

    def update(self, group_key, bindings):
        """Update the aggregator with a new value for a group of bindings"""
        if self._variable in bindings:
            # check if the binding exists or not
            # the binding is stored like this: self._id + '_' + group_key + '_BINDINGS_' + hash(bindings)
            # self._id is the unique id of the aggregator
            e = bindings[self._variable]
            count = True
            if self._index.has_bindings(query_id=self.get_query_id(), aggregator_id=self.get_id(), group_key=group_key, bindings=e):
                count = False
            else:
                # store the binding
                self._index.set_bindings(query_id=self.get_query_id(), aggregator_id=self.get_id(), group_key=group_key, bindings=e)

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
                    if count:
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
        return 'count-distinct'

    def __repr__(self):
        return "<Aggregator({})(COUNT(DISTINCT {}) AS {})>".format(self._id, self._variable, self._binds_to)

    def is_distinct(self):
        return True