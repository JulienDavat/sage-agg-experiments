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
        print("count-distinct update: ", group_key, bindings)
        if self._variable in bindings:
            # check if the binding exists or not
            # the binding is stored like this: self._id + '_' + group_key + '_BINDINGS_' + hash(bindings)
            # self._id is the unique id of the aggregator
            b_ex = self._index.hasBingings(self._id, group_key, bindings)
            # enable the count or not
            count = True
            if b_ex[0] and b_ex[1] is not None and b_ex[1] == bindings:
                count = False
            else:
                # store the binding
                self._index.setBindings(self._id, group_key, bindings)

            print("count-distinct update: binding written")
            exist = self._index.has(self._query_id, group_key)
            if not exist[0] or (exist[0] and exist[1] is None):
                print('not exist')
                elem = dict()
                elem['bindings'] = bindings
                elem[self.get_binds_to()] = 1

                self._index.set(self._query_id, group_key, elem)
            else:
                print('exist')
                elem = exist[1]
                if count:
                    elem[self.get_binds_to()] = elem[self.get_binds_to()] + 1
                elem['bindings'] = bindings
                self._index.set(self._query_id, group_key, elem)
            print("count-distinct update: finished")

    def done(self):
        try:
            val = self._index.iterator(self._query_id)
            for v in val:
                k, v = self._index.format_entry(v)
                if k.startswith(self._query_id):
                    print(self._query_id, k, v, k[37:])
                else:
                    print(k, v)
        except Exception as e:
            print(e)
            exit(1)


    def get_type(self):
        """Return the name of the aggregator (used for serialization)"""
        return 'count-distinct'

    def __repr__(self):
        return "<Aggregator({})(COUNT(DISTINCT {}) AS {})>".format(self._id, self._variable, self._binds_to)

    def is_distinct(self):
        return True