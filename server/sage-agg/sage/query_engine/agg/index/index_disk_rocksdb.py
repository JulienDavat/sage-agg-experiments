import os, json, shutil, xxhash, rocksdb
from sage.query_engine.agg.index.index_disk_abstract import IndexAbstract

class IndexRocksdb(IndexAbstract):
    def __init__(self, aggregator):
        super(IndexRocksdb, self).__init__(aggregator)
        self._location = "/tmp/sage-distinct/database.db".format(self._id)
        self._seed = 0
        # create instance of rocksdb
        self._prefix_extractor = StaticPrefix()
        self._db = rocksdb.DB(self._location, rocksdb.Options(
            create_if_missing=True,
            prefix_extractor=self._prefix_extractor
        ))

    def update(self, group_key, binding, write=True):
        key = xxhash.xxh64_hexdigest("{}_{}".format(self._id, group_key))
        print('Update: ', key, binding)
        if write:
            self._db.put(bytes(key), bytes(json.dumps(binding)))
        else:
            self._db.put(bytes(key), b'')


    def list(self, group_key, read=True):
        prefix = xxhash.xxh64_hexdigest("{}_{}".format(self._id, group_key))

        it = self._db.iteritems()
        it.seek(prefix)

        group = []
        for elt in it:
            print(elt)

        print(group)
        return []

    def close(self):
        pass


class StaticPrefix(rocksdb.interfaces.SliceTransform):
    def __init__(self):
        super(StaticPrefix, self).__init__()
        self.__prefix_length = 16


    def set_prefix_length(self, length):
        self.__prefix_length = length

    def name(self):
        return b'static'

    def transform(self, src):
        return (0, self.__prefix_length)

    def in_domain(self, src):
        return len(src) >= self.__prefix_length

    def in_range(self, dst):
        return len(dst) == self.__prefix_length