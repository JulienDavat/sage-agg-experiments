import os, json, shutil, xxhash
from sage.query_engine.agg.index.index_disk_abstract import IndexAbstract

class IndexRocksdb(IndexAbstract):
    def __init__(self, aggregator):
        super(IndexRocksdb, self).__init__(aggregator)
        self._location = "/tmp/sage-distinct/{}/".format(self._id)
        self._seed = 0

    def update(self, group_key, binding, write=True):
        ghash = xxhash.xxh64_hexdigest(group_key, self._seed)
        bhash = xxhash.xxh64_hexdigest(json.dumps(binding), self._seed)


    def list(self, group_key, read=True):
        ghash = xxhash.xxh64_hexdigest(group_key, self._seed)


    def close(self):
        pass
