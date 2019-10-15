from __future__ import annotations
from threading import Lock
from typing import Optional
import rocksdb, json, xxhash

class SingletonMeta(type):
    """
    This is a thread-safe implementation of Singleton.
    """
    _instance: Optional[IndexRocksdb] = None
    _lock: Lock = Lock()

    def __call__(cls, *args, **kwargs):
        with cls._lock:
            if not cls._instance:
                cls._instance = super().__call__(*args, **kwargs)
        return cls._instance


class IndexRocksdb(metaclass=SingletonMeta):
    def __init__(self):
        super(IndexRocksdb, self).__init__()
        self._seed = 0
        self._db = rocksdb.DB("/tmp/sage-distinct/database.db", rocksdb.Options(
            create_if_missing=True
        ))
        print('[rocksdb] initialized with seed = %d' % self._seed)

    @property
    def db(self):
        return self._db

    def has(self, ID, group_key):
        key = bytes("{}_{}".format(
            ID,
            xxhash.xxh64_hexdigest(group_key)
        ), encoding = 'utf-8')
        (check, val) = self.db.key_may_exist(key=key, fetch=True)
        if check and val is not None:
            val = json.loads(val.decode('utf-8'))
        return (check, val)

    def hasBingings(self, ID, group_key, bindings):
        key = bytes("{}_{}_BINDINGS_{}".format(
            xxhash.xxh64_hexdigest(group_key),
            ID,
            xxhash.xxh64_hexdigest(json.dumps(bindings))
        ), encoding='utf-8')
        return self.db.key_may_exist(key=key, fetch=True)

    def setBindings(self, ID, group_key, bindings):
        key = bytes("{}_{}_BINDINGS_{}".format(
            xxhash.xxh64_hexdigest(group_key),
            ID,
            xxhash.xxh64_hexdigest(json.dumps(bindings))
        ), encoding='utf-8')
        self.db.put(key, b'{}')

    def set(self, ID, group_key, elem):
        key = bytes("{}_{}".format(
            ID,
            xxhash.xxh64_hexdigest(group_key)
        ), encoding='utf-8')

        val = bytes(json.dumps(elem), encoding='utf-8')
        self._db.put(key, val)

    def get(self, ID, group_key):
        val = self.db.get(bytes("{}_{}".format(ID, xxhash.xxh64_hexdigest(group_key)), encoding='utf-8'))
        return json.loads(val.decode(encoding='utf-8'))


    def iterator(self, ID=None):
        """Return an iterator on rocks db at prefix = ID, with sorted elements by their keys"""
        it = self._db.iteritems()
        it.seek(bytes(ID, encoding='utf-8'))
        return it

    def format_entry(self, elt):
        return (elt[0].decode('utf-8'), json.loads(elt[1].decode('utf-8')))