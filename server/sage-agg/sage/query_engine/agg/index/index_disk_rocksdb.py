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


    def has_bindings(self, group_key=None, aggregator_id=None, query_id=None, bindings=None):
        """
            Return the resut of a call to rocksdb.key_may_exist with fetch=true
            The key is the following: hash(group_key) + '_' + ID + '_' + hash(bindings)
            The ID is the id of the aggregator
        """
        hashb = xxhash.xxh64_hexdigest(bindings)
        key = bytes("{}_{}_{}_{}".format(
            xxhash.xxh64_hexdigest(group_key),
            query_id,
            aggregator_id,
            hashb
        ), encoding='utf-8')
        exist = self.db.key_may_exist(key=key, fetch=True)
        if exist[0] and exist[1] is not None:
            if exist[1].decode('utf-8') == hashb:
                return True
            else:
                # we need to call a get to verify
                val = self.db.get(key).decode('utf-8')
                return val == hashb
        else:
            return False

    def set_bindings(self, group_key=None, aggregator_id=None, query_id=None, bindings=None):
        """
            Add the binding to rocksdb
            The key is the following: hash(group_key) + '_' + ID + '_' + hash(bindings)
            The ID is the id of the aggregator
        """
        hashb = xxhash.xxh64_hexdigest(bindings)
        key = bytes("{}_{}_{}_{}".format(
            xxhash.xxh64_hexdigest(group_key),
            query_id,
            aggregator_id,
            hashb
        ), encoding='utf-8')
        self.db.put(key, bytes(str(hashb), encoding='utf-8'))

    def get_first_group_key_for_query(self, query_id=None):
        """
        get the first group key for specified query_id
        :param query_id:
        :return:
        """
        if query_id is None:
            raise Exception('The query_id should not be None')
        it = self.db.iteritems()
        it.seek(bytes(query_id, encoding='utf-8'))
        for elt in it:
            elem = self.format_entry(elt)
            if elem[0].startswith(query_id):
                return elem
            else:
                return None

    def remove_group_key_for_query(self, query_id, group_key):
        """
        If you remove a group key you remove everything attach to it
        for a specified query_id provided
        :param query_id:
        :param group_key:
        :return:
        """
        key = bytes("{}_{}".format(query_id, xxhash.xxh64_hexdigest(group_key)), encoding='utf-8')
        self.db.delete(key=key)
        # for a group_key remove all bindings attached to it
        key = "{}_{}".format(xxhash.xxh64_hexdigest(group_key), query_id)
        it = self.iterator(key)
        for elt in it:
            k, v = self.format_entry(elt, jsonLoad=False)
            if k.startswith(key):
                self.db.delete(elt[0])
            else:
                break

    def set(self, query_id, group_key=None, value=None):
        """
        Set a value with key equal to <query_id> + '_' + hash(group_key)
        :param query_id:
        :param group_key:
        :param value:
        :return:
        """
        key = bytes("{}_{}".format(
            query_id,
            xxhash.xxh64_hexdigest(group_key)
        ), encoding = 'utf-8')
        self.db.put(key, bytes(json.dumps(value), encoding='utf-8'))

    def has(self, query_id=None, group_key=None):
        """
        Check if a group key already exist or not using the key_may_exist function of rocksdb
        it uses a the bloom filter of rocks db to be lighter than calling the get method.
        But be aware that the element return could not be the one you expected as it could be
        a false positive.
        Return a couple (bool, val|None) which can be (False, None) or (True, None), (True, elem)
        :param query_id:
        :param group_key:
        :return:
        """
        key = bytes("{}_{}".format(
            query_id,
            xxhash.xxh64_hexdigest(group_key)
        ), encoding = 'utf-8')
        (check, val) = self.db.key_may_exist(key=key, fetch=True)
        if check:
            # bloom filter possible false positive, we need to check if is in the database
            val = self.db.get(key)
            if val is None:
                return False, None
            else:
                return True, json.loads(val.decode('utf-8'))
        else:
            return False, None

    def get(self, query_id=None, group_key=None):
        """
        Get a group_key and every information attach to it.
        => {
                'bindings': <last_bindings_red>,
                <'agg1_binsto_key': agg1.val,
                ...,
                'aggn_bindsto_key': aggn.val>
            }
        :param ID:
        :param group_key:
        :return:
        """
        val = self.db.get(bytes("{}_{}".format(query_id, xxhash.xxh64_hexdigest(group_key)), encoding='utf-8'))
        return json.loads(val.decode(encoding='utf-8'))

    def iterator(self, ID=None):
        """
        Return an iterator on rocks db at prefix = ID, with sorted elements by their keys
        :param ID:
        :return:
        """
        it = self.db.iteritems()
        it.seek(bytes(ID, encoding='utf-8'))
        return it

    def format_entry(self, elt, jsonLoad=True):
        if jsonLoad:
            return elt[0].decode('utf-8'), json.loads(elt[1].decode('utf-8'))
        else:
            return elt[0].decode('utf-8'), elt[1].decode('utf-8')