from __future__ import annotations

import collections
import sys
import uuid
from threading import Lock
from typing import Optional

import cachetools


class SingletonMeta(type):
    """
    This is a thread-safe implementation of Singleton.
    """
    _instance: Optional[BoundedBuffer] = None
    _lock: Lock = Lock()

    def __call__(cls, *args, **kwargs):
        with cls._lock:
            if not cls._instance:
                cls._instance = super().__call__(*args, **kwargs)
        return cls._instance


class BoundedBuffer(metaclass=SingletonMeta):
    def __init__(self):
        super(BoundedBuffer, self).__init__()
        self._caches = dict()
        self._done = dict()
        self._id = str(uuid.uuid4())

    def set(self, query_id, agg_id, group_key, value):
        try:
            if not query_id in self._caches:
                self._create(query_id)
            if not group_key in self._caches[query_id]:
                self._caches[query_id][group_key] = dict()
            if not agg_id in self._caches[query_id][group_key]:
                self._caches[query_id][group_key][agg_id] = dict()
            self._caches[query_id][group_key][agg_id] = value
        except Exception as err:
            print(err)
            exit(1)

    def has(self, query_id, agg_id, group_key):
        if not query_id in self._caches:
            return False
        if not group_key in self._caches[query_id]:
            return False
        else:
            return agg_id in self._caches[query_id][group_key]

    def get(self, query_id, agg_id, group_key):
        if not query_id in self._caches:
            return None
        if not group_key in self._caches[query_id]:
            return None
        if not agg_id in self._caches[query_id][group_key]:
            return None
        else:
            return self._caches[query_id][group_key][agg_id]

    def get_evicted(self, query_id, buffer=0):
        if not query_id in self._caches:
            return None
        else:
            return self._caches[query_id].evict(buffer)

    def get_all(self, query_id, buffer=0):
        if not query_id in self._caches:
            return None
        else:
            return self._caches[query_id].evict(buffer)

    def clear(self, query_id):
        if query_id in self._caches:
            del self._caches[query_id]

    def _create(self, query_id):
        self._caches[query_id] = Cache(maxsize=0, getsizeof=self.getsizeof)

    def getsizeof(self, obj, seen=None):
        """Recursively finds size of objects in bytes"""
        size = sys.getsizeof(obj)
        if seen is None:
            seen = set()
        obj_id = id(obj)
        if obj_id in seen:
            return 0
        # Important mark as seen *before* entering recursion to gracefully handle
        # self-referential objects
        seen.add(obj_id)
        if isinstance(obj, dict):
            size += sum([self.getsizeof(v, seen) for v in obj.values()])
            size += sum([self.getsizeof(k, seen) for k in obj.keys()])
        elif hasattr(obj, '__dict__'):
            size += self.getsizeof(obj.__dict__, seen)
        elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes, bytearray)):
            size += sum([self.getsizeof(i, seen) for i in obj])
        return size


class Cache(cachetools.Cache):
    def __init__(self, maxsize=1000000000, getsizeof=None):
        cachetools.Cache.__init__(self, maxsize, getsizeof)
        self.__order = collections.OrderedDict()

    def __getitem__(self, key, cache_getitem=cachetools.Cache.__getitem__):
        value = cache_getitem(self, key)
        self.__update(key)
        return value

    def __setitem__(self, key, value, cache_setitem=None):
        self.__data[key] = value
        self.__update(key)

    def __delitem__(self, key, cache_delitem=cachetools.Cache.__delitem__):
        cache_delitem(self, key)
        del self.__order[key]

    def popitem(self):
        """Remove and return the `(key, value)` pair least recently used."""
        try:
            key = next(iter(self.__order))
        except StopIteration:
            raise KeyError('%s is empty' % self.__class__.__name__)
        else:
            return key, self.pop(key)

    def evict(self, size):
        evicted = dict()
        # update currsize
        self.__currsize = 0
        for k, v in self.__data.items():
            self.__size[k] = self.getsizeof(v)
            self.__currsize += self.__size[k]

        # print("size:", self.currsize, size)

        if self.__currsize > size:
            while self.__currsize > size:
                k, v = self.popitem()
                # print("evicted", k, self.getsizeof(v))
                evicted[k] = v

        # print("remaining size:", self.currsize, self.maxsize)

        return evicted

    if hasattr(collections.OrderedDict, 'move_to_end'):
        def __update(self, key):
            try:
                self.__order.move_to_end(key)
            except KeyError:
                self.__order[key] = None
    else:
        def __update(self, key):
            try:
                self.__order[key] = self.__order.pop(key)
            except KeyError:
                self.__order[key] = None
