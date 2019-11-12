from __future__ import annotations
from threading import Lock
from typing import Optional
import cachetools
import sys

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
    def __init__(self, global_limit, local_limit):
        super(BoundedBuffer, self).__init__()
        self._global_limit = global_limit
        self._local_limit = local_limit
        self._current_global_limit = 0
        self._current_local_limit = 0
        print("[BoundedBuffer] with global_limit={} and local_limit={}".format(global_limit, local_limit))
        self._caches = dict()

    def get(self, agg_id, key):
        self._init(agg_id)
        return self._caches.get(agg_id).get(key)

    def set(self, agg_id, key, value):
        self._init(agg_id)
        self._caches.get(agg_id)[key] = value

    def _init(self, agg_id):
        if not agg_id in self._caches:
            self._caches[agg_id] = Cache(parent=self, maxsize=self._local_limit, getsizeof=self._getsizeof)

    def _getsizeof(self, value):
        return self._get_size(value)

    def _get_size(self, obj, seen=None):
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
            size += sum([self._get_size(v, seen) for v in obj.values()])
            size += sum([self._get_size(k, seen) for k in obj.keys()])
        elif hasattr(obj, '__dict__'):
            size += self._get_size(obj.__dict__, seen)
        elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes, bytearray)):
            size += sum([self._get_size(i, seen) for i in obj])
        print("sizeof({})={}", obj, size)
        return size

class Cache(cachetools.LRUCache):
    def __init__(self, parent, maxsize, getsizeof):
        super(Cache, self).__init__(maxsize=maxsize, getsizeof=getsizeof)
        self._parent = parent
        print("[Cache({}, {}, {})]".format(parent, maxsize, getsizeof))

    def popitem(self):
        key, value = super().popitem()
        print('Key "%s" evicted with value "%s"' % (key, value))
        return key, value




