# distinct.py
# Author: Thomas MINIER - MIT License 2017-2018
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.protobuf.iterators_pb2 import SavedDistinctIterator
from sage.query_engine.iterators.utils import IteratorExhausted


def hash_bindings(bindings):
    res = ""
    for key, v in bindings.items():
        res += "{}={};".format(key, v)
    return res


class DistinctIterator(PreemptableIterator):
    """A DistinctIterator evaluates a DISTINCT solution modifiers"""

    def __init__(self, source):
        super(DistinctIterator, self).__init__()
        self._source = source
        self._seen_before = dict()

    def __repr__(self):
        return "<DistinctIterator on {}>".format(self._source)

    def serialized_name(self):
        return "distinct"

    def _evaluate(self, bindings):
        """Evaluate the FILTER expression with a set mappings"""
        hashed = hash_bindings(bindings)
        if hashed in self._seen_before:
            return False
        self._seen_before[hashed] = 1
        return True

    async def next(self):
        if not self.has_next():
            raise IteratorExhausted()
        mu = await self._source.next()
        return mu if self._evaluate(mu) else None

    def has_next(self):
        """Return True if the iterator has more item to yield"""
        return self._source.has_next()

    def save(self):
        """Save and serialize the iterator as a machine-readable format"""
        saved_distinct = SavedDistinctIterator()
        source_field = self._source.serialized_name() + '_source'
        getattr(saved_distinct, source_field).CopyFrom(self._source.save())
        return saved_distinct
