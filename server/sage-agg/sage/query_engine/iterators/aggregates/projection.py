# projection.py
# Author: Thomas MINIER - MIT License 2017-2018
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.iterators.utils import IteratorExhausted
from sage.query_engine.protobuf.iterators_pb2 import SavedAggregatesProjectionIterator


class AggregatesProjectionIterator(PreemptableIterator):
    """
    An AggregatesProjectionIterator performs a projection over solution mappings for aggregation queries
    """

    def __init__(self, source, dataset, default_graph, values=None):
        super(AggregatesProjectionIterator, self).__init__()
        self._source = source
        self._dataset = dataset
        self._default_graph = default_graph
        self._values = values

    def __repr__(self):
        return '<AggregatesProjectionIterator SELECT %s FROM { %s }>' % (self._values, self._source)

    def serialized_name(self):
        return "agg_proj"

    def is_aggregator(self):
        return True

    def has_next(self):
        return self._source.has_next()

    def generate_results(self):
        """
            Default behavior when the optimization is not applied
            Results are generated after each quantum
            see sage.sage_engine.py
        """
        bindings_list = self._source.generate_results()
        if self._values is None:
            return [{}] * len(bindings_list)
        graph = self._dataset.get_graph(self._default_graph)
        return [{k: graph.get_value(v) for k, v in value.items() if (k in self._values or k == '?__group_key')} for value in bindings_list]

    async def next(self):
        """
        Get the next item from the iterator, reading from the left source and then the right source
        """
        if not self.has_next():
            raise IteratorExhausted()
        await self._source.next()
        return None

    def save(self):
        """Save and serialize the iterator as a machine-readable format"""
        saved_proj = SavedAggregatesProjectionIterator()
        saved_proj.graph = self._default_graph
        saved_proj.values.extend(self._values)
        source_field = self._source.serialized_name() + '_source'
        getattr(saved_proj, source_field).CopyFrom(self._source.save())
        return saved_proj
