# loader.py
# Author: Thomas MINIER - MIT License 2017-2018
from sage.query_engine.iterators.aggregates.count import CountAggregator
from sage.query_engine.iterators.aggregates.count_distinct import CountDistinctAggregator
from sage.query_engine.iterators.aggregates.approximative_count_distinct import ApproximateCountDistinctAggregator
from sage.query_engine.iterators.aggregates.groupby import GroupByAggregator
from sage.query_engine.iterators.aggregates.min_max import MinAggregator, MaxAggregator
from sage.query_engine.iterators.aggregates.sum import SumAggregator
from sage.query_engine.iterators.aggregates.projection import AggregatesProjectionIterator
from sage.query_engine.iterators.filter import FilterIterator
from sage.query_engine.iterators.nlj import IndexJoinIterator
from sage.query_engine.iterators.projection import ProjectionIterator
from sage.query_engine.iterators.scan import ScanIterator
from sage.query_engine.iterators.union import BagUnionIterator
from sage.query_engine.protobuf.iterators_pb2 import RootTree, SavedProjectionIterator, SavedScanIterator, \
    SavedIndexJoinIterator, SavedBagUnionIterator, SavedFilterIterator, SavedGroupByAgg, SavedAggregatesProjectionIterator
from sage.query_engine.protobuf.utils import protoTriple_to_dict


def load(protoMsg, dataset):
    """Load a preemptable physical query execution plan from a saved state"""
    saved_plan = protoMsg
    if isinstance(protoMsg, bytes):
        root = RootTree()
        root.ParseFromString(protoMsg)
        sourceField = root.WhichOneof('source')
        saved_plan = getattr(root, sourceField)
    if type(saved_plan) is SavedFilterIterator:
        return load_filter(saved_plan, dataset)
    if type(saved_plan) is SavedProjectionIterator:
        return load_projection(saved_plan, dataset)
    elif type(saved_plan) is SavedScanIterator:
        return load_scan(saved_plan, dataset)
    elif type(saved_plan) is SavedIndexJoinIterator:
        return load_nlj(saved_plan, dataset)
    elif type(saved_plan) is SavedBagUnionIterator:
        return load_union(saved_plan, dataset)
    elif type(saved_plan) is SavedGroupByAgg:
        return load_groupby(saved_plan, dataset)
    elif type(saved_plan) is SavedAggregatesProjectionIterator:
        return load_aggregates_projection(saved_plan, dataset)
    else:
        raise Exception('Unknown iterator type "%s" when loading controls' % type(saved_plan))


def load_projection(saved_plan, dataset):
    """Load a ProjectionIterator from a protobuf serialization"""
    sourceField = saved_plan.WhichOneof('source')
    source = load(getattr(saved_plan, sourceField), dataset)
    values = saved_plan.values if len(saved_plan.values) > 0 else None
    return ProjectionIterator(source, values)


def load_filter(saved_plan, dataset):
    """Load a FilterIterator from a protobuf serialization"""
    sourceField = saved_plan.WhichOneof('source')
    source = load(getattr(saved_plan, sourceField), dataset)
    mu = None
    if len(saved_plan.mu) > 0:
        mu = saved_plan.mu
    return FilterIterator(source, saved_plan.expression, mu=mu)


def load_scan(saved_plan, dataset):
    """Load a ScanIterator from a protobuf serialization"""
    triple = saved_plan.triple
    s, p, o, g = (triple.subject, triple.predicate, triple.object, triple.graph)
    iterator, card = dataset.get_graph(g).search(s, p, o, last_read=saved_plan.last_read)
    return ScanIterator(iterator, protoTriple_to_dict(triple), saved_plan.cardinality)


def load_nlj(saved_plan, dataset):
    """Load a IndexJoinIterator from a protobuf serialization"""
    currentBinding = None
    sourceField = saved_plan.WhichOneof('source')
    source = load(getattr(saved_plan, sourceField), dataset)
    innerTriple = protoTriple_to_dict(saved_plan.inner)
    if len(saved_plan.muc) > 0:
        currentBinding = saved_plan.muc
    dataset = dataset.get_graph(innerTriple['graph'])
    return IndexJoinIterator(source, innerTriple, dataset, currentBinding=currentBinding,
                             iterOffset=saved_plan.last_read)


def load_union(saved_plan, dataset):
    """Load a BagUnionIterator from a protobuf serialization"""
    leftField = saved_plan.WhichOneof('left')
    left = load(getattr(saved_plan, leftField), dataset)
    rightField = saved_plan.WhichOneof('right')
    right = load(getattr(saved_plan, rightField), dataset)
    return BagUnionIterator(left, right)


def load_groupby(saved_plan, dataset):
    """Load a GroupByAggregator from a protobuf serialization"""
    sourceField = saved_plan.WhichOneof('source')
    source = load(getattr(saved_plan, sourceField), dataset)
    aggregators = list()
    for agg in saved_plan.aggregators:
        if agg.name == 'count':
            aggregators.append(CountAggregator(agg.variable, binds_to=agg.binds_to))
        elif agg.name == 'count-disk':
            aggregators.append(CountDiskAggregator(agg.variable, binds_to=agg.binds_to))
        elif agg.name == 'count-distinct':
            aggregators.append(CountDistinctAggregator(agg.variable, binds_to=agg.binds_to))
        elif agg.name == 'approximative-count-distinct':
            error_rate = dataset.error_rate
            aggregators.append(ApproximateCountDistinctAggregator(agg.variable, binds_to=agg.binds_to, error_rate=error_rate))
        elif agg.name == 'count-distinct-disk':
            aggregators.append(CountDistinctDiskAggregator(agg.variable, binds_to=agg.binds_to))
        elif agg.name == 'sum':
            aggregators.append(SumAggregator(agg.variable, binds_to=agg.binds_to))
        elif agg.name == 'min':
            aggregators.append(MinAggregator(agg.variable, binds_to=agg.binds_to))
        elif agg.name == 'max':
            aggregators.append(MaxAggregator(agg.variable, binds_to=agg.binds_to))
        else:
            raise Exception("Unknown SPARQL aggregators of type '{}' found when resuming query.".format(agg.name))
    return GroupByAggregator(source, saved_plan.variables, aggregators=aggregators)

def load_aggregates_projection(saved_plan, dataset):
    """Load a AggregatesProjectionIterator from a protobuf serialization"""
    sourceField = saved_plan.WhichOneof('source')
    source = load(getattr(saved_plan, sourceField), dataset)
    values = saved_plan.values if len(saved_plan.values) > 0 else None
    return AggregatesProjectionIterator(source, values)