# groupby.py
# Author: Thomas MINIER - MIT License 2017-2019
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.protobuf.iterators_pb2 import SavedGroupByAgg


class GroupByAggregator(PreemptableIterator):
    """
        A GroupByAggregator evaluates a GROUP BY clause.

        Constructor args:
            - source [:class:`.ProjeectionIterator`] The source iterator
            - grouping_variables [`list(str)`] - GROUP BY variables
            - aggregators [`list(PartialAggregator)`] - Aggregators applied to groups
            - keep_groups [`bool`] - True if the groups should be sent alongside results, False otherwise
    """

    def __init__(self, source, grouping_variables, aggregators=list(), keep_groups=False):
        super(GroupByAggregator, self).__init__()
        self._source = source
        self._grouping_variables = grouping_variables
        self._groups = dict()
        self._aggregators = aggregators
        self._keep_groups = keep_groups

    def serialized_name(self):
        return 'groupby'

    def __get_group_key(self, bindings):
        """Get the grouping key for a set of bindings"""
        if len(self._grouping_variables) == 1:
            return bindings[self._grouping_variables[0]] if self._grouping_variables[0] in bindings else None
        key = ''
        for variable in self._grouping_variables:
            if variable not in bindings:
                return None
            key += "{}_".format(bindings[variable])
        return key[0:-1]

    def current_agg(self):
        res = list()
        # build groups & apply aggregations on the fly
        for key, values in self._groups.items():
            elt = dict()
            # recopy keys
            for variable in self._grouping_variables:
                elt[variable] = values[0][variable]
            # export the groups of bindings if needed
            if self._keep_groups:
                elt['?__group_values'] = values
            # apply aggregators
            for agg in self._aggregators:
                try:
                    elt[agg.get_binds_to()] = agg.done(key)
                except Exception:
                    # ignore errors
                    pass
            # add results
            res.append(elt)
        return res

    def has_next(self):
        return self._source.has_next()

    async def next(self):
        if not self._source.has_next():
            raise StopIteration()
        bindings = await self._source.next()
        group_key = self.__get_group_key(bindings)
        if group_key is not None:
            if group_key not in self._groups:
                self._groups[group_key] = list()
            self._groups[group_key].append(bindings)
            # update aggregators with the new value
            for agg in self._aggregators:
                agg.update(group_key, bindings)
        return None

    def save(self):
        saved = SavedGroupByAgg()
        saved.source.CopyFrom(self._source.save())
        for variable in self._grouping_variables:
            saved.variables.append(variable)
        # save partial aggregators
        for aggregator in self._aggregators:
            agg = saved.aggregators.add()
            agg.name = aggregator.get_type()
            agg.variable = aggregator.get_variable()
            agg.binds_to = aggregator.get_binds_to()
        return saved

    def __repr__(self):
        return "<GroupByAggregator({}) on {}>".format(self._grouping_variables, self._source)
