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

    def __init__(self, source, grouping_variables, aggregators=list(), keep_groups=False, buffer=-1):
        super(GroupByAggregator, self).__init__()
        self._source = source
        self._grouping_variables = grouping_variables
        self._groups = dict()
        self._aggregators = aggregators
        self._keep_groups = keep_groups
        self._default_key = 'https://sage-org.github.io/sage-engine#DefaultGroupKey'
        self._optimized = False
        self._buffer_size = buffer
        self._finished = False
        self._has_next = True
        self._current = 0

    @property
    def bounded(self):
        return self._buffer_size > -1

    @property
    def buffer_size(self):
        return self._buffer_size

    def serialized_name(self):
        return 'groupby'

    def __get_group_key(self, bindings):
        """Get the grouping key for a set of bindings"""
        if len(self._grouping_variables) == 0:
            return self._default_key
        elif len(self._grouping_variables) == 1:
            return bindings[self._grouping_variables[0]] if self._grouping_variables[0] in bindings else None
        key = ''
        # for variable in self._grouping_variables:
        #     if variable not in bindings:
        #         return None
        #     key += "{}_".format(bindings[variable])
        for variable in self._grouping_variables:
            if variable in bindings:
                key += "{}_".format(bindings[variable])
            else:
                # self._grouping_variables.remove(variable)
                key += "null_"
        # remove the trailing "_" in the group key value
        return key[0:-1]

    def generate_results(self):
        """
            Default behavior when the optimization is not applied
            Results are generated after each quantum
            see sage.sage_engine.py
        """
        res = list()
        # get groups
        if self.bounded:
            if self.has_next():
                # pop items until the size of the cache is less than the one provided
                groups = self._aggregators[0]._cache.get_evicted(self._aggregators[0].get_query_id(),
                                                                 buffer=self._buffer_size)
            else:
                # pop everything and return
                groups = self._aggregators[0]._cache.get_all(self._aggregators[0].get_query_id(), buffer=0)

            if groups is None:
                return res
        else:
            groups = self._groups
        # build groups & apply aggregations on the fly for non distinct aggregation
        for key, values in groups.items():
            elt = dict()
            # recopy keys
            if len(self._grouping_variables) == 0:
                elt["?__default_group"] = self._default_key
            else:
                if not self.bounded:
                    for variable in self._grouping_variables:
                        elt[variable] = values[0][variable]
            for agg in self._aggregators:
                try:
                    if self.bounded:
                        elt[agg.get_binds_to()], bindings = agg.done_bounded(key, groups)
                        for variable in self._grouping_variables:
                            elt[variable] = bindings[variable]
                    else:
                        elt[agg.get_binds_to()] = agg.done(key)
                except Exception as e:
                    print("(ERROR) generate_results(aggregations): ", e)
                    # ignore errors
                    pass
            # add results
            res.append(elt)

        # remove the bounded buffer for the current if not hasNext
        if not self.has_next() and self.bounded:
            self._aggregators[0]._cache.clear(self._aggregators[0].get_id())
        return res

    def has_next(self):
        return self._source.has_next()

    async def next(self):
        # Phase 1: aggregate solutions mappings
        if self.has_next():
            bindings = await self._source.next()
            group_key = self.__get_group_key(bindings)
            if group_key is not None:
                if group_key not in self._groups:
                    self._groups[group_key] = list()
                self._groups[group_key].append(bindings)
                # update aggregators with the new value
                for agg in self._aggregators:
                    try:
                        if self.bounded:
                            agg.update_bounded(group_key, bindings)
                        else:
                            agg.update(group_key, bindings)
                        self._current += 1
                    except Exception as e:
                        print("(ERROR) update:", e)
                        # ignore errors
                        pass
            else:
                return None
        else:
            return None

    def save(self):
        saved = SavedGroupByAgg()
        saved.source.CopyFrom(self._source.save())
        for variable in self._grouping_variables:
            saved.variables.append(variable)
        # save partial aggreggroupby.pyators
        for aggregator in self._aggregators:
            agg = saved.aggregators.add()
            agg.name = aggregator.get_type()
            agg.variable = aggregator.get_variable()
            agg.binds_to = aggregator.get_binds_to()
            agg.id = aggregator.get_id()
            agg.query_id = aggregator.get_query_id()
        return saved

    def set_optimization(self, opt=False, buffer=-1):
        self._optimized = opt
        self._buffer_size = buffer

    def is_aggregator(self):
        return True

    def __repr__(self):
        return "<GroupByAggregator({}) on {}>".format(self._aggregators, self._source)

    def get_db_size(self):
        return 0
        # if os.path.exists(IndexRocksdb._location):
        #     return int(subprocess.check_output(['du', '-s', IndexRocksdb._location]).split()[0].decode('utf-8')) * 512
        # else:
        #     return 0
