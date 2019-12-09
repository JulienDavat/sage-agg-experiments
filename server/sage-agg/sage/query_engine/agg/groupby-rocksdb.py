# groupby.py
# Author: Thomas MINIER - MIT License 2017-2019
import os
import subprocess

from sage.query_engine.agg.index_disk_rocksdb import IndexRocksdb
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
        self._default_key = 'https://sage-org.github.io/sage-engine#DefaultGroupKey'
        self._optimized = False
        self._optimized_disk = False
        self._finished = False
        self._has_next = True
        self._current = 0
        self._bounded = False

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
        # build groups & apply aggregations on the fly for non distinct aggregation
        for key, values in self._groups.items():
            elt = dict()
            # recopy keys
            if len(self._grouping_variables) == 0:
                elt["?__default_group"] = self._default_key
            else:
                for variable in self._grouping_variables:
                    elt[variable] = values[0][variable]
            # # export the groups of bindings if needed
            # if self._keep_groups:
            #    elt['?__group_values'] = values
            # apply aggregators
            for agg in self._aggregators:
                try:
                    if not agg.is_distinct():
                        if self._bounded:
                            elt[agg.get_binds_to()] = agg.done_bounded(key)
                        else:
                            elt[agg.get_binds_to()] = agg.done(key)
                    else:
                        if self._bounded:
                            elt['?__group_values'] = agg.done_bounded(key)
                        else:
                            elt['?__group_values'] = agg.done_bounded(key)
                except Exception:
                    # ignore errors
                    pass
            # add results
            res.append(elt)
        return res

    def has_next(self):
        return self._has_next

    async def next(self):
        # Phase 1: aggregate solutions mappings
        if self._source.has_next():
            # print('phase 1')
            bindings = await self._source.next()
            group_key = self.__get_group_key(bindings)
            if group_key is not None:
                if group_key not in self._groups:
                    self._groups[group_key] = list()
                self._groups[group_key].append(bindings)
                # update aggregators with the new value
                for agg in self._aggregators:
                    try:
                        if self._bounded:
                            agg.update_bounded(group_key, bindings)
                        else:
                            agg.update(group_key, bindings)
                        self._current += 1
                    except Exception:
                        # ignore errors
                        pass
            return None
        else:
            # print('phase 2')
            if self._optimized and self._optimized_disk:
                # Phase 2: produce aggregations results
                if not self.has_next():
                    return None
                else:
                    # all aggregator will have the same behavior
                    # just get the first group_key available
                    # if None return None and put the flag finished to True and hasNext to false
                    agg = self._aggregators[0]
                    elem = agg.get_first_group_key()
                    if elem is None:
                        self._finished = True
                        self._has_next = False
                        agg.close()
                        return None
                    else:
                        # now for all aggregator get the result
                        elt = dict()
                        # recopy keys
                        if len(self._grouping_variables) == 0:
                            elt["?__default_group"] = self._default_key
                        else:
                            for variable in self._grouping_variables:
                                elt[variable] = elem[1]['bindings'][variable]
                        for agg in self._aggregators:
                            elt[agg.get_binds_to()] = agg.done_bounded(elem[1])

                        return await self._remove_and_return(group_key=elem[1]['group_key'], value=elt)

        self._finished = True
        self._has_next = False
        return None

    async def _remove_and_return(self, group_key=None, value=None):
        agg = self._aggregators[0]
        agg.remove_group_key(group_key)
        return value

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

    def set_optimization(self, opt=False, opt_disk=False):
        self._optimized = opt
        self._optimized_disk = opt_disk

    def is_aggregator(self):
        return True

    def __repr__(self):
        return "<GroupByAggregator({}) on {}>".format(self._grouping_variables, self._source)

    def get_db_size(self):
        if os.path.exists(IndexRocksdb._location):
            return int(subprocess.check_output(['du', '-s', IndexRocksdb._location]).split()[0].decode('utf-8')) * 512
        else:
            return 0
