from rdflib import Literal
from rdflib.namespace import XSD
from hyperloglog import HyperLogLog
from sys import exit
import logging

logger = logging.getLogger()

class AggregationReducer():

    def __init__(self):
        self._groups = dict()

    def create_group(self, group_key, aggregation):
        raise Exception('This method must be implemented by the subclasses')

    def update_group(self, group_key, aggregation):
        raise Exception('This method must be implemented by the subclasses')

    def accumulate(self, group_key, aggregation):
        if group_key not in self._groups:
            self.create_group(group_key, aggregation)
        else:
            self.update_group(group_key, aggregation)

    def result(self, group_key):
        raise Exception('This method must be implemented by the subclasses')


class CountReducer(AggregationReducer):

    def __init__(self):
        super(CountReducer, self).__init__()

    def create_group(self, group_key, aggregation):
        self._groups[group_key] = aggregation['__value__']

    def update_group(self, group_key, aggregation):
        self._groups[group_key] += aggregation['__value__']

    def result(self, group_key):
        return Literal(self._groups[group_key], datatype=XSD.integer).n3()


class ApproximateCountDistinctReducer(AggregationReducer):

    def __init__(self):
        super(ApproximateCountDistinctReducer, self).__init__()

    def create_group(self, group_key, aggregation):
        hyperloglog = HyperLogLog(0.01)
        hyperloglog.load(aggregation['__value__'])
        self._groups[group_key] = hyperloglog

    def update_group(self, group_key, aggregation):
        hyperloglog = HyperLogLog(0.01)
        hyperloglog.load(aggregation['__value__'])
        self._groups[group_key].update(hyperloglog)

    def result(self, group_key):
        card = int(self._groups[group_key].card())
        return Literal(card, datatype=XSD.integer).n3()


class CountDistinctReducer(AggregationReducer):

    def __init__(self, bind_variable):
        super(CountDistinctReducer, self).__init__()

    def create_group(self, group_key, aggregation):
        self._groups[group_key] = aggregation['__value__']

    def update_group(self, group_key, aggregation):
        self._groups[group_key].extend(aggregation['__value__'])

    def result(self, group_key):
        seen = set()
        seq = self._groups[group_key]
        distinct = [x for x in seq if x not in seen and not seen.add(x)]
        return Literal(len(distinct), datatype=XSD.integer).n3()


# class Reducer():

#     def __init__(self, bind_variable):
#         self._bind_variable = bind_variable
#         self._groups = dict()

#     def create_group(self, group_key, bindings):
#         raise Exception('This method must be implemented by the subclasses')

#     def update_group(self, group_key, bindings):
#         raise Exception('This method must be implemented by the subclasses')

#     def accumulate(self, bindings):
#         if not self._bind_variable in bindings:
#             logger.warning('missing the aggregation variable in bindings')
#             return
#         elif not '?__group_key' in bindings:
#             logger.warning('missing the group key in bindings')
#             return
#         group_key = bindings['?__group_key']
#         if group_key not in self._groups:
#             self.create_group(group_key, bindings)
#         else:
#             self.update_group(group_key, bindings)

#     def result():
#         raise Exception('This method must be implemented by the subclasses')

# class CountReducer(Reducer):

#     def __init__(self, bind_variable):
#         super(CountReducer, self).__init__(bind_variable)

#     def create_group(self, group_key, bindings):
#         group = dict()
#         for variable in bindings.keys():
#             if variable != '?__group_key':
#                 group[variable] = bindings[variable]
#         self._groups[group_key] = group

#     def merge(self, x, y):
#         val_x = int(x.split('^^')[0].replace('"', ''))
#         val_y = int(y.split('^^')[0].replace('"', ''))
#         return Literal(val_x + val_y).n3()

#     def update_group(self, group_key, bindings):
#         group = self._groups[group_key]
#         counter = group[self._bind_variable]
#         group[self._bind_variable] = self.merge(counter, bindings[self._bind_variable])
#         self._groups[group_key] = group

#     def result(self):
#         res = list()
#         for item in self._groups.values():
#             elt = dict()
#             for variable in item.keys():
#                 elt[variable] = item[variable]
#             res.append(elt)
#         return res

# class ApproximateCountDistinctReducer(Reducer):

#     def __init__(self, bind_variable):
#         super(ApproximateCountDistinctReducer, self).__init__(bind_variable)

#     def create_group(self, group_key, bindings):
#         group = dict()
#         for variable in bindings.keys():
#             if variable != '?__group_key' and variable != self._bind_variable:
#                 group[variable] = bindings[variable]
#         hloglog = HyperLogLog(0.01)
#         hloglog.load(bindings[self._bind_variable]['__value__'])
#         group[self._bind_variable] = hloglog
#         self._groups[group_key] = group

#     def merge(self, x, y):
#         x.update(y)
#         return x

#     def update_group(self, group_key, bindings):
#         group = self._groups[group_key]
#         counter = group[self._bind_variable]
#         hloglog = HyperLogLog(0.01)
#         hloglog.load(bindings[self._bind_variable]['__value__'])
#         group[self._bind_variable] = self.merge(counter, hloglog)
#         self._groups[group_key] = group

#     def result(self):
#         res = list()
#         for item in self._groups.values():
#             elt = dict()
#             for variable in item.keys():
#                 if variable != self._bind_variable:
#                     elt[variable] = item[variable]
#             elt[self._bind_variable] = Literal(int(item[self._bind_variable].card())).n3()
#             res.append(elt)
#         return res

# class CountDistinctReducer(Reducer):

#     def __init__(self, bind_variable):
#         super(CountDistinctReducer, self).__init__(bind_variable)

#     def create_group(self, group_key, bindings):
#         group = dict()
#         for variable in bindings.keys():
#             if variable != '?__group_key' and variable != self._bind_variable:
#                 group[variable] = bindings[variable]
#         hloglog = HyperLogLog(0.01)
#         hloglog.load(bindings[self._bind_variable]['__value__'])
#         group[self._bind_variable] = hloglog
#         self._groups[group_key] = group

#     def merge(self, x, y):
#         x.update(y)
#         return x

#     def update_group(self, group_key, bindings):
#         group = self._groups[group_key]
#         counter = group[self._bind_variable]
#         hloglog = HyperLogLog(0.01)
#         hloglog.load(bindings[self._bind_variable]['__value__'])
#         group[self._bind_variable] = self.merge(counter, hloglog)
#         self._groups[group_key] = group

#     def result(self):
#         res = list()
#         for item in self._groups.values():
#             elt = dict()
#             for variable in item.keys():
#                 if variable != self._bind_variable:
#                     elt[variable] = item[variable]
#             elt[self._bind_variable] = Literal(int(item[self._bind_variable].card())).n3()
#             res.append(elt)
#         return res



class GenericReducer():

    def __init__(self):
        self._groups = dict()
        self._count = CountReducer()
        self._count_distinct = CountDistinctReducer('')
        self._approximative_count_distinct = ApproximateCountDistinctReducer()

    def accumulate_aggregation(self, group_key, aggregation):
        kind = aggregation['__type__']
        print(kind)
        if kind == 'count':
            self._count.accumulate(group_key, aggregation)
        elif kind == 'count-distinct':
            self._count_distinct.accumulate(group_key, aggregation)
        elif kind == 'approximative-count-distinct':
            self._approximative_count_distinct.accumulate(group_key, aggregation)
        else:
            logger.error(f'Unknwon aggregation type: {kind}')
            exit(1)

    def create_group(self, group_key, bindings):
        group = dict()
        for variable in bindings.keys():
            if variable == '?__group_key':
                continue # the group key must not appear in the final bindings
            if type(bindings[variable]) is str:
                group[variable] = bindings[variable]
            elif type(bindings[variable]) is dict:
                self.accumulate_aggregation(group_key, bindings[variable])
                group[variable] = {'__type__': bindings[variable]['__type__']}
            else:
                logger.error('Malformed bindings')
                exit(1)
        self._groups[group_key] = group

    def update_group(self, group_key, bindings):
        for variable in bindings.keys():
            if type(bindings[variable]) is dict:
                self.accumulate_aggregation(group_key, bindings[variable])

    def accumulate(self, bindings):
        if not '?__group_key' in bindings:
            logger.warning('Group key is missing')
            exit(1)
        group_key = bindings['?__group_key']
        if group_key not in self._groups:
            self.create_group(group_key, bindings)
        else:
            self.update_group(group_key, bindings)

    def reduce_aggregation(self, group_key, aggregation):
        kind = aggregation['__type__']
        if kind == 'count':
            return self._count.result(group_key)
        elif kind == 'count-distinct':
            return self._count_distinct.result(group_key)
        elif kind == 'approximative-count-distinct':
            return self._approximative_count_distinct.result(group_key)
        else:
            logger.error(f'Unknwon aggregation type: {kind}')
            exit(1)

    def result(self):
        res = list()
        for key, item in self._groups.items():
            elt = dict()
            for variable in item.keys():
                if type(item[variable]) is str:
                    elt[variable] = item[variable]
                elif type(item[variable]) is dict:
                    elt[variable] = self.reduce_aggregation(key, item[variable])
                else:
                    logger.error('Malformed item')
                    exit(1)
            res.append(elt)
        return res

    def len(self):
        return len(self._groups.keys())