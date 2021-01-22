from rdflib import Literal
from rdflib.namespace import XSD
from hyperloglog import HyperLogLog
import logging, sys

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
    """
    This class is used to merge the count partial aggregations
    """

    def __init__(self):
        super(CountReducer, self).__init__()

    def create_group(self, group_key, aggregation):
        self._groups[group_key] = aggregation['__value__']

    def update_group(self, group_key, aggregation):
        self._groups[group_key] += aggregation['__value__']

    def result(self, group_key):
        return Literal(self._groups[group_key], datatype=XSD.integer).n3()


class ApproximateCountDistinctReducer(AggregationReducer):
    """
    This class is used to merge the count distinct partial aggregations
    when approximations are enabled
    """

    def __init__(self):
        super(ApproximateCountDistinctReducer, self).__init__()

    def create_group(self, group_key, aggregation):
        hyperloglog = HyperLogLog(0.01) # do not care about the error rate, it will be overwritten
        hyperloglog.load(aggregation['__value__'])
        self._groups[group_key] = hyperloglog

    def update_group(self, group_key, aggregation):
        hyperloglog = HyperLogLog(0.01) # do not care about the error rate, it will be overwritten
        hyperloglog.load(aggregation['__value__'])
        self._groups[group_key].update(hyperloglog)

    def result(self, group_key):
        card = int(self._groups[group_key].card())
        return Literal(card, datatype=XSD.integer).n3()


class CountDistinctReducer(AggregationReducer):
    """
    This class is used to merge the count distinct partial aggregations
    """

    def __init__(self):
        super(CountDistinctReducer, self).__init__()

    def create_group(self, group_key, aggregation):
        self._groups[group_key] = set()
        for elt in aggregation['__value__']:
            self._groups[group_key].add(elt)

    def update_group(self, group_key, aggregation):
        for elt in aggregation['__value__']:
            self._groups[group_key].add(elt)

    def result(self, group_key):
        return Literal(len(self._groups[group_key]), datatype=XSD.integer).n3()


class GenericReducer():
    """
    This class merges the partial aggregations computed by the server

    To be able to merge partial aggregations, solution mappings must
    have the following shape:

    { ?__group_key: '_', ?v1: '_', ..., ?vn: '_', ?c1: { '__type__': '_', '__value__': '_' }, ..., cn: { '__type__': '_', '__value__': '_' } }

    where:
    - ?__group_key is used to identify each partial aggregation
    - ?v1...?vn are random variables
    - ?c1...?cn are the variables used to bind the result of the aggregations
    - __type__ is used to identify an aggregation function. It can take the following values:
        - count
        - count-distinct
        - approximative-count-distinct
    - __value__ is the value of a partial aggregation
    """

    def __init__(self):
        self._groups = dict()
        self._count = CountReducer()
        self._count_distinct = CountDistinctReducer()
        self._approximative_count_distinct = ApproximateCountDistinctReducer()

    def accumulate_aggregation(self, group_key, aggregation):
        kind = aggregation['__type__']
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
                self.accumulate_aggregation(f'{group_key}-{variable}', bindings[variable])
                group[variable] = {'__type__': bindings[variable]['__type__']}
            else:
                logger.error('Malformed bindings')
                sys.exit(1)
        self._groups[group_key] = group

    def update_group(self, group_key, bindings):
        for variable in bindings.keys():
            if type(bindings[variable]) is dict:
                self.accumulate_aggregation(f'{group_key}-{variable}', bindings[variable])

    def accumulate(self, bindings):
        if not '?__group_key' in bindings:
            logger.warning('Group key is missing')
            return
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
            sys.exit(1)

    def get(self):
        res = list()
        for key, item in self._groups.items():
            elt = dict()
            for variable in item.keys():
                if type(item[variable]) is str:
                    elt[variable] = item[variable]
                elif type(item[variable]) is dict:
                    elt[variable] = self.reduce_aggregation(f'{key}-{variable}', item[variable])
                else:
                    logger.error('Malformed item')
                    sys.exit(1)
            res.append(elt)
        return res

    def size(self):
        return len(self._groups.keys())