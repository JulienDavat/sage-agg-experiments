# query_parser.py
# Author: Thomas MINIER - MIT License 2017-2018
import uuid

from rdflib import URIRef, BNode
from rdflib.plugins.sparql.algebra import translateQuery
from rdflib.plugins.sparql.parser import parseQuery
from sage.http_server.utils import format_graph_uri
from sage.query_engine.iterators.aggregates.count import CountAggregator
from sage.query_engine.iterators.aggregates.count_distinct import CountDistinctAggregator
from sage.query_engine.iterators.aggregates.approximative_count_distinct import ApproximateCountDistinctAggregator
from sage.query_engine.iterators.aggregates.groupby import GroupByAggregator
from sage.query_engine.iterators.aggregates.min_max import MinAggregator, MaxAggregator
from sage.query_engine.iterators.aggregates.sum import SumAggregator
from sage.query_engine.iterators.aggregates.projection import AggregatesProjectionIterator
from sage.query_engine.iterators.filter import FilterIterator
from sage.query_engine.iterators.projection import ProjectionIterator
from sage.query_engine.iterators.union import BagUnionIterator
from sage.query_engine.optimizer.plan_builder import build_left_plan


class UnsupportedSPARQL(Exception):
    """Thrown when a SPARQL feature is not supported by the Sage query engine"""
    pass


def localize_triple(triples, graphs):
    """Using a set of RDF graphs, performs data localization of a set of RDF triples"""
    for t in triples:
        s, p, o = format_term(t[0]), format_term(t[1]), format_term(t[2])
        for graph in graphs:
            yield {
                'subject': s,
                'predicate': p,
                'object': o,
                'graph': graph
            }


def format_term(term):
    """Convert a rdflib RDF Term into the format used by Sage"""
    if type(term) is URIRef:
        return str(term)
    elif type(term) is BNode:
        return '?v_' + str(term)
    else:
        return term.n3()


def fetch_graph_triples(node, current_graphs, server_url):
    """Fetch triples in a BGP or a BGP nested in a GRAPH clause"""
    if node.name == 'Graph' and node.p.name == 'BGP':
        graph_uri = format_graph_uri(format_term(node.term), server_url)
        return list(localize_triple(node.p.triples, [graph_uri]))
    elif node.name == 'BGP':
        return list(localize_triple(node.triples, current_graphs))
    else:
        raise UnsupportedSPARQL(
            'Unsupported SPARQL Feature: a Sage engine can only perform joins between Graphs and BGPs')


def build_aggregator(dataset, aggregate, renaming_map):
    """Build an aggregator from its logical representation and a renaming Map"""
    binds_to = renaming_map[aggregate.res.n3()]
    if aggregate.name == 'Aggregate_Count':
        if aggregate.distinct == 'DISTINCT' and dataset.is_approximation_enabled:
            error_rate = dataset.error_rate
            return ApproximateCountDistinctAggregator(aggregate.vars.n3(), binds_to=binds_to, error_rate=error_rate)
        elif aggregate.distinct == 'DISTINCT' and not dataset.is_approximation_enabled:
            return CountDistinctAggregator(aggregate.vars.n3(), binds_to=binds_to)
        else:
            return CountAggregator(aggregate.vars.n3(), binds_to=binds_to)
    elif aggregate.name == 'Aggregate_Sum':
        return SumAggregator(aggregate.vars.n3(), binds_to=binds_to)
    elif aggregate.name == 'Aggregate_Min':
        return MinAggregator(aggregate.vars.n3(), binds_to=binds_to)
    elif aggregate.name == 'Aggregate_Max':
        return MaxAggregator(aggregate.vars.n3(), binds_to=binds_to)
    else:
        raise UnsupportedSPARQL("Unsupported SPARQL Aggregate: {}".format(aggregate.name))


def parse_query(query, dataset, default_graph, server_url):
    """Parse a regular SPARQL query into a query execution plan"""
    logical_plan = translateQuery(parseQuery(query)).algebra
    cardinalities = list()
    iterator = parse_query_node(logical_plan, dataset, [default_graph], server_url, cardinalities)
    return iterator, cardinalities


def parse_query_node(node, dataset, current_graphs, server_url, cardinalities, renaming_map=None):
    """
        Recursively parse node in the query logical plan to build a preemptable physical query execution plan.

        Args:
            * node - Node of the logical plan to parse (in rdflib format)
            * dataset - RDF dataset used to execute the query
            * current_graphs - List of IRI of the current RDF graph queried
            * server_url - URL of the SaGe server
            * cardinalities - Map<triple,integer> used to track triple patterns cardinalities
    """
    if node.name == 'SelectQuery':
        # in case of a FROM clause, set the new default graphs used
        graphs = current_graphs
        if node.datasetClause is not None:
            graphs = [format_graph_uri(format_term(graph_iri.default), server_url) for graph_iri in node.datasetClause]
        return parse_query_node(node.p, dataset, graphs, server_url, cardinalities)
    elif node.name == 'Project':
        query_vars = list(map(lambda t: t.n3(), node.PV))
        if node.p.name == 'AggregateJoin' or node.p.name == 'Extend':
            # forward projection variables, as we need them for parsing an AggregateJoin
            node.p['PV'] = query_vars
            return parse_query_node(node.p, dataset, current_graphs, server_url, cardinalities)
        child = parse_query_node(node.p, dataset, current_graphs, server_url, cardinalities)
        return ProjectionIterator(child, query_vars)
    elif node.name == 'BGP':
        # bgp_vars = node._vars
        triples = list(localize_triple(node.triples, current_graphs))
        iterator, query_vars, c = build_left_plan(triples, dataset, current_graphs)
        # track cardinalities of every triple pattern
        cardinalities += c
        return iterator
    elif node.name == 'Union':
        left = parse_query_node(node.p1, dataset, current_graphs, server_url, cardinalities)
        right = parse_query_node(node.p2, dataset, current_graphs, server_url, cardinalities)
        return BagUnionIterator(left, right)
    elif node.name == 'Filter':
        expression = parse_filter_expr(node.expr)
        iterator = parse_query_node(node.p, dataset, current_graphs, server_url, cardinalities)
        return FilterIterator(iterator, expression)
    elif node.name == 'Join':
        # only allow for joining BGPs from different GRAPH clauses
        triples = fetch_graph_triples(node.p1, current_graphs, server_url)
        triples += fetch_graph_triples(node.p2, current_graphs, server_url)
        iterator, query_vars, c = build_left_plan(triples, dataset, current_graphs)
        # track cardinalities of every triple pattern
        cardinalities += c
        return iterator
    elif node.name == 'Extend':
        # remove all extend operators, as they are not needed
        current = node
        renaming = dict()
        while current.name == 'Extend':
            renaming[current.expr.n3()] = current.var.n3()
            current = current.p
        current['PV'] = node['PV']
        return parse_query_node(current, dataset, current_graphs, server_url, cardinalities, renaming_map=renaming)
    elif node.name == 'AggregateJoin':
        groupby_variables = list()
        # build GROUP BY variables
        last_groupby_var = None
        if node.p.expr is None: # case 1: no explicit group BY, so we group by all variables in the query
            last_groupby_var = list(node.p._vars)[0]
            # for variable in node.p._vars:
            #     groupby_variables.append(variable.n3())
            #     last_groupby_var = variable
        else:  # case 2: there is an explicit group by
            for variable in node.p.expr:
                groupby_variables.append(variable.n3())
                last_groupby_var = variable
        # build aggregators for evaluating SPARQL aggregations (if any)
        aggregators = list()
        for agg in node.A:
            if agg.vars == '*':
                agg.vars = last_groupby_var
            if agg.name != 'Aggregate_Sample':
                aggregators.append(build_aggregator(dataset, agg, renaming_map))
        # build source iterator from child node
        source = parse_query_node(node.p.p, dataset, current_graphs, server_url, cardinalities)
        # add the GROUP BY operator (with aggregators) to the pipeline
        source = GroupByAggregator(source, groupby_variables, aggregators=aggregators, max_size=dataset.max_group_keys)
        # add the projection to the pipeline, depending of the context
        return AggregatesProjectionIterator(source, node.PV)
    else:
        raise UnsupportedSPARQL("Unsupported SPARQL feature: {}".format(node.name))


def parse_filter_expr(expr):
    """Stringify a rdflib Filter expression"""
    if not hasattr(expr, 'name'):
        return format_term(expr)
    else:
        if expr.name == 'RelationalExpression':
            return "({} {} {})".format(parse_filter_expr(expr.expr), expr.op, parse_filter_expr(expr.other))
        elif expr.name == 'AdditiveExpression':
            expression = parse_filter_expr(expr.expr)
            for i in range(len(expr.op)):
                expression = "({} {} {})".format(expression, expr.op[i], parse_filter_expr(expr.other[i]))
            return expression
        elif expr.name == 'ConditionalAndExpression':
            expression = parse_filter_expr(expr.expr)
            for other in expr.other:
                expression = "({} && {})".format(expression, parse_filter_expr(other))
            return expression
        elif expr.name == 'ConditionalOrExpression':
            expression = parse_filter_expr(expr.expr)
            for other in expr.other:
                expression = "({} || {})".format(expression, parse_filter_expr(other))
            return expression
        elif expr.name.startswith('Builtin_') and 'arg' in expr:
            fn_name = expr.name[8:]
            return "{}({})".format(fn_name, parse_filter_expr(expr.arg))
        raise UnsupportedSPARQL("Unsupported SPARQL FILTER expression: {}".format(expr.name))
