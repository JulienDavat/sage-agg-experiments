
import http_client as http
from aggregators import GenericReducer
from spy import Spy
import logging, re

from rdflib.term import Literal
from rdflib.plugins.sparql.algebra import translateQuery
from rdflib.plugins.sparql.parser import parseQuery

logger = logging.getLogger()

def prepare_query(query):
    """
    As the server does not support "sample aggregations", this
    function transforms a query Q into a query Q' that can be
    evaluated on the server.

    For example:
    
    Q = select (count(?s) as ?count) ?p where { ?s ?p ?o } group by ?p

    The query Q uses a sample aggregation as ?p appears both in the group
    by clause and in the select clause...
    To be executable, the query Q is transforms into a query Q' and the
    client will be in charge of the projection of ?p

    Q' = select (count(?s) as ?count) where { ?s ?p ?o } group by ?p
    """
    query_plan = translateQuery(parseQuery(query)).algebra

    projection = [f'?{Literal(x).value}' for x in query_plan.PV]

    [select_clause, where_clause] = query.split('where')
    count_aggregates = re.findall(r'\(count\((?:distinct)?.*?\).*?\)', select_clause)
    
    return (f"select {' '.join(count_aggregates)} where {where_clause}", projection)

def execute(query, dataset):
    prepared_query, projection = prepare_query(query)
    logger.info(prepared_query)
    accumulator = GenericReducer(projection)
    spy = Spy()
    http.execute(prepared_query, dataset, accumulator, spy)
    return accumulator, spy