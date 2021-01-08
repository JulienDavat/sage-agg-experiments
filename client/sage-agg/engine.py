
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

    [select_clause, where_clause] = query.split('WHERE')
    count_aggregates = re.findall(r'\(COUNT\((?:DISTINCT)?.*?\).*?\)', select_clause)
    
    return (f"SELECT {' '.join(count_aggregates)} WHERE {where_clause}", projection)

def execute(server, dataset, query):
    prepared_query, projection = prepare_query(query)
    logger.info(prepared_query)
    accumulator = GenericReducer(projection)
    spy = Spy()
    http.execute(server, dataset, prepared_query, accumulator, spy)
    return accumulator, spy