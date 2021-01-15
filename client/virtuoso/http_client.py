import json, logging, sys, coloredlogs

from spy import Spy
from time import time
from SPARQLWrapper import SPARQLWrapper, JSON, XML

coloredlogs.install(level='INFO', fmt='%(asctime)s - %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

def select_format(format):
    if format == "w3c/json":
        return JSON
    elif format == "w3c/xml":
        return XML
    else:
        return JSON

def stringify_result(result, format):
    if format == "w3c/json":
        return json.dumps(result)
    elif format == "w3c/xml":
        return result.toxml()
    else:
        return json.dumps(result)

def execute(server, dataset, query, format):
    sparql = SPARQLWrapper(server)
    sparql.setQuery(query)
    sparql.setReturnFormat(select_format(format))
    sparql.addParameter("default-graph-uri", dataset)

    stats = Spy()

    try:
        start = time()
        results = sparql.query()
        formatedResults = stringify_result(results.convert(), format)
        execution_time = time() - start
        
        stats.report_execution_time(execution_time)
        stats.report_data_transfer(sys.getsizeof(formatedResults))
        stats.report_nb_calls(1)

        return formatedResults, stats
    except Exception as error:
        logger.error(f"An error occured during query execution:\n{error}")
        sys.exit(1)