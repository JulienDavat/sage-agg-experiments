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

def execute(server, dataset, query, format):
    sparql = SPARQLWrapper(server)
    sparql.setQuery(query)
    sparql.setReturnFormat(select_format(format))
    sparql.addParameter("default-graph-uri", dataset)

    stats = Spy()

    try:
        start = time()
        results = sparql.query()
        formatedResults = results.convert()
        execution_time = time() - start
        
        stats.report_execution_time(execution_time)
        stats.report_data_transfer(sys.getsizeof(json.dumps(formatedResults, ensure_ascii=False)))
        stats.report_nb_calls(1)

        return formatedResults, stats
    except Exception as error:
        logger.error(f"An error occured during query execution:\n{error}")
        sys.exit(1)