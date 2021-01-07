from rdflib import Namespace, Literal, URIRef
from rdflib.graph import Graph, ConjunctiveGraph
from rdflib.plugins.memory import IOMemory

from os import listdir
from os.path import isfile, join, basename
import json
import sys
import glob
import logging
import coloredlogs
import requests
from json import dumps
from time import time

coloredlogs.install(level='INFO', fmt='%(asctime)s - %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

def execute(query, dataset, accumulator, spy):
    default_graph_uri = f"http://localhost:8080/sparql/{dataset}"
    has_next = True
    nb_results = 0
    nb_calls = 0
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        'Cache-Control': 'no-cache',
        "next": None
    }
    payload = {
        "query": query,
        "defaultGraph": default_graph_uri,
        "next": None,
        "optimized": True,
        "buffer": -1
    }

    start = time()
    while has_next:
        response = requests.post("http://localhost:8080/sparql", headers=headers, data=dumps(payload))
        json_response = response.json()
        has_next = json_response['hasNext']
        payload["next"] = json_response["next"]
        
        spy.report_nb_calls(1)
        spy.report_data_transfer(sys.getsizeof(json.dumps(json_response)))
        
        for bindings in json_response['bindings']:
            # print(bindings)
            accumulator.accumulate(bindings)
    execution_time = time() - start

    spy.report_execution_time(execution_time)
    spy.report_nb_result(accumulator.size())