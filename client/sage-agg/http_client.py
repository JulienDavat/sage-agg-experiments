import json
import sys
import logging
import coloredlogs
import requests
from time import time

coloredlogs.install(level='INFO', fmt='%(asctime)s - %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

def execute(server, dataset, query, accumulator, spy):
    has_next = True
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        'Cache-Control': 'no-cache',
        "next": None
    }
    payload = {
        "query": query,
        "defaultGraph": dataset,
        "next": None,
        "optimized": True,
        "buffer": -1
    }

    start = time()
    while has_next:
        response = requests.post(server, headers=headers, data=json.dumps(payload))
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