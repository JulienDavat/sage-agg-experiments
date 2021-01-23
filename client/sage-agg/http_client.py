import json
import sys
import logging
import coloredlogs
import requests
from time import time, sleep

coloredlogs.install(level='INFO', fmt='%(asctime)s - %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

def send_request(server, payload, retry=100):
    try:
        headers = {
            "accept": "application/json",
            "content-type": "application/json",
            'Cache-Control': 'no-cache',
            "next": None
        }
        response = requests.post(server, headers=headers, data=json.dumps(payload))
        return response.json()
    except:
        if retry > 0:
            sleep(0.5)
            return send_request(server, payload, retry=retry - 1)
        else:
            raise Exception('A network error occured...')

def execute(server, dataset, query, accumulator, spy):
    has_next = True
    payload = {
        "query": query,
        "defaultGraph": dataset,
        "next": None
    }
    start = time()
    while has_next:
        response = send_request(server, payload)
        has_next = response['hasNext']
        payload["next"] = response["next"]
        
        spy.report_nb_calls(1)
        spy.report_data_transfer(sys.getsizeof(json.dumps(response)))
        
        for bindings in response['bindings']:
            logger.debug(bindings)
            accumulator.accumulate(bindings)
    execution_time = time() - start

    spy.report_execution_time(execution_time)
    spy.report_nb_result(accumulator.size())