
import http_client as http
from aggregators import GenericReducer
from spy import Spy
import logging

logger = logging.getLogger()

def execute(query, dataset):
    accumulator = GenericReducer()
    spy = Spy()
    http.execute(query, dataset, accumulator, spy)
    print(accumulator.result())
    print(f'time: {spy.execution_time()} sec \ntransfer: {spy.data_transfer()} bytes \ncalls: {spy.nb_calls()} \nresult: {spy.nb_result()}')
    return spy