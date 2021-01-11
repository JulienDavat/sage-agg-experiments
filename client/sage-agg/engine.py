
import http_client as http
from aggregators import GenericReducer
from spy import Spy

def execute(server, dataset, query):
    accumulator = GenericReducer()
    spy = Spy()
    http.execute(server, dataset, query, accumulator, spy)
    return accumulator, spy