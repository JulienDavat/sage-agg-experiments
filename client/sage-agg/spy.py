class Spy():

    def __init__(self):
        self._nb_calls = 0
        self._data_transfer = 0
        self._execution_time = 0
        self._nb_result = 0

    def nb_calls(self):
        return self._nb_calls

    def data_transfer(self):
        return self._data_transfer

    def execution_time(self):
        return self._execution_time

    def nb_result(self):
        return self._nb_result

    def report_nb_calls(self, nb_calls):
        self._nb_calls += nb_calls

    def report_data_transfer(self, nb_bytes):
        self._data_transfer += nb_bytes

    def report_execution_time(self, time):
        self._execution_time = time

    def report_nb_result(self, nb_result):
        self._nb_result = nb_result