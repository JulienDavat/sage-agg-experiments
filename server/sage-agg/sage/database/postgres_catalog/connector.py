# postgre_connector.py
# Author: Thomas MINIER - MIT License 2017-2019
import json
import uuid
from functools import reduce
from math import ceil
from time import time

from sage.database.db_connector import DatabaseConnector
from sage.database.db_iterator import DBIterator, EmptyIterator
from sage.database.postgres_catalog.queries import get_start_query, get_resume_query, get_insert_query, get_delete_query
from sage.database.postgres_catalog.transaction_manager import TransactionManager
from sage.database.postgres_catalog.utils import id_to_predicate
from sage.query_engine.optimizer.utils import is_variable


def fetch_histograms(cursor, table_name, attribute_name):
    """
        Download PostgreSQL histograms from a given table and attribute
    """
    base_query = "SELECT null_frac, n_distinct, most_common_vals, most_common_freqs FROM pg_stats WHERE tablename = '{}' AND attname = '{}'".format(
        table_name, attribute_name)
    cursor.execute(base_query)
    record = cursor.fetchone()
    null_frac, n_distinct, most_common_vals, most_common_freqs = record
    # build selectivity table
    selectivities = {}
    cpt = 0
    for common_val in most_common_vals[1:-1].split(","):
        if cpt < len(most_common_freqs):
            selectivities[common_val] = most_common_freqs[cpt]
        cpt += 1
    return (null_frac, n_distinct, selectivities, sum(most_common_freqs))


class PostgresIterator(DBIterator):
    """An PostgresIterator implements a DBIterator for a triple pattern evaluated using a Postgre database file"""

    def __init__(self, cursor, connection, start_query, start_params, table_name, pattern, fetch_size=2000):
        super(PostgresIterator, self).__init__(pattern)
        self._cursor = cursor
        self._connection = connection
        self._current_query = start_query
        self._table_name = table_name
        self._fetch_size = fetch_size
        # resume query execution with a SQL query
        self._cursor.execute(self._current_query, start_params)
        # always keep the current set of rows buffered inside the iterator
        self._last_reads = self._cursor.fetchmany(size=self._fetch_size)
        # stats
        self._read = 0

    def __del__(self):
        """Destructor (close the database cursor)"""
        self._cursor.close()

    def last_read(self):
        """Return the index ID of the last element read"""
        if not self.has_next():
            return ''
        triple = self._last_reads[0]
        print('[PostgresIterator] Red {} triples during this quantum'.format(self._read))
        return json.dumps({
            's': triple[0],
            'p': triple[1],
            'o': triple[2]
        }, separators=(',', ':'))

    def next(self):
        """Return the next solution mapping or raise `StopIteration` if there are no more solutions"""
        if not self.has_next():
            return None
        self._read += 1
        triple = self._last_reads.pop(0)
        return (f'id_{triple[0]}', f'id_{triple[1]}', f'id_{triple[2]}')

    def has_next(self):
        """Return True if there is still results to read, and False otherwise"""
        if len(self._last_reads) == 0:
            self._last_reads = self._cursor.fetchmany(size=self._fetch_size)
        return len(self._last_reads) > 0


class PostgresConnector(DatabaseConnector):
    """
        A PostgresConnector search for RDF triples in a PostgreSQL database.
        Constructor arguments:
            - table_name `str`: Name of the SQL table containing RDF data.
            - dbname `str`: the database name
            - user `str`: user name used to authenticate
            - password `str`: password used to authenticate
            - host `str`: database host address (defaults to UNIX socket if not provided)
            - port `int`: connection port number (defaults to 5432 if not provided)
            - fetch_size `int`: how many RDF triples are fetched per SQL query (default to 2000)
    """

    def __init__(self, table_name, dbname, user, password, host='', port=5432, fetch_size=2000):
        super(PostgresConnector, self).__init__()
        self._table_name = table_name
        self._manager = TransactionManager(dbname, user, password, host=host, port=port)
        self._fetch_size = fetch_size
        self._warmup = True

        # Data used for cardinality estimation.
        # They are initialized using PostgreSQL histograms, after the 1st connection to the DB.
        self._avg_row_count = 0
        self._subject_histograms = {
            'selectivities': dict(),
            'null_frac': 0,
            'n_distinct': 0,
            'sum_freqs': 0
        }
        self._predicate_histograms = {
            'selectivities': dict(),
            'null_frac': 0,
            'n_distinct': 0,
            'sum_freqs': 0
        }
        self._object_histograms = {
            'selectivities': dict(),
            'null_frac': 0,
            'n_distinct': 0,
            'sum_freqs': 0
        }

    def open(self):
        """Open the database connection"""
        if self._manager.is_open():
            self._manager.open_connection()

        # Do warmup phase if required, i.e., fetch stats for query execution
        if self._warmup:
            cursor = self._manager.start_transaction()
            # fetch estimated table cardinality
            cursor.execute(
                "SELECT reltuples AS approximate_row_count FROM pg_class WHERE relname = '{}'".format(self._table_name))
            self._avg_row_count = cursor.fetchone()[0]
            # fetch subject histograms
            (null_frac, n_distinct, selectivities, sum_freqs) = fetch_histograms(cursor, self._table_name, 'subject')
            self._subject_histograms = {
                'selectivities': selectivities,
                'null_frac': null_frac,
                'n_distinct': n_distinct,
                'sum_freqs': sum_freqs
            }
            # fetch predicate histograms
            (null_frac, n_distinct, selectivities, sum_freqs) = fetch_histograms(cursor, self._table_name, 'predicate')
            self._predicate_histograms = {
                'selectivities': selectivities,
                'null_frac': null_frac,
                'n_distinct': n_distinct,
                'sum_freqs': sum_freqs
            }
            # fetch object histograms
            (null_frac, n_distinct, selectivities, sum_freqs) = fetch_histograms(cursor, self._table_name, 'object')
            self._object_histograms = {
                'selectivities': selectivities,
                'null_frac': null_frac,
                'n_distinct': n_distinct,
                'sum_freqs': sum_freqs
            }
            # commit & close cursor
            self._manager.commit()
            self._warmup = False

    def close(self):
        """Close the database connection"""
        # commit, then close the cursor and the connection
        self._manager.close_connection()

    def start_transaction(self):
        """Start a PostgreSQL transaction"""
        # print("Process {} called start_transaction".format(os.getpid()))
        self._manager.start_transaction()

    def commit_transaction(self):
        """Commit any ongoing transaction"""
        self._manager.commit()

    def abort_transaction(self):
        """Abort any ongoing transaction"""
        self._manager.abort()

    def _estimate_cardinality(self, subject, predicate, obj):
        """
            Estimate the cardinality of a triple pattern using PostgreSQL histograms.
            Args:
                - subject ``string`` - Subject of the triple pattern
                - predicate ``string`` - Predicate of the triple pattern
                - obj ``string`` - Object of the triple pattern
            Returns:
                The estimated cardinality of the triple pattern
        """
        s = int(subject.split('_')[1]) if (subject is not None) and (not is_variable(subject)) else None
        p = int(predicate.split('_')[1]) if (predicate is not None) and (not is_variable(predicate)) else None
        o = int(obj.split('_')[1]) if (obj is not None) and (not is_variable(obj)) else None

        # estimate the selectivity of the triple pattern using PostgreSQL histograms
        selectivity = 1
        # avoid division per zero when some histograms are not fully up-to-date
        try:
            # compute the selectivity of a bounded subject
            if s is not None:
                if s in self._subject_histograms['selectivities']:
                    selectivity *= self._subject_histograms['selectivities'][s]
                else:
                    selectivity *= (1 - self._subject_histograms['sum_freqs']) / (
                            self._subject_histograms['n_distinct'] - len(self._subject_histograms['selectivities']))
            # compute the selectivity of a bounded predicate
            if p is not None:
                if p in self._predicate_histograms['selectivities']:
                    selectivity *= self._predicate_histograms['selectivities'][p]
                else:
                    selectivity *= (1 - self._predicate_histograms['sum_freqs']) / (
                            self._predicate_histograms['n_distinct'] - len(
                        self._predicate_histograms['selectivities']))
            # compute the selectivity of a bounded object
            if o is not None:
                if o in self._object_histograms['selectivities']:
                    selectivity *= self._object_histograms['selectivities'][o]
                else:
                    selectivity *= (1 - self._object_histograms['sum_freqs']) / (
                            self._object_histograms['n_distinct'] - len(self._object_histograms['selectivities']))
        except ZeroDivisionError:
            pass
        # estimate the cardinality from the estimated selectivity
        cardinality = int(ceil(selectivity * self._avg_row_count))
        return cardinality if cardinality > 0 else 1

    def get_value(self, term):
        if not isinstance(term, str) or not term.startswith('id_'):
            return term
        self.open()
        term_id = int(term.split('_')[1])
        cursor = self._manager.get_connection().cursor(str(uuid.uuid4()))
        cursor.execute(f'SELECT value FROM {self._table_name}_catalog WHERE id = {term_id}')
        result = cursor.fetchone()
        if result is None:
            raise Exception(f'Unknown id ({term_id}) for table {self._table_name}')
        return result[0]

    def get_identifiant(self, term):
        if not isinstance(term, str):
            return term
        self.open()
        cursor = self._manager.get_connection().cursor(str(uuid.uuid4()))
        cursor.execute(f'SELECT id FROM {self._table_name}_catalog WHERE value = \'{term}\'')
        result = cursor.fetchone()
        return 'id_-1' if result is None else f'id_{result[0]}'

    def search(self, subject, predicate, obj, last_read=None, as_of=None):
        """
            Get an iterator over all RDF triples matching a triple pattern.
            Args:
                - subject ``string`` - Subject of the triple pattern
                - predicate ``string`` - Predicate of the triple pattern
                - obj ``string`` - Object of the triple pattern
                - last_read ``string=None`` ``optional`` -  OFFSET ID used to resume scan
                - as_of ``datetime=None`` ``optional`` - Perform all reads against a consistent snapshot represented by a timestamp.
            Returns:
                A tuple (`iterator`, `cardinality`), where `iterator` is a Python iterator over RDF triples matching the given triples pattern, and `cardinality` is the estimated cardinality of the triple pattern
        """
        # do warmup if necessary
        self.open()

        # format triple patterns for the PostgreSQL API
        s = int(subject.split('_')[1]) if (subject is not None) and (not is_variable(subject)) else None
        p = int(predicate.split('_')[1]) if (predicate is not None) and (not is_variable(predicate)) else None
        o = int(obj.split('_')[1]) if (obj is not None) and (not is_variable(obj)) else None
        pattern = {'subject': s, 'predicate': p, 'object': o}

        # dedicated cursor used to scan this triple pattern
        # WARNING: we need to use a dedicated cursor per triple pattern iterator.
        # Otherwise, we might reset a cursor whose results were not fully consumed.
        cursor = self._manager.get_connection().cursor(str(uuid.uuid4()))

        # create a SQL query to start a new index scan
        if last_read is None:
            start_query, start_params = get_start_query(s, p, o, self._table_name)
        else:
            # empty last_read key => the scan has already been completed
            if len(last_read) == 0:
                return EmptyIterator(pattern), 0
            # otherwise, create a SQL query to resume the index scan
            last_read = json.loads(last_read)
            t = (last_read["s"], last_read["p"], last_read["o"])
            start_query, start_params = get_resume_query(s, p, o, t, self._table_name)

        # create the iterator to yield the matching RDF triples
        iterator = PostgresIterator(cursor, self._manager.get_connection(), start_query, start_params, self._table_name,
                                    pattern, fetch_size=self._fetch_size)
        card = self._estimate_cardinality(subject, predicate, obj) if iterator.has_next() else 0
        return iterator, card

    def from_config(config):
        """Build a PostgresConnector from a configuration object"""
        if 'dbname' not in config or 'user' not in config or 'password' not in config:
            raise SyntaxError(
                'A valid configuration for a PostgreSQL connector must contains the dbname, user and password fields')

        table_name = config['name']
        host = config['host'] if 'host' in config else ''
        port = config['port'] if 'port' in config else 5432
        fetch_size = config['fetch_size'] if 'fetch_size' in config else 5000

        return PostgresConnector(table_name, config['dbname'], config['user'], config['password'], host=host, port=port,
                                 fetch_size=fetch_size)

    def insert(self, subject, predicate, obj):
        """
            Insert a RDF triple into the RDF Graph.
        """
        # do warmup if necessary, then start a new transaction
        self.open()
        transaction = self._manager.start_transaction()
        if subject is not None and predicate is not None and obj is not None:
            insert_query = get_insert_query(self._table_name)
            transaction.execute(insert_query, (subject, predicate, obj))
            self._manager.commit()

    def delete(self, subject, predicate, obj):
        """
            Delete a RDF triple from the RDF Graph.
        """
        # do warmup if necessary, then start a new transaction
        self.open()
        transaction = self._manager.start_transaction()
        if subject is not None and predicate is not None and obj is not None:
            delete_query = get_delete_query(self._table_name)
            transaction.execute(delete_query, (subject, predicate, obj))
            self._manager.commit()
