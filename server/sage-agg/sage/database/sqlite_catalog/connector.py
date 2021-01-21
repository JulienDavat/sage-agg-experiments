# postgre_connector.py
# Author: Thomas MINIER - MIT License 2017-2019
import json
import uuid
from functools import reduce
from math import ceil
from time import time

from sage.database.db_connector import DatabaseConnector
from sage.database.db_iterator import DBIterator, EmptyIterator
from sage.database.sqlite.queries import get_start_query, get_resume_query, get_insert_query, get_delete_query
from sage.database.sqlite.transaction_manager import TransactionManager
from sage.database.sqlite.utils import id_to_predicate
from sage.query_engine.optimizer.utils import is_variable
from sage.database.utils import get_kind


class SQliteIterator(DBIterator):
    """An PostgresIterator implements a DBIterator for a triple pattern evaluated using a Postgre database file"""

    def __init__(self, cursor, connection, start_query, start_params, table_name, pattern, fetch_size=2000):
        super(SQliteIterator, self).__init__(pattern)
        self._cursor = cursor
        self._connection = connection
        self._current_query = start_query
        self._table_name = table_name
        self._fetch_size = fetch_size
        self._start = time()
        self._cursor.execute(self._current_query, start_params)
        # always keep the current set of rows buffered inside the iterator
        self._last_reads = self._cursor.fetchmany(size=100)
        # stats
        self._read = 0
        self._readtab = []
        self._cur = time()

    def __del__(self):
        """Destructor (close the database cursor)"""
        self._cursor.close()

    def last_read(self):
        """Return the index ID of the last element read"""
        if not self.has_next():
            return ''
        triple = self._last_reads[0]
        print('[SQliteIterator] Red {} triples during this quantum'.format(self._read))
        if len(self._readtab) > 0:
            res = reduce(lambda a, b: a + b, self._readtab) / len(self._readtab)
        else:
            res = 0
        print('[SQliteIterator] Average overhead per triple is: {}'.format(res))
        return json.dumps({
            's': triple[0],
            'p': triple[1],
            'o': triple[2]
        }, separators=(',', ':'))

    def next(self):
        """Return the next solution mapping or raise `StopIteration` if there are no more solutions"""
        if not self.has_next():
            return None
        triple = self._last_reads.pop(0)
        self._read += 1

        cur = time()
        self._readtab.append(cur - self._cur)
        self._cur = cur

        return (f'id_{triple[0]}', f'id_{triple[1]}', f'id_{triple[2]}')

    def has_next(self):
        """Return True if there is still results to read, and False otherwise"""
        if len(self._last_reads) == 0:
            st = time()
            self._last_reads = self._cursor.fetchmany(size=self._fetch_size)
            # print('fetching many {} in {}seconds'.format(self._fetch_size, time() - st))
        return len(self._last_reads) > 0


class SQliteConnector(DatabaseConnector):
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

    def __init__(self, table_name, database, fetch_size=2000):
        super(SQliteConnector, self).__init__()
        self._table_name = table_name
        self._manager = TransactionManager(database)
        self._fetch_size = fetch_size
        self._warmup = True

        # Data used for cardinality estimation.
        # They are initialized using SQlite statistics, after the 1st connection to the DB.
        self._spo_index_stats = {
            'row_count': 0,
            'same_s_row_count': 0,
            'same_sp_row_count': 0,
            'same_spo_row_count': 0
        }
        self._pos_index_stats = {
            'row_count': 0,
            'same_p_row_count': 0,
            'same_po_row_count': 0,
            'same_pos_row_count': 0
        }
        self._osp_index_stats = {
            'row_count': 0,
            'same_o_row_count': 0,
            'same_os_row_count': 0,
            'same_osp_row_count': 0
        }

    def open(self):
        """Open the database connection"""
        if self._manager.is_open():
            self._manager.open_connection()

        # Do warmup phase if required, i.e., fetch stats for query execution
        if self._warmup:
            cursor = self._manager.start_transaction()
            # fetch SPO index statistics
            cursor.execute(f'SELECT stat FROM sqlite_stat1 WHERE idx = \'{self._table_name}_spo_index\'')
            (row_count, same_s_row_count, same_sp_row_count, same_spo_row_count) = cursor.fetchone()[0].split(' ')
            self._spo_index_stats = {
                'row_count': int(row_count),
                'same_s_row_count': int(same_s_row_count),
                'same_sp_row_count': int(same_sp_row_count),
                'same_spo_row_count': int(same_spo_row_count)
            }
            # fetch POS index statistics
            cursor.execute(f'SELECT stat FROM sqlite_stat1 WHERE idx = \'{self._table_name}_pos_index\'')
            (row_count, same_p_row_count, same_po_row_count, same_pos_row_count) = cursor.fetchone()[0].split(' ')
            self._pos_index_stats = {
                'row_count': int(row_count),
                'same_p_row_count': int(same_p_row_count),
                'same_po_row_count': int(same_po_row_count),
                'same_pos_row_count': int(same_pos_row_count)
            }
            # fetch OSP index statistics
            cursor.execute(f'SELECT stat FROM sqlite_stat1 WHERE idx = \'{self._table_name}_osp_index\'')
            (row_count, same_o_row_count, same_os_row_count, same_osp_row_count) = cursor.fetchone()[0].split(' ')
            self._osp_index_stats = {
                'row_count': int(row_count),
                'same_o_row_count': int(same_o_row_count),
                'same_os_row_count': int(same_os_row_count),
                'same_osp_row_count': int(same_osp_row_count)
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
        # format triple patterns for the SQlite API
        s = int(subject.split('_')[1]) if (subject is not None) and (not is_variable(subject)) else None
        p = int(predicate.split('_')[1]) if (predicate is not None) and (not is_variable(predicate)) else None
        o = int(obj.split('_')[1]) if (obj is not None) and (not is_variable(obj)) else None
        # estimate triple cardinality using sqlite statistics (more or less a variable counting join ordering)
        kind = get_kind(s, p, o)
        if kind == 'spo':
            return self._spo_index_stats['same_spo_row_count']
        elif kind == '???':
            return self._spo_index_stats['row_count']
        elif kind == 's??':
            return self._spo_index_stats['same_s_row_count']
        elif kind == 'sp?':
            return self._spo_index_stats['same_sp_row_count']
        elif kind == '?p?':
            return self._pos_index_stats['same_p_row_count']
        elif kind == '?po':
            return self._pos_index_stats['same_po_row_count']
        elif kind == 's?o':
            return self._osp_index_stats['same_os_row_count']
        elif kind == '??o':
            return self._osp_index_stats['same_o_row_count']
        else:
            raise Exception("Unkown pattern type: {}".format(kind))

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

        # format triple patterns for the SQlite API
        s = int(subject.split('_')[1]) if (subject is not None) and (not is_variable(subject)) else None
        p = int(predicate.split('_')[1]) if (predicate is not None) and (not is_variable(predicate)) else None
        o = int(obj.split('_')[1]) if (obj is not None) and (not is_variable(obj)) else None
        pattern = {'subject': s, 'predicate': p, 'object': o}

        # dedicated cursor used to scan this triple pattern
        # WARNING: we need to use a dedicated cursor per triple pattern iterator.
        # Otherwise, we might reset a cursor whose results were not fully consumed.
        cursor = self._manager.get_connection().cursor()

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
        iterator = SQliteIterator(
            cursor, self._manager.get_connection(), 
            start_query, start_params, 
            self._table_name, 
            pattern, 
            fetch_size=self._fetch_size)
        card = self._estimate_cardinality(subject, predicate, obj) if iterator.has_next() else 0
        return iterator, card

    def get_value(self, term):
        if not isinstance(term, str) or not term.startswith('id_'):
            return term
        self.open()
        term_id = int(term.split('_')[1])
        cursor = self._manager.get_connection().cursor()
        cursor.execute(f'SELECT value FROM {self._table_name}_catalog WHERE id = {term_id}')
        result = cursor.fetchone()
        if result is None:
            raise Exception(f'Unknown id ({term_id}) for table {self._table_name}')
        return result[0]

    def get_identifiant(self, term):
        if not isinstance(term, str):
            return term
        self.open()
        cursor = self._manager.get_connection().cursor()
        cursor.execute(f'SELECT id FROM {self._table_name}_catalog WHERE value = \'{term}\'')
        result = cursor.fetchone()
        return 'id_-1' if result is None else f'id_{result[0]}'

    def from_config(config):
        """Build a SQliteConnector from a configuration object"""
        if 'database' not in config:
            raise SyntaxError(
                'A valid configuration for a SQlite connector must contains the database file')

        table_name = config['name']
        database = config['database']
        fetch_size = config['fetch_size'] if 'fetch_size' in config else 5000

        return SQliteConnector(table_name, database, fetch_size=fetch_size)

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
