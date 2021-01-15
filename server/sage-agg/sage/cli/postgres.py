# coding: utf8
# postgres.py
# Author: Thomas MINIER - MIT License 2017-2019
import logging
from time import time

import click
import coloredlogs
import psycopg2
import sage.cli.postgres_utils as p_utils
import sys
from rdflib.util import from_n3
from rdflib import BNode, Literal, URIRef, Variable,Graph
import codecs
from psycopg2.extras import execute_values
from sage.cli.utils import load_dataset, get_rdf_reader
from rdflib.plugins.parsers.ntriples import NTriplesParser, Sink, ParseError


def bucketify(iterable, bucket_size):
    """Group items from an iterable by buckets"""
    bucket = list()
    for s, p, o, triple in iterable:
        bucket.append((s, p, o))
        if len(bucket) >= bucket_size:
            yield bucket
            bucket = list()
    if len(bucket) > 0:
        yield bucket


def bucketify_bytes(iterable, bucket_size, encoding='utf-8', throw=True):
    """Group items from an iterable by buckets"""
    print("Throwable: ", throw)
    bucket = list()
    for s, p, o in iterable:
        s_encoded = s
        p_encoded = p
        o_encoded = o
        try:
            s_encoded = s_encoded.decode(encoding)
            p_encoded = p_encoded.decode(encoding)
            o_encoded = o_encoded.decode(encoding)
        except Exception as e:
            print("Cant decode: s={} p={} o={}".format(s_encoded, p_encoded, o_encoded))
            if throw:
                raise e
                exit(1)
            else:
                pass

        bucket.append((s_encoded, p_encoded, o_encoded))
        if len(bucket) >= bucket_size:
            yield bucket
            bucket = list()

    if len(bucket) > 0:
        yield bucket


def connect_postgres(dataset):
    """Try to connect to a PostgreSQL server"""
    if 'dbname' not in dataset or 'user' not in dataset or 'password' not in dataset:
        print("Error: a valid PostgreSQL dataset must be declared with fields 'dbname', 'user' and 'password'")
        return None
    dbname = dataset['dbname']
    user = dataset['user']
    password = dataset['password']
    host = dataset['host'] if 'host' in dataset else ''
    port = int(dataset['port']) if 'port' in dataset else 5432
    return psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)


@click.command()
@click.argument("config")
@click.argument("dataset_name")
@click.option('--index/--no-index', default=True,
              help="Enable/disable indexing of SQL tables. The indexes can be created separately using the command sage-postgre-index")
def init_postgres(config, dataset_name, index):
    """
        Initialize the RDF dataset DATASET_NAME with a PostgreSQL/PostgreSQL-MVCC backend, described in the configuration file CONFIG.
    """
    # install logger
    coloredlogs.install(level='INFO', fmt='%(asctime)s - %(levelname)s %(message)s')
    logger = logging.getLogger(__name__)

    # load dataset from config file
    dataset, kind = load_dataset(config, dataset_name, logger, backends=['postgres', 'postgres-mvcc'])
    enable_mvcc = kind == 'postgres-mvcc'

    # init postgre connection
    connection = connect_postgres(dataset)
    if connection is None:
        exit(1)
    # turn off autocommit
    connection.autocommit = False

    # create all SQL queries used to init the dataset, using the dataset name
    table_name = dataset['name']
    create_table_query = p_utils.get_postgres_create_table(table_name, enable_mvcc=enable_mvcc)
    create_indexes_queries = p_utils.get_postgres_create_indexes(table_name, enable_mvcc=enable_mvcc)

    cursor = connection.cursor()
    # create the main SQL table
    logger.info("Creating SQL table {}...".format(table_name))
    cursor.execute(create_table_query)
    logger.info("SPARQL table {} successfully created".format(table_name))

    # create the additional inexes on OSP and POS
    if index:
        logger.info("Creating additional B-tree indexes...")
        for q in create_indexes_queries:
            cursor.execute(q)
        logger.info("Additional B-tree indexes successfully created")
    else:
        logger.info("Skipping additional indexes creation on user-demand")

    # commit and cleanup connection
    logger.info("Committing and cleaning up...")
    connection.commit()
    cursor.close()
    connection.close()
    logger.info("Sage PostgreSQL model for table {} successfully initialized".format(table_name))


@click.command()
@click.argument("config")
@click.argument("dataset_name")
def index_postgres(config, dataset_name):
    """
        Create the additional B-tree indexes on the RDF dataset DATASET_NAME, described in the configuration file CONFIG. The dataset must use the PostgreSQL or PostgreSQL-MVCC backend.
    """
    # install logger
    coloredlogs.install(level='INFO', fmt='%(asctime)s - %(levelname)s %(message)s')
    logger = logging.getLogger(__name__)

    # load dataset from config file
    dataset, kind = load_dataset(config, dataset_name, logger, backends=['postgres', 'postgres-mvcc'])
    enable_mvcc = kind == 'postgres-mvcc'

    # init PostgreSQL connection
    connection = connect_postgres(dataset)
    if connection is None:
        exit(1)
    # turn off autocommit
    connection.autocommit = False
    # create all SQL queries used to init the dataset, using the dataset name
    table_name = dataset['name']
    create_indexes_queries = p_utils.get_postgres_create_indexes(table_name, enable_mvcc=enable_mvcc)

    # create indexes
    cursor = connection.cursor()
    start = time()
    logger.info("Creating additional B-tree indexes...")
    for q in create_indexes_queries:
        cursor.execute(q)
    stop = time()
    logger.info("Additional B-tree indexes successfully created in {}s".format(stop - start))

    # commit and cleanup connection
    logger.info("Committing...")
    connection.commit()
    # run an ANALYZE query to rebuild statistics
    logger.info("Rebuilding table statistics...")
    start = time()
    cursor.execute("ANALYZE {}".format(table_name))
    end = time()
    logger.info("Table statistics successfully rebuilt in {}s".format(end - start))

    logger.info("Committing and cleaning up...")
    connection.commit()
    # quit
    cursor.close()
    connection.close()
    logger.info("Sage PostgreSQL model for table {} successfully initialized".format(table_name))


@click.command()
@click.argument("rdf_file")
@click.argument("config")
@click.argument("dataset_name")
@click.option("-f", "--format", type=click.Choice(["nt", "ttl", "hdt"]),
              default="nt", show_default=True,
              help="Format of the input file. Supported: nt (N-triples), ttl (Turtle) and hdt (HDT).")
@click.option("-b", "--block_size", type=int, default=500, show_default=True,
              help="Block size used for the bulk loading")
@click.option("-c", "--commit_threshold", type=int, default=500000, show_default=True,
              help="Commit after sending this number of RDF triples")
@click.option("-e", "--encoding", type=str, default="utf-8", show_default=True,
              help="Define the encoding of the dataset")
@click.option("--throw/--no-throw", default=True, show_default=True,
              help="if loaded with hdt, throw an error when we cannot convert to utf-8 otherwise pass")
def put_postgres(config, dataset_name, rdf_file, format, block_size, commit_threshold, encoding, throw):
    """
        Insert RDF triples from file RDF_FILE into the RDF dataset DATASET_NAME, described in the configuration file CONFIG. The dataset must use the PostgreSQL or PostgreSQL-MVCC backend.
    """
    # install logger
    coloredlogs.install(level='INFO', fmt='%(asctime)s - %(levelname)s %(message)s')
    logger = logging.getLogger(__name__)

    # load dataset from config file
    dataset, kind = load_dataset(config, dataset_name, logger, backends=['postgres', 'postgres-mvcc'])
    enable_mvcc = kind == 'postgres-mvcc'

    # init PostgreSQL connection
    logger.info("Connecting to PostgreSQL server...")
    connection = connect_postgres(dataset)
    logger.info("Connected to PostgreSQL server")
    if connection is None:
        exit(1)
    # turn off autocommit
    connection.autocommit = False

    # compute SQL table name and the bulk load SQL query
    table_name = dataset['name']
    insert_into_query = p_utils.get_postgres_insert_into(table_name, enable_mvcc=enable_mvcc)

    logger.info("Reading RDF source file...")
    iterator, nb_triples = None, None
    if format == 'nt':
        iterator, nb_triples, file = get_rdf_reader(rdf_file, format=format)
    else:
        iterator, nb_triples = get_rdf_reader(rdf_file, format=format)
    logger.info("RDF source file loaded. Found ~{} RDF triples to ingest.".format(nb_triples))

    logger.info("Starting RDF triples ingestion...")
    cursor = connection.cursor()

    # insert rdf triples
    start = time()
    to_commit = 0
    inserted = 0
    # insert by bucket (and show a progress bar)
    with click.progressbar(length=nb_triples,
                           label="Inserting RDF triples 0/{}, encoding={}".format(nb_triples, encoding)) as bar:
        def do_it(inserted, to_commit, bucket):
            inserted += len(bucket)
            to_commit += len(bucket)
            # bulk load the bucket of RDF triples, then update progress bar
            execute_values(cursor, insert_into_query, bucket, page_size=block_size)
            bar.label = "Inserting RDF triples {}/{}, encoding={}".format(inserted, nb_triples, encoding)
            bar.update(len(bucket))

            # commit if above threshold
            if to_commit >= commit_threshold:
                # logger.info("Commit threshold reached. Committing all changes...")
                connection.commit()
                # logger.info("All changes were successfully committed.")
                to_commit = 0
            return inserted, to_commit

        if format == 'hdt':
            for bucket in bucketify_bytes(iterator, block_size, encoding=encoding, throw=throw):
                inserted, to_commit = do_it(inserted, to_commit, bucket)
        else:
            for bucket in bucketify(iterator, block_size):
                inserted, to_commit = do_it(inserted, to_commit, bucket)
    end = time()

    logger.info("RDF triples ingestion successfully completed in {}s".format(end - start))

    # run an ANALYZE query to rebuild statistics
    logger.info("Rebuilding table statistics...")
    start = time()
    cursor.execute("ANALYZE {}".format(table_name))
    end = time()
    logger.info("Table statistics successfully rebuilt in {}s".format(end - start))

    # commit and cleanup connection
    logger.info("Committing and cleaning up...")
    connection.commit()
    cursor.close()
    connection.close()
    logger.info("RDF data from file '{}' successfully inserted into RDF dataset '{}'".format(rdf_file, table_name))
    if format == 'nt':
        file.close()


class SkipParser(NTriplesParser):
    def skipparse(self, f):
        """Parse f as an N-Triples file."""
        if not hasattr(f, 'read'):
            raise ParseError("Item to parse must be a file-like object.")

        # since N-Triples 1.1 files can and should be utf-8 encoded
        f = codecs.getreader('utf-8')(f)

        self.file = f
        self.buffer = ''
        while True:
            self.line = self.readline()
            if self.line is None:
                break
            try:
                self.parseline()
            except ParseError:
                logger.warning(f"parse error: dropping {self.line}. Reason {sys.exc_info()[0]}")
                continue
                # raise ParseError("Invalid line: %r" % self.line)
        return self.sink

class StreamSink(Sink):
    """
    A sink is used to store the results of parsing, this almost matches the sink
    example shown in ntriples:
      https://github.com/RDFLib/rdflib/blob/395a40101fe133d97f454ee61da0fc748a93b007/rdflib/plugins/parsers/ntriples.py#L43
    """
    #bucket = list()
    length = 0
    to_commit=0

    def __init__(self, bucket,  bucket_size,insert_into_query,cursor,connection,commit_threshold,logger):
        self.bucket=bucket
        self._bucket_size = bucket_size
        self._insert_into_query=insert_into_query
        self._cursor=cursor
        self._connection=connection
        self._commit_threshold=commit_threshold
        # insert rdf triples
        self.to_commit = 0
        self._logger=logger

    def triple(self, s, p, o):
        self.length += 1

        # drop incorrect triple
        try:
            a=f"{s.n3()} {p.n3()} {o.n3()}".encode('utf-8')
        except:
            self._logger.warning(f"print error. ({s},{p},{o}) Dropped.  Reason {sys.exc_info()[0]}")
            return

        #how to store objects ??
        # if literal store starting with \"
        # else store plain text (BNOde or URI)
        if isinstance(o,Literal):
            ow=o.n3()
        else:
            ow=str(o)
        # try:
        #     ow.encode('utf-8')
        # except (UnicodeEncodeError):
        #     ow=ow.encode('utf-8','backslashreplace').decode('utf-8')
        #     self._logger.warning(f"corrected bad encoding for {ow} in {s} {p} {ow}. Reason {sys.exc_info()[0]}")

        # max size for a btree index cell in postgres
        # for experiments !! otherwise index md5 on object
        #ERROR:  index row size 2712 exceeds btree version 4 maximum 2704 for index "pos"
        #Consider a function index of an MD5 hash of the value, or use full text indexing.
        # if sys.getsizeof(ow)<2000:
        self.bucket.append((s, p, ow))
        # else:
        #     self._logger.warning("truncated object {}".format(o))
        #     # 2 bytes for an utf-8 ??
        #     self.bucket.append((s,p,ow[:800]))
        self.to_commit +=1
        if len(self.bucket) >= self._bucket_size:
            try :
                execute_values(self._cursor, self._insert_into_query, self.bucket, page_size=self._bucket_size)
            except:
                self._logger.error(f"bucket rejected {self.bucket}. Reason {sys.exc_info()[0]}")
            self.bucket.clear()

        if self.to_commit >= self._commit_threshold:
            self._connection.commit();
            self._logger.info("inserted {} triples".format(self.length))
            self.to_commit = 0

@click.command()
@click.argument("rdf_file")
@click.argument("config")
@click.argument("dataset_name")
@click.option("-b", "--block_size", type=int, default=100, show_default=True,
              help="Block size used for the bulk loading")
@click.option("-c", "--commit_threshold", type=int, default=500000, show_default=True,
              help="Commit after sending this number of RDF triples")
def stream_postgres(config, dataset_name, rdf_file, block_size, commit_threshold):
    """
        Insert RDF triples from file RDF_FILE into the RDF graph GRAPH_NAME, described in the configuration file CONFIG. The graph must use the PostgreSQL or PostgreSQL-MVCC backend.
    """
    # install logger
    coloredlogs.install(level='INFO', fmt='%(asctime)s - %(levelname)s %(message)s')
    logger = logging.getLogger(__name__)

    # load graph from config file
    dataset, kind = load_dataset(config, dataset_name, logger, backends=['postgres', 'postgres-mvcc'])
    enable_mvcc = kind == 'postgres-mvcc'

    # init PostgreSQL connection
    logger.info("Connecting to PostgreSQL server...")
    connection = connect_postgres(dataset)
    logger.info("Connected to PostgreSQL server")
    if connection is None:
        exit(1)
    # turn off autocommit
    connection.autocommit = False

    # compute SQL table name and the bulk load SQL query
    table_name = dataset['name']
    insert_into_query = p_utils.get_postgres_insert_into(table_name, enable_mvcc=enable_mvcc)

    cursor = connection.cursor()


    logger.info("Reading NT RDF source file...(I hope)")
#    iterator, nb_triples = get_rdf_reader(rdf_file, format=format)

    start = time()
    bucket= list()
    n = SkipParser(StreamSink(bucket, block_size,insert_into_query,cursor,connection,commit_threshold,logger))
    with open(rdf_file, "rb") as anons:
        n.skipparse(anons)
    try :
        #print("remaining bucket:"+str(bucket))
        execute_values(cursor, insert_into_query, bucket, page_size=block_size)
    except:
        logger.error("Failed to insert:"+str(bucket))

    connection.commit()

    end = time()
    logger.info("RDF triples ingestion successfully completed in {}s".format(end - start))

    # run an ANALYZE query to rebuild statistics
    # logger.info("Rebuilding table statistics...")
    # start = time()
    # cursor.execute("ANALYZE {}".format(table_name))
    # end = time()
    # logger.info("Table statistics successfully rebuilt in {}s".format(end - start))

    # commit and cleanup connection
    logger.info("Committing and cleaning up...")
    connection.commit()
    cursor.close()
    connection.close()
    logger.info("RDF data from file '{}' successfully inserted into RDF graph '{}'".format(rdf_file, table_name))
    logger.info(f"Remember to run ANALYZE on Postgres table: {table_name}")