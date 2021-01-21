# coding: utf8
# postgres.py
# Author: Thomas MINIER - MIT License 2017-2019
import logging
from time import time

import click
import coloredlogs
import sys
import codecs
import pylru
import sqlite3
from rdflib.util import from_n3
from rdflib import BNode, Literal, URIRef, Variable,Graph
from sage.cli.utils import load_dataset, get_rdf_reader
from rdflib.plugins.parsers.ntriples import NTriplesParser, Sink, ParseError

coloredlogs.install(level='INFO', fmt='%(asctime)s - %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

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


def connect_sqlite(dataset):
    if 'database' not in dataset:
        print("Error: a valid SQlite dataset must be declared with a field 'database'")
        return None
    database = dataset['database']
    return sqlite3.connect(database)


def get_create_tables_queries(table_name, backend):
    """Format a postgre CREATE TABLE with the name of a SQL table"""
    if backend == 'sqlite':
        return [(
            f'CREATE TABLE {table_name} ('
            f'subject TEXT, '
            f'predicate TEXT, '
            f'object TEXT);'
        )]
    elif backend == 'sqlite-catalog':
        return [
            (
                f'CREATE TABLE {table_name}_catalog ('
                f'id INTEGER PRIMARY KEY, '
                f'value TEXT UNIQUE);'
            ),
            (
                f'CREATE TABLE {table_name} ('
                f'subject INTEGER, '
                f'predicate INTEGER, '
                f'object INTEGER);'
            )
        ]
    else:
        raise Exception(f'Unknown backend: {backend}')


def get_create_indexes_queries(table_name, backend):
    """Format all postgre CREATE INDEXE with the name of a SQL table"""
    if backend == 'sqlite':
        return [
            f'CREATE INDEX {table_name}_spo_index ON {table_name} (subject,predicate,object);',
            f'CREATE INDEX {table_name}_osp_index ON {table_name} (object,subject,predicate);',
            f'CREATE INDEX {table_name}_pos_index ON {table_name} (predicate,object,subject);'
        ]
    elif backend == 'sqlite-catalog':
        return [
            f'CREATE INDEX {table_name}_spo_index ON {table_name} (subject,predicate,object);',
            f'CREATE INDEX {table_name}_osp_index ON {table_name} (object,subject,predicate);',
            f'CREATE INDEX {table_name}_pos_index ON {table_name} (predicate,object,subject);'
        ]
    else:
        raise Exception(f'Unknown backend: {backend}')


@click.command()
@click.argument("config")
@click.argument("dataset_name")
@click.option('--index/--no-index', default=True,
              help="Enable/disable indexing of SQL tables. The indexes can be created separately using the command sage-postgre-index")
def init_sqlite(config, dataset_name, index):
    """
        Initialize the RDF dataset DATASET_NAME with a SQlite backend, described in the configuration file CONFIG.
    """
    # load dataset from config file
    dataset, backend = load_dataset(config, dataset_name, logger, backends=['sqlite', 'sqlite-catalog'])

    # init postgre connection
    connection = connect_sqlite(dataset)
    if connection is None:
        exit(1)

    # create all SQL queries used to init the dataset, using the dataset name
    table_name = dataset['name']
    create_tables_queries = get_create_tables_queries(table_name, backend)
    create_indexes_queries = get_create_indexes_queries(table_name, backend)

    cursor = connection.cursor()
    # create the main SQL table
    logger.info("Creating SQL tables for {}...".format(table_name))
    for query in create_tables_queries:
        cursor.execute(query)
    # cursor.execute(create_table_query)
    logger.info("SPARQL tables for {} successfully created".format(table_name))

    # create the additional inexes on OSP and POS
    if index:
        logger.info("Creating additional B-tree indexes...")
        for query in create_indexes_queries:
            cursor.execute(query)
        logger.info("Additional B-tree indexes successfully created")
    else:
        logger.info("Skipping additional indexes creation on user-demand")

    # commit and cleanup connection
    logger.info("Committing and cleaning up...")
    connection.commit()
    cursor.close()
    connection.close()
    logger.info("Sage SQlite model for table {} successfully initialized".format(table_name))


@click.command()
@click.argument("config")
@click.argument("dataset_name")
def index_sqlite(config, dataset_name):
    """
        Create the additional B-tree indexes on the RDF dataset DATASET_NAME, described in the configuration file CONFIG. The dataset must use the PostgreSQL or PostgreSQL-MVCC backend.
    """
    # load dataset from config file
    dataset, backend = load_dataset(config, dataset_name, logger, backends=['sqlite', 'sqlite-catalog'])

    # init PostgreSQL connection
    connection = connect_sqlite(dataset)
    if connection is None:
        exit(1)

    # create all SQL queries used to init the dataset, using the dataset name
    table_name = dataset['name']
    create_indexes_queries = get_create_indexes_queries(table_name, backend)

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
    logger.info("Sage SQlite model for table {} successfully initialized".format(table_name))


def insert_bucket(cursor, bucket, cache, table_name, backend):
    if backend == 'sqlite':
        insert_query = f'INSERT INTO {table_name} (subject,predicate,object) VALUES (?, ?, ?);'
        cursor.executemany(insert_query, bucket)
    elif backend == 'sqlite-catalog':
        # Insert terms
        insert_into_catalog_query = f'INSERT INTO {table_name}_catalog (value) VALUES (?) ON CONFLICT DO NOTHING'
        terms = []
        for (s, p, o) in bucket:
            if s not in cache:
                terms.append([s])
            if p not in cache:
                terms.append([p])
            if o not in cache:
                terms.append([o])
        if len(terms) > 0:
            cursor.executemany(insert_into_catalog_query, terms)
        # Insert triples
        triples = []
        for (s, p, o) in bucket:
            if s not in cache:
                cursor.execute(f'SELECT id FROM {table_name}_catalog WHERE value = \'{s}\'')
                subject_id = cursor.fetchone()[0]
                cache[s] = subject_id
            else:
                subject_id = cache[s]
            if p not in cache:
                cursor.execute(f'SELECT id FROM {table_name}_catalog WHERE value = \'{p}\'')
                predicate_id = cursor.fetchone()[0]
                cache[p] = predicate_id
            else:
                predicate_id = cache[p]
            if o not in cache:
                cursor.execute(f'SELECT id FROM {table_name}_catalog WHERE value = \'{o}\'')
                object_id = cursor.fetchone()[0]
                cache[o] = object_id
            else:
                object_id = cache[o]
            triples.append((subject_id, predicate_id, object_id))
        insert_query = f'INSERT INTO {table_name} (subject,predicate,object) VALUES (?, ?, ?);'
        cursor.executemany(insert_query, triples)
    else:
        raise Exception(f'Unknown backend: {backend}')


@click.command()
@click.argument("rdf_file")
@click.argument("config")
@click.argument("dataset_name")
@click.option("-f", "--format", type=click.Choice(["nt", "ttl", "hdt"]),
              default="nt", show_default=True,
              help="Format of the input file. Supported: nt (N-triples), ttl (Turtle) and hdt (HDT).")
@click.option("-b", "--block_size", type=int, default=1000, show_default=True,
              help="Block size used for the bulk loading")
@click.option("-c", "--commit_threshold", type=int, default=500000, show_default=True,
              help="Commit after sending this number of RDF triples")
@click.option("-e", "--encoding", type=str, default="utf-8", show_default=True,
              help="Define the encoding of the dataset")
@click.option("--throw/--no-throw", default=True, show_default=True,
              help="if loaded with hdt, throw an error when we cannot convert to utf-8 otherwise pass")
def put_sqlite(config, dataset_name, rdf_file, format, block_size, commit_threshold, encoding, throw):
    """
        Insert RDF triples from file RDF_FILE into the RDF dataset DATASET_NAME, described in the configuration file CONFIG. The dataset must use the PostgreSQL or PostgreSQL-MVCC backend.
    """
    # load dataset from config file
    dataset, backend = load_dataset(config, dataset_name, logger, backends=['sqlite', 'sqlite-catalog'])

    # init PostgreSQL connection
    connection = connect_sqlite(dataset)
    if connection is None:
        exit(1)

    # compute SQL table name and the bulk load SQL query
    table_name = dataset['name']

    logger.info("Reading RDF source file...")
    iterator, nb_triples = None, None
    if format == 'nt':
        iterator, nb_triples, data_file = get_rdf_reader(rdf_file, format=format)
    else:
        iterator, nb_triples = get_rdf_reader(rdf_file, format=format)
    logger.info("RDF source file loaded. Found ~{} RDF triples to ingest.".format(nb_triples))

    logger.info("Starting RDF triples ingestion...")
    cursor = connection.cursor()

    # insert rdf triples
    start = time()
    to_commit = 0
    inserted = 0
    cache = pylru.lrucache(10000000)
    # insert by bucket (and show a progress bar)
    with click.progressbar(length=nb_triples,
                           label="Inserting RDF triples 0/{}, encoding={}".format(nb_triples, encoding)) as bar:
        def do_it(inserted, to_commit, bucket):
            inserted += len(bucket)
            to_commit += len(bucket)
            # bulk load the bucket of RDF triples, then update progress bar
            insert_bucket(cursor, bucket, cache, table_name, backend)
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
        data_file.close()