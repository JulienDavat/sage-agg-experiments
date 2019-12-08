# utils.py
# Author: Thomas MINIER - MIT License 2017-2019
from sys import exit
from os.path import isfile
from yaml import load
from rdflib import Graph
from hdt import HDTDocument
import subprocess
from time import time
import re
from rdflib.util import from_n3

SAGE_NTRIPLES_REGEX = re.compile('(\<.*\>) (\<.*\>) (.*) .')

def load_dataset(config_path, dataset_name, logger, backends=[]):
    """Load a dataset from a Sage config file"""
    if isfile(config_path):
        config = load(open(config_path))
        if 'datasets' not in config:
            logger.error("No RDF datasets declared in the configuration provided")
            exit(1)
        datasets = config['datasets']
        dataset = None
        kind = None
        for d in datasets:
            if d['name'] == dataset_name and d['backend'] in backends:
                dataset = d
                kind = d['backend']
                break
        if dataset is None:
            logger.error("No compatible RDF dataset named '{}' declared in the configuration provided".format(dataset_name))
            exit(1)
        return dataset, kind
    else:
        logger.error("Invalid configuration file supplied '{}'".format(config_path))
        exit(1)


def __n3_to_str(triple):
    s, p, o = triple
    s = s.n3()
    p = p.n3()
    o = o.n3()
    if s.startswith('<') and s.endswith('>'):
        s = s[1:len(s) - 1]
    if p.startswith('<') and p.endswith('>'):
        p = p[1:len(p) - 1]
    if o.startswith('<') and o.endswith('>'):
        o = o[1:len(o) - 1]
    return (s, p, o, triple)


def wccount(filename):
    return int(subprocess.run('wc -l ' + filename + " | awk '{print $1}'",
                              shell=True,
                              text=True,
                              stdout=subprocess.PIPE).stdout)

def yield_triples(file):
    total = 0
    blocks = []
    block_size = 100000
    start = time()
    parsed = 0
    print('-> starting yielding...')
    for cnt, line in enumerate(file):
        triple = SAGE_NTRIPLES_REGEX.findall(line)[0]
        blocks.append((from_n3(triple[0]), from_n3(triple[1]), from_n3(triple[2])))
        parsed += 1
        if cnt % block_size == 0:
            print('-> Parsed {} triples in {} s'.format(parsed, time() - start))
            parsed = 0
            for t in blocks:
                total += 1
                yield __n3_to_str(t)
            blocks = []
            start = time()

    if len(blocks) > 0:
        start = time()
        print('-> Parsed {} triples in {} s'.format(parsed, time() - start))
        for t in blocks:
            total += 1
            yield __n3_to_str(t)
    print('-> yielded {} triples'.format(total))



def get_rdf_reader(file_path, format='nt'):

    """Get an iterator over RDF triples from a file"""
    iterator = None
    nb_triples = 0
    # load using rdflib
    if format == 'ttl':
        g = Graph()
        g.parse(file_path, format=format)
        nb_triples = len(g)
        iterator = map(__n3_to_str, g.triples((None, None, None)))
    elif format == 'nt':
        print('Counting triples using the wc command...')
        total = wccount(file_path)
        print('The file contains {} triples.'.format(total))
        f = open(file_path, 'r')
        iter = yield_triples(f)
        return iter, total, f

    elif format == 'hdt':
        # load HDTDocument without additional indexes (not needed since we do a ?s ?p ?o)
        doc = HDTDocument(file_path, True, True)
        iterator, nb_triples = doc.search_triples_bytes("", "", "")
    return iterator, nb_triples
