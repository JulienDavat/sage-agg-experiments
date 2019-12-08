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

SAGE_NTRIPLES_REGEX = re.compile('^(\<.*\>) (\<.*\>) (.*|.*\n.*) .$')

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
    block_size = 2000
    parsed = 0
    print('-> starting yielding...')
    to_read = ""
    for cnt, line in enumerate(file):
        print(cnt, line)
        try:
            # do not touch  all the lines below. We read every new line,
            # if we see a triple on multiple lines aka we cant find any matches, we continue to read
            # and we accumulate the string and test the accumulated string, on a match we continue
            if to_read == "":
                triple = SAGE_NTRIPLES_REGEX.findall(line)
                to_read += line
                if len(triple) > 0:
                    triple = triple[0]
                    blocks.append((from_n3(triple[0]), from_n3(triple[1]), from_n3(triple[2])))
                    parsed += 1
                    to_read = ""
                else:
                    to_read = to_read.replace('\n', '')
            else:
                to_read += line
                triple = SAGE_NTRIPLES_REGEX.findall(to_read)
                # try to eat it with rdflib
                if len(triple) > 0:
                    triple = triple[0]
                    blocks.append((from_n3(triple[0]), from_n3(triple[1]), from_n3(triple[2])))
                    parsed += 1
                    to_read = ""
                else:
                    to_read = to_read.replace('\n', '')
                    # try:
                    #     #print('try to RDFLIB EAT: ', to_read)
                    #     g = Graph('IOMemory')
                    #     g.parse(data=to_read, format='nt')
                    #     triple = next(g.triples((None, None, None)))
                    #     blocks.append((from_n3(triple[0]), from_n3(triple[1]), from_n3(triple[2])))
                    #     parsed += 1
                    #     to_read = ""
                    #     g.close()
                    # except Exception as e:
                    #     print("Error({})".format(e))
                    #     # continue to eat
                    #     pass
            if cnt % block_size == 0:
                parsed = 0
                for t in blocks:
                    total += 1
                    yield __n3_to_str(t)
                blocks = []
        except Exception as err:
            print(err)
            print(line)
            exit(1)

    if len(blocks) > 0:
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
