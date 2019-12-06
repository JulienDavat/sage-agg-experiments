import hdt
import argparse
import os
from time import time
import click
from rdflib import Graph
from rdflib.util import from_n3
import re

parser = argparse.ArgumentParser(description='Transform an hdt file (with indexes) into an ntriples file by elminating all non utf-8 strings')
parser.add_argument('-i', action='store', default='dataset.hdt', help='Input file in HDT format')
parser.add_argument('-o', action='store', default='dataset.nt', help='Output file in ntriples format')
args = parser.parse_args()

block_size = 500

LANG_PATTERN = re.compile('\"([\w\W]*)\"(@)(.+)')
TYPE_PATTERN = re.compile('\"([\w\W]*)\"(\^\^)(.+)')


def escape(lit):
    return lit.replace('"', '\\"').replace('\r', '\\r').replace('\n', '\\n')

def formatTriple(e):
    return e[0] + " " + e[1] + " " + e[2] + " . \n"


def write(file, block):
    if len(block) == 1:
        s = "" + formatTriple(block[0])
    else:
        s = "".join(map(lambda e: formatTriple(e), block))
    file.write(s)

def transform(input, output):
    print('Parsing: ', input, ' into ', output)
    if os.path.exists(output):
        print("Output file deleted first")
        os.remove(output)
    else:
        print("Output file does not exist")

    with open(output, 'a') as file:
        # now open the hdt document,
        doc = hdt.HDTDocument(input, True, True)
        it, card = doc.search_triples_bytes("", "", "")
        print('Cardinality: ', card)
        red = 0
        block = []
        start = time()
        with click.progressbar(label="Progression", length=card) as bar:
            for s, p, o in it:
                # now decode it, or handle any error
                try:
                    s, p, o = s.decode('UTF-8'), p.decode('UTF-8'), o.decode('UTF-8')
                    if not s.startswith('\"'):
                        s = "<{}>".format(s)
                    else:
                        raise Exception("subject not handled")
                    if not p.startswith("\""):
                        p = "<{}>".format(p)
                    else:
                        raise Exception("predicate not handled")
                    if not o.startswith("\""):
                        o = "<{}>".format(o)
                    #else:
                        if LANG_PATTERN.match(o):
                            lit, symbol, lang = LANG_PATTERN.findall(o)[0]
                            lit = escape(lit)
                            o = '"{}"{}{}'.format(lit, symbol, lang)
                        elif TYPE_PATTERN.match(o):
                            lit, symbol, type = TYPE_PATTERN.findall(o)[0]
                            lit = escape(lit)
                            o = '"{}"{}{}'.format(lit, symbol, type)
                        else:
                            o = '"' + escape(o[1:len(o) - 1]) + '"'

                    graph = Graph('IOMemory')
                    graph.add((from_n3(s), from_n3(p), from_n3(o)))
                    triples = graph.triples((None, None, None))
                    for s, p, o in triples:
                        block.append((s.n3(), p.n3(), o.n3()))
                        red += 1

                    if len(block) == block_size:
                        write(file, block)
                        bar.label = "Progression ({}/{}) ".format(red, card)
                        bar.update(block_size)
                        block = []
                except UnicodeDecodeError as err:
                    pass
                except Exception as err:
                    print(err)
                    exit(1)
            if len(block) > 0:
                write(file, block)
            print("\n {} triples written in {} seconds".format(red, time() - start))
transform(args.i, args.o)