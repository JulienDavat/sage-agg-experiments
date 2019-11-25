import hdt
import argparse
import os
parser = argparse.ArgumentParser(description='Transform an hdt file (with indexes) into an ntriples file by elminating all non utf-8 strings')
parser.add_argument('-i', action='store', default='dataset.hdt', help='Input file in HDT format')
parser.add_argument('-o', action='store', default='dataset.nt', help='Output file in ntriples format')
args = parser.parse_args()

def transform(input, output):
    print('Parsing: ', input, ' into ', output)
    if os.path.exists(output):
        os.remove(output)
    else:
        print("Output file does not exist")

    with open(output, 'a') as file:
        # now open the hdt document,
        doc = hdt.HDTDocument(input, True, True)
        it, card = doc.search_triples_bytes("", "", "")
        print('Cardinality: ', card)
        red = 0
        step = card / 100
        status = 0
        for s, p, o in it:
            red += 1
            # now decode it, or handle any error
            try:
                s, p, o = s.decode('UTF-8'), p.decode('UTF-8'), o.decode('UTF-8')
                if not s.startswith('\"'):
                    s = "<{}>".format(s)
                if not p.startswith("\""):
                    p = "<{}>".format(p)
                if not o.startswith("\""):
                    o = "<{}>".format(o)

                to_write = "{} {} {} . \n".format(s, p, o)
                file.write(to_write)
            except UnicodeDecodeError as err:
                # try another other codecs
                pass
            if red > (step * status):
                print("Processing ... ({}%)".format(status))
                status += 1

transform(args.i, args.o)