import argparse
import psycopg2
from time import time
import os

parser = argparse.ArgumentParser(description='Dump of postresql table where columns are: subject,predicate,object')
parser.add_argument('-connect', action='store', help='Information to connect to database where the table is stored')
parser.add_argument('-table', action='store', help='The name of the table to dump')
parser.add_argument('-page-size', action='store', help='Fetch size', default=50000)
parser.add_argument('-output', action='store', help='Output file')
args = parser.parse_args()

page_size = float(args.page_size)
print('Fetch size:', page_size)
connection = psycopg2.connect(args.connect)
query = "SELECT * FROM {};".format(args.table)
cursor = connection.cursor("mycursor")
cursor.execute(query, None)
print('Output file: ', args.output)
file = None

if os.path.exists(args.output):
    print('Already exsists, removing content...')
    os.remove(args.output)
    file = open(args.output, 'w')
else:
    file = open(args.output, 'w')

def process (record):
    s, p, o = record
    if not s.startswith('"'):
        s = "<{}>".format(s)
    if not p.startswith('"'):
        p = "<{}>".format(p)
    if not o.startswith('"'):
        o = "<{}>".format(o)
    return "{} {} {} .\n".format(s, p, o)

start = time()
records = cursor.fetchmany(size=page_size)
print('=> processing... ')
red = 0
block = []
b = 0
while len(records) > 0 and records is not None:
    for record in records:
        block.append(process(record))
        red += 1
        if len(block) > page_size:
            b += 1
            file.write("".join(block))
            block = []
            if b % 100 == 0:
                print('# {}/s'.format(red / (time() - start)))
            else:
                print('#', end='')
    records = cursor.fetchmany(size=page_size)
    # print('=> Fetching {} triples from {} ...'.format(page_size, args.table))
if len(block) > 0:
    file.write("".join(block))
print('\n*****> Parsed {} triples in {} seconds'.format(red, time() - start))
file.close()