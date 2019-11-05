import csv
import argparse

def transform(e):
    try:
        return float(e)
    except:
        return 0

parser = argparse.ArgumentParser(description='Create an average output csv file based on all files provided. '
                                             'They should have the same schema. ')
parser.add_argument('-file', type=argparse.FileType('r'), nargs='+', help='All files to parse')
parser.add_argument('-o', action='store', default='average.csv', help='Output file')
args = parser.parse_args()

print('===> Computing average on ', len(args.file), ' files and outputting into: ', args.o)

results = []

nb_files = 0
nb_rows = 0
first = True
for f in args.file:
    tab = []
    rows = csv.reader(f, delimiter=',')
    if first:
        for row in rows:
            elem = list(map(transform, row))
            results.append(elem)
            nb_rows += 1
        first = False
    else:
        j = 0
        for row in rows:
            elem = list(map(transform, row))
            results[j] = list(map(lambda x, y: x + y, results[j], elem))
            j += 1
    nb_files += 1

# computing average
i = 0

output = open(args.o, 'w')
for row in results:
    j = 0
    for e in row:
        results[i][j] = results[i][j] / nb_files
        j += 1

    elem = map(lambda e: str(e), results[i])
    toPrint = ','.join(elem)
    output.write(toPrint + "\n")
    i += 1
output.close()

