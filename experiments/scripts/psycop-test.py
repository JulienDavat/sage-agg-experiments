import psycopg2
from time import time
import math

dbname = 'grall-a'
user = 'grall-a'
password = ''
host = '172.16.8.50'
port = 7122
table_name = 'dbpedia351'

# count_total = 1000000
count_total = 232542405

page_size = 1000


def main(dbname, user, password, host, port, table_name, page_size):
    print(dbname, user, password, host, port, table_name, page_size)
    connection = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)

    start_query = "SELECT * FROM {} ORDER BY md5(subject), md5(predicate), md5(object)".format(table_name)
    # start_query = "SELECT * FROM {} ORDER BY subject, predicate, object".format(table_name)
    cursor = connection.cursor("mycursor")
    cpt = 0
    total = 0
    real_start = time()
    finished = False
    last = None

    start = time()
    cursor = connection.cursor("mycursor")
    cursor.itersize = page_size
    cursor.execute(start_query, None)
    print("=> time to execute the cursor.execute() = {}".format(time() - start))
    step = 0.001
    ind = 0
    record = cursor.fetchmany(page_size)
    while len(record) > 0 and record is not None:
        while len(record) > 0:
            total += 1
            last = record.pop()
        if total / count_total * 100 > ind:
            ind += step
            print(total, count_total)
            d = time() - start
            print('({}) progression: {} %, throughtput: {}/s'.format(d, total / count_total * 100, total / d))
        record = cursor.fetchmany(page_size)



    # for record in cursor:
    #     total += 1
    #     if total / count_total * 100 > ind:
    #         ind += step
    #         print(total, count_total)
    #         d = time() - st
    #         print('({}) progression: {} %, throughtput: {}/s'.format(d, total / count_total * 100, total / d))
    #     last = record

    total_duration = time() - start
    print('=> (END) in {} s'.format(time() - start))
    print('Throughput: {}/second'.format(total / total_duration))

    cursor.close()
    connection.close()


main(dbname, user, password, host, port, table_name, page_size)