import psycopg2
from time import time
import math
from functools import reduce

dbname = 'grall-a'
user = 'grall-a'
password = ''
host = 'localhost'
port = 5432
table_name = 'bsbm1k'

# count_total = 1000000
count_total = 371911

page_size = 100000


def main(dbname, user, password, host, port, table_name, page_size):
    print(dbname, user, password, host, port, table_name, page_size)
    connection = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)

    start_query = "SELECT * FROM {} ORDER BY subject, predicate, md5(object)".format(table_name)
    resume_query = "SELECT * FROM {} where (subject, predicate, md5(object)) > (%s, %s, md5(%s)) ORDER BY subject, predicate, md5(object)".format(table_name)
    total = 0
    last = None

    start = time()
    cursor = connection.cursor("mycursor")
    cursor.execute(start_query, None)
    print("=> time to execute the cursor.execute() = {}".format(time() - start))
    step = 0.001
    ind = 0
    record = cursor.fetchmany(size=page_size)
    while len(record) > 0 and record is not None:
        st = time()
        ov = []
        while len(record) > 0:
            total += 1
            cur = record.pop()
            if st is not None:
                c = time()
                ov.append(c - st)
                st = c
            if last == cur:
                raise Exception("last == cur")
            last = cur
        print('avg time: ', reduce(lambda a, b: a+b, ov) / len(ov))
        if total / count_total * 100 > ind:
            ind += step
            print(total, count_total)
            d = time() - start
            print('({}) progression: {} %, throughtput: {}/s'.format(d, total / count_total * 100, total / d))
        if last is not None:
            cursor.close()
            cursor = connection.cursor("mycursor")
            cursor.execute(resume_query, last)
            record = cursor.fetchmany(size=page_size)
            if total > count_total:
                print(resume_query, last)



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