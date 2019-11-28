import psycopg2
from time import time

dbname = 'grall-a'
user = 'grall-a'
password = ''
host = '172.16.8.50'
port = 7122
table_name = 'dbpedia351'

# count_total = 1000000
count_total = 232542405

page_size = 100000
max=10000000

def main(dbname, user, password, host, port, table_name, page_size):
    print(dbname, user, password, host, port, table_name, page_size)
    connection = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
    start_query = "SELECT * FROM {} ORDER BY md5(subject), md5(predicate), md5(object) FETCH first {} ROWS ONLY"
    resume_query = "SELECT * FROM {} WHERE (md5(subject), md5(predicate), md5(object)) > (md5(%s), md5(%s), md5(%s)) ORDER BY md5(subject), md5(predicate), md5(object) FETCH first {} ROWS ONLY"
    cursor = connection.cursor("mycursor")
    cpt = 0
    total = 0
    real_start = time()
    finished = False
    last = None
    size = 10000
    while not finished:
        if last is None:
            print("------ Start ({}) -----".format(page_size))
            query = start_query.format(table_name, page_size)
        else:
            print("------ Next ({}) -----".format(page_size))
            query = resume_query.format(table_name, page_size)
        print('=> (START) Processing ...')
        start = time()
        try:
            st = time()
            cursor = connection.cursor("mycursor")
            cursor.execute(query, last)
            print("=> time to execute the cursor.execute() = {}".format(time() - st))
            st = time()
            for record in cursor:
                total += 1
                last = record
            cursor.close()
            # if cur is None or len(cur) == 0 or total >= max:
            #     finished = True
            #     pass
            # else:
            #     total += len(cur)
            #     while cur is not None and len(cur) > 0:
            #         print(" => fetching {} results out of {}".format(len(cur), page_size))
            #         last = cur[len(cur) - 1]
            #         cur = cursor.fetchmany(size=size)
            #         total += len(cur)
            #     print("=> time to fetch using fetchmany({}) = {}".format(size, time() - st))
            #
            total_duration = time() - real_start
            print('=> (END) ({}, {}, {}) (progression: {}%)'.format(time() - start, total_duration, total, total / count_total * 100))
            print('Throughput: {}/second'.format(total / total_duration))
            cpt += 1
        except Exception as e:
            print(query)
            raise e
    print('END => with {} calls to postgres database.'.format(cpt))
    cursor.close()
    connection.close()



main(dbname, user, password, host, port, table_name, page_size)