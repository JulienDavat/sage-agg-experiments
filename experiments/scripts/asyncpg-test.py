import asyncio
import asyncpg
from time import time
from asyncio import get_event_loop, set_event_loop, set_event_loop_policy
import uvloop

set_event_loop_policy(uvloop.EventLoopPolicy())

async def run():
    connection = await asyncpg.connect(user='grall-a', password='',
                                 database='grall-a', host='172.16.8.50', port=7122)

    start_query = "SELECT * FROM {} ORDER BY md5(subject), md5(predicate), md5(object) LIMIT {}"
    resume_query = "SELECT * FROM {} WHERE (md5(subject), md5(predicate), md5(object)) > (md5($1), md5($2), md5($3)) ORDER BY md5(subject), md5(predicate), md5(object) LIMIT {}"
    count_total = 232542405
    page_size = 100000
    max = 10000000
    table_name = "dbpedia351"
    cpt = 0
    total = 0
    real_start = time()
    finished = False
    last = None

    await connection.set_type_codec(
        'text',
        encoder=lambda e: e,
        decoder=lambda e: e,
        schema='pg_catalog'
    )
    while not finished:
        if last is None:
            print("------ Start -----")
            query = start_query.format(table_name, page_size)
        else:
            print("------ Next -----")
            query = resume_query.format(table_name, page_size)
        print('=> (START) Processing ...')

        stmt = await connection.prepare(query)
        async with connection.transaction():
            start = time()
            c = 0
            if last is None:
                async for record in stmt.cursor():
                    last = record
                    c += 1
            else:
                async for record in stmt.cursor((last["subject"], last["predicate"], last["object"])):
                    last = record
                    c += 1
            total += c
            total_duration = time() - real_start
            print('=> (END) ({}, {}, {}) (progression: {}%)'.format(time() - start, total_duration, total, total / count_total * 100))
            print('Throughput: {}/second'.format(total / total_duration))
            cpt += 1
    print('END => with {} calls to postgres database.'.format(cpt))

    await connection.close()

loop = get_event_loop()
loop.run_until_complete(run())