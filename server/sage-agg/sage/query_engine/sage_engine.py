# sage_engine.py
# Author: Thomas MINIER - MIT License 2017-2018
from asyncio import Queue, wait_for, sleep, set_event_loop_policy, new_event_loop, set_event_loop, get_running_loop
from asyncio import TimeoutError as asyncTimeoutError
from math import inf

import uvloop
from sage.query_engine.iterators.utils import IteratorExhausted
from sage.query_engine.protobuf.iterators_pb2 import RootTree

set_event_loop_policy(uvloop.EventLoopPolicy())


class TooManyResults(Exception):
    """
        Exception raised when the max. number of results for a query execution
        has been exceeded
    """
    pass


async def executor(plan, queue, limit):
    """Executor used to evaluated a plan under a time quota"""
    try:
        cpt = 0
        agg = False
        if plan.is_aggregator():
            agg = True
        while plan.has_next():
            value = await plan.next()
            # discard None values
            cpt += 1
            if value is not None:
                await queue.put(value)
            if queue.qsize() >= limit:
                raise TooManyResults()
            # WARNING: await sleep(0) cost a lot, so we only trigger it every 50 cycle.
            # additionnaly, there may be other call to await sleep(0) in index join in the pipeline.
            if cpt > 50:
                cpt = 0
                await sleep(0)
    except IteratorExhausted:
        pass
    except StopIteration:
        pass


class SageEngine(object):
    """SaGe query engine, used to evaluated a preemptable physical query execution plan"""

    def __init__(self):
        super(SageEngine, self).__init__()
        try:
            self._loop = get_running_loop()
        except RuntimeError as e:
            self._loop = new_event_loop()
            set_event_loop(self._loop)

    def execute(self, plan, quota, limit=inf):
        """
            Execute a preemptable physical query execution plan under a time quota.

            Args:
                - plan :class:`.PreemptableIterator` - The root of the plan
                - quota ``float`` - The time quota used for query execution

            Returns:
                A tuple (``results``, ``saved_plan``, ``is_done``) where:
                - ``results`` is a list of solution mappings found during query execution
                - ``saved_plan`` is the state of the plan saved using protocol-buffers
                - ``is_done`` is True when the plan has completed query evalution, False otherwise
        """
        results = list()
        queue = Queue()
        query_done = False
        try:
            task = wait_for(executor(plan, queue, limit), timeout=quota)
            self._loop.run_until_complete(task)
            query_done = True
        except asyncTimeoutError:
            pass
        except TooManyResults:
            pass
        finally:
            # dont forget to close the event loop or we get a Too Many open files OSError
            self._loop.close()
            # fetch partial aggregate if the query is an aggreation query
            if plan.is_aggregator():
                results += plan.generate_results()
            if results == [{'?__default_group': 'https://sage-org.github.io/sage-engine#DefaultGroupKey'}]:
                results = []
            # collect results from classic query
            while not queue.empty():
                results.append(queue.get_nowait())
            # print('Returning response ({}) ...'.format(time() - start))
        # print('final results:', results)
        saved_plan = plan.save()
        root = RootTree()
        source_field = plan.serialized_name() + '_source'
        getattr(root, source_field).CopyFrom(saved_plan)
        return results, root, query_done
