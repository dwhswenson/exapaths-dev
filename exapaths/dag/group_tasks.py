import random
import collections
from collections import abc

from exapaths.dag.dag import ExecutingDAG, DAG
from exapaths.move_to_ops.tasknodes import StorageTaskNode, TaskBatch

import logging
_logger = logging.getLogger(__name__)

"""
Algorithm for reorganizing tasks so that each task has exactly one expensive
move.

1. Start with the DAG of tasks. Identify available tasks.
2. Pick one available task. Mark the others as "capped" (unavailable)
3. Perform an execution cycle. Mark tasks as completed until either:
    a. A second expensive task is identified. At this point, terminate this
       batching.
    b. No tasks are available. At this point, uncap one of the capped tasks,
       making it available. Restart the execution cycle.
"""


class TaskGrouper:
    # TODO: extend this to have more generalized rules of task batches
    def __init__(self, max_expensive=1):
        self.max_expensive = max_expensive

    def is_expensive(self, task):
        raise NotImplementedError()

    def _execution_cycle(self, dag):
        n_expensive = 0
        while True:
            try:
                task = dag.find_next_node()
            except StopIteration:
                # no tasks available
                if n_expensive < self.max_expensive:
                    # keep going? uncap an expensive one
                    if capped := dag.capped:
                        chosen = random.choice(list(capped))
                        dag.uncap_nodes([chosen])
                        continue
                    else:
                        return
                else:
                    # not capped and no tasks: must be done!
                    return
            else:
                # process the task we got
                is_expensive = self.is_expensive(task)
                if is_expensive and n_expensive >= self.max_expensive:
                    # cap and move on
                    dag.cap_nodes([task])
                    continue
                else:
                    if is_expensive:
                        n_expensive += 1

                    yield task
                    dag.mark_node_completed(task)

    def rebatch(self, dag):
        # make a execution copy of the DAG
        exec_dag = ExecutingDAG(dag)
        rebatch_edges = set()

        # mapping of node to all nodes it directly depends on
        node_to_input = collections.defaultdict(set)
        for n1, n2 in dag.edges:
            node_to_input[n2].add(n1)

        # mapping of final nodes from a batch (ready to be linked) the batch
        # it was in
        node_to_batch = {}
        nodes = set()
        while list_batch := list(self._execution_cycle(exec_dag)):
            batch = TaskBatch(list_batch)
            nodes.add(batch)
            node_to_batch.update({node: batch for node in batch})
            for node in batch:
                external = {inp for inp in node_to_input[node]
                            if inp not in batch}
                edges = {(node_to_batch[ext], batch) for ext in external}
                rebatch_edges |= edges

        return DAG(rebatch_edges, nodes=nodes)


class OPSTaskGrouper(TaskGrouper):
    def is_expensive(self, task):
        cheap_movers = {
            'ReplicaExchangeMover',
            'PathReversalMover',
        }
        return task.mover.__class__.__name__ not in cheap_movers


class StorageRegrouper(TaskGrouper):
    def _execution_cycle(self, dag):
        storage_tasks = []
        while True:
            try:
                task = dag.find_next_node()
            except StopIteration:
                # no tasks available, either all capped or we're done
                if storage_tasks:
                    # can't add tasks; yield the ones we've save and finish
                    yield from storage_tasks
                    return
                else:
                    if capped := dag.capped:
                        # uncap a task and continue
                        chosen = random.choice(list(capped))
                        dag.uncap_nodes([chosen])
                        continue
                    else:
                        # no available tasks; no capped tasks: we're done
                        return
            else:
                # we got a task; handle it
                if isinstance(task, StorageTaskNode):
                    # add to our storage tasks
                    storage_tasks.append(task)
                    dag.mark_node_completed(task)
                    continue
                else:
                    # task is an existing task batch
                    if storage_tasks:
                        # if we already have storage tasks, we cap this an move on
                        dag.cap_nodes([task])
                        continue
                    else:
                        # if we don't have storage tasks, we return this task batch
                        yield task
                        dag.mark_node_completed(task)
                        return
