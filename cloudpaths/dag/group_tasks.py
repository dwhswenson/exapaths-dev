import random
import collections
from collections import abc

from cloudpaths.dag.dag import ExecutingDAG

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

class TaskBatch(abc.Sequence):
    def __init__(self, batch):
        self._list_batch = list(batch)
        self._set_batch = set(batch)

    def __contains__(self, item):
        return item in self._set_batch

    def __len__(self):
        return len(self._list_batch)

    def __getitem__(self, item):
        return self._list_batch[item]

    def __repr__(self):
        return f"{self.__class__.__name__}({self._list_batch})"

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self._list_batch == other._list_batch
        return NotImplemented

    def __hash__(self):
        return hash(frozenset(self._set_batch))


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
        # import pdb; pdb.set_trace()
        node_to_batch = {}
        while list_batch := list(self._execution_cycle(exec_dag)):
            batch = TaskBatch(list_batch)
            node_to_batch.update({node: batch for node in batch})
            for node in batch:
                external = {inp for inp in node_to_input[node]
                            if inp not in batch}
                edges = {(node_to_batch[ext], batch) for ext in external}
                rebatch_edges |= edges

        return rebatch_edges


class OPSTaskGrouper(TaskGrouper):
    def is_expensive(self, task):
        # tasks here are instance methods from an OPS pathmover; this needs
        # to be made into a thing on the OPS mover object itself
        cheap_movers = [
            'ReplicaExchangeMover',
            'PathReversalMover',
        ]
        return task.__self__.__class__.__name not in cheap_movers
