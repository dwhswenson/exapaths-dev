import uuid
import numpy as np
import inspect

from exapaths.dag.dag import DAG
from exapaths.move_to_ops.tasknodes import MoverNode, StorageTaskNode


def preselect_movers(scheme, nsteps):
    """Select a number of movers from a MoveScheme without running any.

    This essentially preplans a path sampling simulation.
    """
    scheme.move_decision_tree()  # ensure that the tree exists
    movers, probs = zip(*scheme.choice_probability.items())
    mover_nums = np.random.choice(range(len(movers)), nsteps, p=probs)
    return [movers[num] for num in mover_nums]


def mover_list_to_graph(scheme, mover_list, start_from=1, simulation=None):
    """Convert a list of movers to edge pairs for a DAG

    This tracks the dependency of results for different ensembles across for
    the list of movers.
    """
    edges = []
    last_mover = {e: f"initial {e.name}" for e in scheme.network.all_ensembles}
    for num, mover in enumerate(mover_list):
        input_ensembles, output_ensembles = mover.ensemble_signature_set
        node = MoverNode(mover, number=num + start_from,
                         simulation=simulation)
        for inp_ens in input_ensembles:
            prev = last_mover[inp_ens]
            last_mover[inp_ens] = None
            edges.append((prev, node))

        for out_ens in output_ensembles:
            last_mover[out_ens] = node

    for ens, mover in last_mover.items():
        edges.append((mover, f"final {ens.name}"))

    return edges


def edges_to_dag(edges):
    # TODO: this should be the output of the preplan_pathsampling; not just the edges
    def is_node(node):
        return not isinstance(node, str)

    nodes = set.union(*[
        set(n for n in edge if is_node(n))
        for edge in edges
    ])
    true_edges = [(n1, n2) for n1, n2 in edges if is_node(n1) and is_node(n2)]
    return DAG(true_edges, nodes=nodes)


def itertools_batched(iterable, n):
    from itertools import islice
    # available in itertools starting in Python 3.12; this is used in
    # add_storage_every_n
    # batched('ABCDEFG', 3) → ABC DEF G
    if n < 1:
        raise ValueError('n must be at least one')
    iterator = iter(iterable)
    while batch := tuple(islice(iterator, n)):
        yield batch


def add_storage_every_n(batched_dag, nsteps=1):
    num_to_taskbatch = {
        movernode.number: taskbatch
        for taskbatch in batched_dag.nodes
        for movernode in taskbatch
    }
    num_to_taskid = {
        movernode.number: movernode.uuid.hex
        for taskbatch in batched_dag.nodes
        for movernode in taskbatch
    }
    min_val = min(num_to_taskbatch)
    max_val = max(num_to_taskbatch)
    storage_batchnums = itertools_batched(range(min_val, max_val+1), nsteps)
    edges = set()
    storage_nodes = set()
    prev_storage_node = None
    for storage_batch in storage_batchnums:
        from_batches = set(num_to_taskbatch[num] for num in storage_batch)
        mover_uuids = {num_to_taskid[num]: num for num in storage_batch}
        storage_node = StorageTaskNode(step_number=max(storage_batch),
                                       mover_tasks=mover_uuids)
        local_edges = set((batch, storage_node) for batch in from_batches)
        if prev_storage_node:
            local_edges.add((prev_storage_node, storage_node))
        edges |= local_edges
        storage_nodes.add(storage_node)
        prev_storage_node = storage_node

    dag = DAG(batched_dag.edges | edges, nodes=batched_dag.nodes | storage_nodes)
    return dag


def preplan_pathsampling(scheme, nsteps, start_from=1, simulation=None):
    """
    """
    mover_list = preselect_movers(scheme, nsteps)
    return mover_list_to_graph(scheme, mover_list, start_from, simulation)
