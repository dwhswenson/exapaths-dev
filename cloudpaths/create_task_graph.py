"""
Run this as a lambda when we need to create a new task graph
"""
import openpathsampling as paths
from cloudpaths.move_to_ops.preplanned import (
    MoverNode, preplan_pathsampling
)
import networkx as nx


def create_task_graph(scheme, nsteps, objectdb):
    """
    Save individual tasks to task storage; return task graph

    This creates the task graph, saves a serialized representation of the
    tasks to storage in a way that it can be reloaded later, and return a
    networkx graph suitable for loading in Exorcist.
    """
    # plan the graph using preplan_pathsampling
    edges = preplan_pathsampling(scheme, nsteps)

    # preplan_pathsampling provides all information as edges between tasks
    # (movers), which means that the first tasks and final tasks are
    # connected to "fake" placeholder nodes, represented by strings. This
    # next block gets all the actual (non-string) nodes, and all the actual
    # edges between them.
    _all_nodes = set.union(*[{n1, n2} for n1, n2 in edges])
    nodes = {node for node in _all_nodes if not isinstance(node, str)}
    mover_edges = [(str(n1.uuid), str(n2.uuid)) for n1, n2 in edges
                   if not isinstance(n1, str) and not isinstance(n2, str)]

    # create a networkx graph suitable for exorcist (string task names as
    # nodes, we use MoverNode.uuid for task name) -- we attach the node so
    # that exapaths can use this to identify type of task to direct to
    # different queues
    graph = nx.DiGraph()
    for node in nodes:
        graph.add_node(str(node.uuid), obj=node)
    graph.add_edges_from(mover_edges)

    # serialize all tasks to disk
    for node in nodes:
        objectdb.save_task(str(node.uuid), node.mover)

    return graph

# TODO: version where we the task graph includes additional components

if __name__ == "__main__":
    import sys
    import pathlib
    opsfile = sys.argv[1]
    nsteps = int(sys.argv[2])
    from openpathsampling.experimental.storage import Storage, monkey_patch_all
    from cloudpaths.run_task import SimStoreZipStorage
    from cloudpaths.move_to_ops.storage_handlers import LocalFileStorageHandler
    import exorcist
    paths = monkey_patch_all(paths)
    st = Storage(opsfile, mode='r')
    scheme = st.schemes[0]

    root_dir = pathlib.Path("task_graph")

    objectdb = SimStoreZipStorage(LocalFileStorageHandler(root_dir))
    taskdb = exorcist.TaskStatusDB.from_filename(root_dir / "taskdb.db")
    task_graph = create_task_graph(scheme, nsteps, objectdb)

    # save task graph to exorcist database
    taskdb.add_task_network(task_graph, max_tries=3)

    # TODO: add initial conditions
    try:
        init_conds = st.tags['initial_conditions']
    except KeyError:
        init_conds = st.tags['initial_trajectory']

    init_conds = scheme.initial_conditions_from_trajectories(init_conds)
    for sample in init_conds:
        objectdb.save_sample(sample)
