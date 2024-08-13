import uuid
import numpy as np


def preselect_movers(scheme, nsteps):
    """Select a number of movers from a MoveScheme without running any.

    This essentially preplans a path sampling simulation.
    """
    scheme.move_decision_tree()  # ensure that the tree exists
    movers, probs = zip(*scheme.choice_probability.items())
    mover_nums = np.random.choice(range(len(movers)), nsteps, p=probs)
    return [movers[num] for num in mover_nums]


class TaskNode:
    """Generic node representing a task.

    Subclasses should specify TYPE.
    """
    TYPE = None
    def __init__(self):
        self.uuid = uuid.uuid4()


class StorageTaskNode(TaskNode):
    """Task node for storage tasks.

    This needs to know which mover task IDs it is associated with.
    """
    TYPE = "StorageTask"
    def __init__(self, step_number, mover_tasks):
        super().__init__()
        self.mover_tasks = mover_tasks
        self.step_number = step_number

    def __repr__(self):
        # TODO: maybe shorten the mover tasks representation?
        return (f"{self.__class__.__name__}({self.step_number}, "
                f"mover_tasks={self.mover_tasks})")


class MoverNode(TaskNode):
    """Node in the graph of movers.

    This essentially just adds a UUID for each node to distinguish multiple
    uses of the same mover. It also provides better string representation.
    """
    TYPE = "MoverTask"
    def __init__(self, mover, number):
        super().__init__()
        self.number = number
        self.mover = mover

    def __repr__(self):
        ens_names = [f"'{e.name}'" for e in self.mover.input_ensembles]
        moverstr = f"{self.mover.__class__.__name__}({','.join(ens_names)})"
        return f"MoverNode({moverstr}, '{str(self.uuid)[:8]}')"


def mover_list_to_graph(scheme, mover_list, start_from=1):
    """Convert a list of movers to edge pairs for a DAG

    This tracks the dependency of results for different ensembles across for
    the list of movers.
    """
    edges = []
    last_mover = {e: f"initial {e.name}" for e in scheme.network.all_ensembles}
    for num, mover in enumerate(mover_list):
        input_ensembles, output_ensembles = mover.ensemble_signature_set
        node = MoverNode(mover, number=num + start_from)
        for inp_ens in input_ensembles:
            prev = last_mover[inp_ens]
            last_mover[inp_ens] = None
            edges.append((prev, node))

        for out_ens in output_ensembles:
            last_mover[out_ens] = node

    for ens, mover in last_mover.items():
        edges.append((mover, f"final {ens.name}"))

    return edges


def add_storage(mover_graph, store_every=1):
    """Add storage tasks to an existing mover graph.
    """
    number_to_node = {node.number: node for node in mover_graph.nodes}
    min_num = min(number_to_node)
    max_num = max(number_to_node)
    # TODO: figure out efficient algorithm for this; I think the trick will
    # just be to make new edges to the tasks that we need
    new_edges = []
    _mover_tasks = []
    prev_storage_task = None
    for num in range(min_num, max_num+1):
        _mover_tasks.append(number_to_node[num])
        if num % store_every == 0:
            storage_task = StorageTask(
                step_number=num,
                mover_tasks=[str(task.uuid) for task in _mover_tasks]
            )
            new_edges.extend([
                (task, storage_task) for task in _mover_tasks
            ])
            if prev_storage_task is not None:
                new_edges.append((prev_storage_task, storage_task))

            prev_storage_task = storage_task
            _mover_tasks = []
    # TODO: add new_edges to mover_graph


def preplan_pathsampling(scheme, nsteps, start_from=1):
    """
    """
    mover_list = preselect_movers(scheme, nsteps)
    return mover_list_to_graph(scheme, mover_list, start_from)
