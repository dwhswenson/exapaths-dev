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


class MoverNode:
    """Node in the graph of movers.

    This essentially just adds a UUID for each node to distinguish multiple
    uses of the same mover. It also provides better string representation.
    """
    def __init__(self, mover, number):
        self.uuid = uuid.uuid4()
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


def preplan_pathsampling(scheme, nsteps, start_from=1):
    """
    """
    mover_list = preselect_movers(scheme, nsteps)
    return mover_list_to_graph(scheme, mover_list, start_from)
