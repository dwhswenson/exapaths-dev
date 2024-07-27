import pytest

import openpathsampling as paths

import collections

from .preplanned import *
from openpathsampling.tests.fixtures.two_state_toy import *


def test_preselect_movers(two_state_tis):
    scheme, network, engine, init_conds = two_state_tis
    assert len(scheme.choice_probability) == 9
    n_steps = 10_000
    moves = preselect_movers(scheme, n_steps)
    move_count = collections.Counter(moves)
    for mover, count in move_count.items():
        expected = scheme.choice_probability[mover]
        # NOTE: there's a chance that statistical noise wil cause false
        # errors here; may need to adjust over time
        assert expected - 0.1 < count / n_steps < expected + 0.1

def test_mover_list_to_graph(two_state_tis):
    scheme, network, engine, init_conds = two_state_tis
    shoot_A, shoot_B, shoot_C = scheme.movers['shooting']
    repex_AB, repex_BC = scheme.movers['repex']
    pathrev_A, pathrev_B, pathrev_C = scheme.movers['pathreversal']
    ens_A, ens_B, ens_C = network.sampling_ensembles
    ens_minus = network.minus_ensembles[0]
    mover_list = [
        shoot_A,
        repex_AB,
        shoot_C,
        pathrev_A,
        pathrev_B,
        pathrev_C,
        repex_BC
    ]
    # GRAPH:
    # s_A         rev_A
    #      x_AB   rev_B  x_BC
    # s_C  rev_C
    # NB: no move used more than once, so the set of edges works to test
    # that we got all the edges we expected
    graph_edges = mover_list_to_graph(scheme, mover_list)
    expected_edges = {
        (f"initial {ens_A.name}", shoot_A),
        (f"initial {ens_B.name}", repex_AB),
        (f"initial {ens_C.name}", shoot_C),
        (shoot_A, repex_AB),
        (shoot_C, pathrev_C),
        (repex_AB, pathrev_A),
        (repex_AB, pathrev_B),
        (pathrev_B, repex_BC),
        (pathrev_C, repex_BC),
        (pathrev_A, f"final {ens_A.name}"),
        (repex_BC, f"final {ens_B.name}"),
        (repex_BC, f"final {ens_C.name}"),
        (f"initial {ens_minus.name}", f"final {ens_minus.name}"),
    }
    mover_edges = set(
        tuple(node.mover if isinstance(node, MoverNode) else node
              for node in edge)
        for edge in graph_edges
    )
    assert mover_edges == expected_edges

def test_preplan_pathsampling(two_state_tis):
    # mostly a smoke test; a little here to check against details of number
    # of edges/nodes
    scheme, network, engine, init_conds = two_state_tis
    nsteps = 100
    edges = preplan_pathsampling(scheme, nsteps)

    def get_move_type(scheme, node):
        if not isinstance(node, MoverNode):
            return None
        mover = node.mover
        mover_types = ['repex', 'pathreversal', 'shooting', 'minus']
        for typ in mover_types:
            if mover in scheme.movers[typ]:
                return typ
        raise RuntimeError(f"Unable to define mover type for {mover}")

    nodes = set.union(*[set(e) for e in edges])
    move_types = collections.Counter(get_move_type(scheme, node)
                                     for node in nodes)
    move_count = sum(move_types[m] for m in move_types if m is not None)
    assert move_count == nsteps
    # we count the output edges
    input_ensembles = move_types[None] / 2  # show up as input and output
    expected_edge_count = (
        input_ensembles
        + 1 * move_types['shooting']
        + 2 * move_types['repex']
        + 1 * move_types['pathreversal']
        + 2 * move_types['minus']
    )
    assert len(edges) == expected_edge_count
