import pytest

from exapaths.dag.dag import *

@pytest.fixture
def capped_exec_dag():
    # CAP - a
    #       b  \  c
    # should execute b and nothing else
    edges = [(ExecutingDAG.CAP, 'a'), ('a', 'c'), ('b', 'c')]
    dag = DAG(edges)
    return ExecutingDAG(dag)

@pytest.fixture
def diamond_dag():
    #   / b \
    # a      d
    #   \ c /
    edges = [('a', 'b'), ('a', 'c'), ('b', 'd'), ('c', 'd')]
    dag = DAG(edges)
    return dag

@pytest.fixture
def diamond_exec_dag(diamond_dag):
    return ExecutingDAG(diamond_dag)

class TestExecutingDAG:
    @pytest.mark.parametrize('fixture, exp_counter, exp_count_to_node', [
        (
            'capped_exec_dag',
            {'a': 1, 'b': 0, 'c': 2},
            {0: {'b'}, 1: {'a'}, 2: {'c'}},
        ),
        (
            'diamond_exec_dag',
            {'a': 0, 'b': 1, 'c': 1, 'd': 2},
            {0: {'a'}, 1: {'b', 'c'}, 2: {'d'}},
        ),
    ])
    def test_calculate_count_to_node(self, request, fixture, exp_counter,
                                     exp_count_to_node):
        dag = request.getfixturevalue(fixture)
        counter, count_to_node = dag._calculate_counts()
        assert dict(counter) == exp_counter
        assert dict(count_to_node) == exp_count_to_node

    @pytest.mark.parametrize('fixture, expected', [
        ('capped_exec_dag', set('b')),
        ('diamond_exec_dag', set('a')),
    ])
    def test_available_nodes(self, request, fixture, expected):
        dag = request.getfixturevalue(fixture)
        assert dag.available_nodes == expected

    def test_iteration_capped(self, capped_exec_dag):
        nodes = list(capped_exec_dag)
        assert nodes == ['b']

    def test_iteration_diamond(self, diamond_exec_dag):
        nodes = list(diamond_exec_dag)
        assert nodes[0] == 'a'
        assert nodes[1:3] in [['b', 'c'], ['c', 'b']]
        assert nodes[3] == 'd'

    def test_cap_nodes(self):
        ...

    def test_uncap_nodes(self):
        ...

    def test_available_nodes(self):
        ...
