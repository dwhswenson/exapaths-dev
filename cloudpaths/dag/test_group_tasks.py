from cloudpaths.dag.group_tasks import *
from cloudpaths.dag.dag import DAG
import pytest

class Task:
    def __init__(self, name):
        self.name = name

    def __eq__(self, other):
        # cheap way of doing this; not great
        return other.__class__ == self.__class__ and other.name == self.name

    def __hash__(self):
        return hash((self.__class__, self.name))

    def __repr__(self):
        return f"{self.__class__.__name__}('{self.name}')"

class ExpensiveTask(Task):
    pass


class CheapTask(Task):
    pass


class MyTaskGrouper(TaskGrouper):
    def is_expensive(self, task):
        return isinstance(task, ExpensiveTask)


@pytest.fixture
def linear_task_dag():
    a, c, e = [CheapTask(ll) for ll in 'ace']
    b, d = [ExpensiveTask(ll) for ll in 'bd']
    edges = [(a, b), (b, c), (c, d), (d, e)]
    batches = [
        TaskBatch([a, b, c]), TaskBatch([d, e])
    ]
    expected_edges = [{(batches[0], batches[1])}]
    return DAG(edges), expected_edges

@pytest.fixture
def blocked_repex_task_dag():
    # '*' after a label indicates that this is the expensive edge
    # a* - c - d*
    # b* /   \ e - f* - g
    # allowed groupings:
    # [[a], [b, c, e], [d], [f, g]]
    # [[b], [a, c, e], [d], [f, g]]
    # I don't think there's a way to do this without having multiple options
    # possible
    a, b, d, f = [ExpensiveTask(ll) for ll in 'abdf']
    c, e, g = [CheapTask(ll) for ll in 'ceg']
    edges = [(a, c), (b, c), (c, d), (c, e), (e, f), (f, g)]
    expected_1 = [[a], [b, c, e], [d], [f, g]]
    expected_2 = [[b], [a, c, e], [d], [f, g]]

    expected_edges = []
    for expected_batches in [expected_1, expected_2]:
        batches = [TaskBatch(batch) for batch in expected_batches]
        # fortunately, the edges between batches stay the same no matter,
        # even if the content of those edges are different
        expected_edges.append({(batches[0], batches[1]),
                               (batches[1], batches[2]),
                               (batches[1], batches[3])})

    # batches = [
    #     TaskBatch([a]), TaskBatch([b, c, e]), TaskBatch([d]),
    #     TaskBatch([f, g])
    # ]
    # expected_edges = {(batches[0], batches[1]), (batches[1], batches[2]),
    #                   (batches[1], batches[3])}
    return DAG(edges), expected_edges


class TestTaskGrouper:
    @pytest.mark.parametrize('fixture', [
        'linear_task_dag',
        'blocked_repex_task_dag',
    ])
    def test_rebatch(self, request, fixture):
        dag, expected = request.getfixturevalue(fixture)
        grouper = MyTaskGrouper()
        rebatched = grouper.rebatch(dag)
        def batches_from_edges(edges):
            # I think this can be managed cleanly with some fancier python,
            # but this is easy to write
            batches = set()
            for edge in edges:
                batches.update(set(edge))
            return batches

        # check that the TaskBatch nodes are as expected
        batches = batches_from_edges(rebatched)
        expected_batches = [batches_from_edges(edges)
                            for edges in expected]
        assert batches in expected_batches

        # check that the edgs are as expected
        assert rebatched in expected
