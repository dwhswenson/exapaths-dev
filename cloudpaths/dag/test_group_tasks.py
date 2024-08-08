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
    expected_edges = {(batches[0], batches[1])}
    return DAG(edges), expected_edges

@pytest.fixture
def blocked_repex_task_dag():
    a, b, d, f = [ExpensiveTask(ll) for ll in 'abdf']
    c, e, g = [CheapTask(ll) for ll in 'ceg']
    edges = [(a, c), (b, c), (c, d), (c, e), (e, f), (f, g)]
    expected = [['a'], ['b', 'c', 'e'], ['d'], ['f', 'g']]
    batches = [
        TaskBatch([a]), TaskBatch([b, c, e]), TaskBatch([d]),
        TaskBatch([f, g])
    ]
    expected_edges = {(batches[0], batches[1]), (batches[1], batches[2]),
                      (batches[1], batches[3])}
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
        assert rebatched == expected
