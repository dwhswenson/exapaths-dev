from collections import abc
import uuid
import importlib

class TaskNode:
    """Generic node representing a task.

    Subclasses should specify TYPE.
    """
    TYPE = None
    def __init__(self):
        self.uuid = uuid.uuid4()

    @property
    def taskid(self):
        return self.uuid.hex

    def __eq__(self, other):
        return (
            self.__class__ == other.__class__
            and self.to_dict() == other.to_dict()
        )

    def __hash__(self):
        return hash(self.uuid)

    def to_dict(self):
        dct = self._to_dict()
        dct.update({
            'uuid': self.uuid.hex,
            ':module:': self.__class__.__module__,
            ':qualname:': self.__class__.__qualname__,
        })
        return dct

    def _set_uuid(self, str_uuid):
        self.uuid = uuid.UUID(str_uuid)

    def _to_dict(self):
        return {}

    @classmethod
    def _from_dict(cls, dct):
        return cls(**dct)

    @classmethod
    def from_dict(cls, dct):
        uuid = dct.pop('uuid')
        modname = dct.pop(":module:")
        qualname = dct.pop(":qualname:")
        module = importlib.import_module(modname)
        splitted = qualname.split('.')
        myloader = module
        for step in splitted:
            myloader = getattr(myloader, step)

        obj = myloader._from_dict(dct)
        obj._set_uuid(uuid)
        return obj


class StorageTaskNode(TaskNode):
    """Task node for storage tasks.

    This needs to know which mover task IDs it is associated with.
    """
    TYPE = "StorageTask"
    def __init__(self, step_number, mover_tasks):
        super().__init__()
        self.mover_tasks = mover_tasks
        self.step_number = step_number

    def _to_dict(self):
        return {
            'mover_tasks': self.mover_tasks,
            'step_number': self.step_number,
        }

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

    def _to_dict(self):
        return {
            'mover': self.mover,
            'number': self.number,
        }

    def __repr__(self):
        ens_names = [f"'{e.name}'" for e in self.mover.input_ensembles]
        moverstr = f"{self.mover.__class__.__name__}({','.join(ens_names)})"
        return (
            f"MoverNode({self.number}, "
            f"{moverstr}, "
            f"'{str(self.uuid)[:8]}')"
        )


class TaskBatch(TaskNode, abc.Sequence):
    TYPE = "TaskBatch"
    def __init__(self, batch: list):
        super().__init__()
        self._list_batch = list(batch)
        self._set_batch = set(batch)

    def _to_dict(self):
        return {
            'batch': [node.to_dict() for node in self._list_batch],
        }

    @classmethod
    def _from_dict(cls, dct):
        batches = [cls.from_dict(bdct) for bdct in dct['batch']]
        dct['batch'] = batches
        return cls(**dct)

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



