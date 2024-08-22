import pathlib
import tempfile
import zipfile
from contextlib import contextmanager, ExitStack

import openpathsampling as paths

from openpathsampling.experimental.storage import Storage, monkey_patch_all
paths = monkey_patch_all(paths)

from exapaths.move_to_ops.tasknodes import TaskNode

import logging
_logger = logging.getLogger(__name__)


@contextmanager
def SimStoreZipFile(storage_handler, storage_path, mode):
    """Context manager to transparently handle a zipped SimStore file.

    This allows us to yield out the OPS (SimStore) Storage object,
    regardless of how/where teh data is actually stored (filesystem, S3,
    etc.)

    Parameters
    ----------
    storage_handler : StorageHandler
        Base storage location for the zipped file
    storage_path : str
        Key for where the zipped file is stored within the storage handler
        (e.g., path to the file)
    mode : 'r' or 'w'
        Whether to load the file for reading only ('r') or read/write/append
        ('w').

    Yields
    ------
    storage : Storage
        The OPS/SimStore storage object at this path
    """
    storage_path = pathlib.Path(storage_path)
    _logger.info(f"Loading storage object '{str(storage_path)}'")
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = pathlib.Path(tmpdir)
        local_zip = tmpdir / storage_path.name
        local_db = local_zip.with_suffix(".db")
        if mode == 'r':
            storage_handler.load(storage_path, local_zip)
            with zipfile.ZipFile(local_zip, mode='r') as zipf:
                db = zipf.extract(local_db.name, local_db)
                storage = Storage(db, mode='r')
                yield storage
                storage.close()
                # count on the cleanup of tmpdir in cleaning
        elif mode == 'w':
            storage = Storage(local_db, mode='w')
            yield storage
            storage.close()
            with zipfile.ZipFile(local_zip, mode='x') as zipf:
                zipf.write(local_db, arcname=local_db.name)
            storage_handler.store(storage_path, local_zip)
            # trust cleanup of tmpdir to do the rest


class SimStoreZipStorage:
    """OPS object storage using individual SimStore zip/db files.

    Each task, each active sample and each change coming from a completed
    move is saved as a zipped SimStore database. The task can be determined
    before running the simulation; the active samples are the context at the
    moment the task is run, and the change object is the output of a task.

    The changes can be collected into standard SimStore db as the final
    result storage.
    """
    def __init__(self, storage_handler):
        self.storage_handler = storage_handler

    def _get_storage(self, storage_path, mode):
        if mode not in ['r', 'w']:
            raise ValueError()

        return SimStoreZipFile(self.storage_handler,
                               storage_path,
                               mode=mode)

    def save_task(self, task):
        taskid = task.uuid.hex
        with self._get_storage(f"tasks/{taskid}.zip", mode='w') as storage:
            storage.tags['task'] = task.to_dict()

    @contextmanager
    def load_task(self, taskid):
        with self._get_storage(f"tasks/{taskid}.zip", mode='r') as storage:
            serialized = storage.tags['task']
            yield TaskNode.from_dict(serialized)

    @contextmanager
    def load_sample_set(self, ensembles=None):
        if ensembles is None:
            # TODO: all ensembles in this case
            ...
        with ExitStack() as stack:
            samples = []
            for ensemble in ensembles:
                path = f"current/{ensemble.__uuid__}.zip"
                st = stack.enter_context(self._get_storage(path, mode='r'))
                sample = st.tags['sample']
                samples.append(sample)

            yield paths.SampleSet(samples)

    def save_change(self, taskid, change):
        with self._get_storage(f"changes/{taskid}.zip", mode='w') as storage:
            storage.tags['result'] = change

    def save_sample(self, sample):
        ensemble = sample.ensemble
        path = f"current/{ensemble.__uuid__}.zip"
        with self._get_storage(path, mode='w') as storage:
            storage.tags['sample'] = sample

    def update_active_samples(self, taskid):
        with self._get_storage(f"changes/{taskid}.zip", mode='r') as ch_st:
            change = ch_st.tags['result']
            if change.accepted:
                for sample in change.trials:
                    self.save_sample(sample)

    @contextmanager
    def get_permanent_storage(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = pathlib.Path(tmpdir)
            local_results = tmpdir / "results.db"
            results_db = "results/results.db"
            self.storage_handler.load(results_db, local_results)
            storage = Storage(local_results)
            try:
                yield storage
            finally:
                storage.sync_all()
                self.storage_handler.save(results_db, local_results)


    def save_step(self, taskid, mccycle):
        # this assumes that we have exclusive access to the storage during
        # this function
        with self._get_storage(f"changes/{taskid}.zip", mode='r') as ch_st:
            change = ch_st.tags['result']
            with self.get_permanent_storage() as storage:
                last_step = storage.steps[-1]
                last_active = last_step.active
                assert last_step.mccycle == mccycle - 1
                step = paths.MCStep(
                    simulation="fill_this_im",
                    mccycle=mccycle,
                    previous=None,  # not used by SimStore
                    active=active,
                    change=change,
                )
                storage.save(step)

    def task_db_location(self):
        return "tasks/taskdb.db"

def run_mover_task(task, objectdb):
    print(f"Running task: {task}")
    mover = task.mover
    inp_ens = mover.input_ensembles
    with objectdb.load_sample_set(inp_ens) as active:
        change = mover.move(active)
        objectdb.save_change(task.taskid, change)
    objectdb.update_active_samples(task.taskid)

def run_task_batch(task, objectdb):
    for subtask in task:
        task_type = subtask.TYPE
        runner = TASK_DISPATCH[task_type]
        runner(subtask, objectdb)

def run_storage_task(task, objectdb):
    print(f"Running task: {task}")
    return
    raise RuntimeError()

TASK_DISPATCH = {
    "TaskBatch": run_task_batch,
    "StorageTask": run_storage_task,
    "MoverTask": run_mover_task,
}


class ExorcistLocalWorker:
    """Run using exorcist taskdb and tasks details stored in objectdb.
    """
    def __init__(self, objectdb, taskdb, simid):
        self.objectdb = objectdb
        self.taskdb = taskdb

    def get_task(self):
        taskid = self.taskdb.check_out_task()
        return taskid

    def complete_task(self, taskid):
        self.taskdb.mark_task_completed(taskid)

    def run_task(self, taskid, ntasks=None):
        task_type = self.taskdb.get_task_type(taskid)
        print(f"Running task : {taskid} ({task_type}: #{ntasks})")
        runner = TASK_DISPATCH[task_type]
        with self.objectdb.load_task(taskid) as task:
            runner(task, self.objectdb)

        self.taskdb.mark_task_completed(taskid, success=True)

    def run_loop(self):
        ntasks = 1
        while (taskid := self.get_task()) is not None:
            self.run_task(taskid, ntasks)
            ntasks += 1


if __name__ == "__main__":
    import sys
    import pathlib
    from openpathsampling.experimental.storage import Storage, monkey_patch_all
    from exapaths.move_to_ops.storage_handlers import LocalFileStorageHandler
    from .taskdb import TaskStatusDB
    paths = monkey_patch_all(paths)
    basedir = pathlib.Path(sys.argv[1])

    handler = LocalFileStorageHandler(basedir)
    objectdb = SimStoreZipStorage(handler)
    taskdb = TaskStatusDB.from_filename(basedir / 'taskdb.db')

    worker = ExorcistLocalWorker(objectdb, taskdb, simid=str(basedir))
    worker.run_loop()

