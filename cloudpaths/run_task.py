import pathlib
import tempfile
import zipfile
from contextlib import contextmanager, ExitStack

import openpathsampling as paths

from openpathsampling.experimental.storage import Storage, monkey_patch_all
paths = monkey_patch_all(paths)


@contextmanager
def SimStoreZipFile(storage_handler, storage_path, mode):
    """Context manager to transparently handle a zipped SimStore file.
    """
    storage_path = pathlib.Path(storage_path)
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
    """This is an object storage model where all objects are stored in
    indivual SimStore zip/db files except the final result storage, which is
    standard SimStore db.
    """
    def __init__(self, storage_handler):
        self.storage_handler = storage_handler

    def _get_storage(self, storage_path, mode):
        if mode not in ['r', 'w']:
            raise ValueError()

        return SimStoreZipFile(self.storage_handler,
                               storage_path,
                               mode=mode)

    def save_task(self, taskid, task):
        with self._get_storage(f"tasks/{taskid}.zip", mode='w') as storage:
            storage.tags['task'] = task

    @contextmanager
    def load_task(self, taskid):
        with self._get_storage(f"tasks/{taskid}.zip", mode='r') as storage:
            yield storage.tags['task']

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

    def save_step(self, taskid, previous):
        # TODO: there may be a smarter way to do this than actually loading
        # everything up; seems like an S3-based change and active could be
        # minimal in loading the actual contents, this saving a lot of
        # memory requirements
        with self._get_storage(f"changes/{taskid}.zip", mode='r') as ch_st:
            change = ch_st.tags['result']

        active = self.load_sample_set()
        # save the step
        # TODO: attach time to step details
        step = paths.MCStep(
            simulation=...,  # TODO
            mccycle=...,  # TODO
            previous=None,  # not used by SimStore
            active=active,
            change=change,
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            local_results = tmpdir / "results.db"
            self.storage_hander.load("results/results.db", local_results)
            storage = Storage(local_results)
            storage.save(step)
            storage.sync_all()
            self.storage_handler.save("results/results.db", local_results)

    def task_db_location(self):
        return "tasks/taskdb.db"

    def result_db_location(self):
        ...


class SerialTaskRunner:
    """
    """
    def __init__(self, objectdb, taskdb, simid):
        self.objectdb = objectdb
        self.taskdb = taskdb

    def get_task(self):
        taskid = self.taskdb.check_out_task()
        return taskid

    def complete_task(self, taskid):
        self.taskdb.mark_task_completed(taskid)

    def run_task(self, taskid):
        # this will be the worker part on a cluster
        with self.objectdb.load_task(taskid) as mover:
            inp_ens = mover.input_ensembles
            with self.objectdb.load_sample_set(inp_ens) as active:
                change = mover.move(active)
                self.objectdb.save_change(taskid, change)

    def run_loop(self):
        ntasks = 1
        while (taskid := self.get_task()) is not None:
            print(f"Running task : {taskid} (#{ntasks})")
            self.run_task(taskid)
            # remaining here are the parts that happen in lambda
            self.objectdb.update_active_samples(taskid)
            # self.objectdb.save_step(taskid)  # TODO
            # self.objectdb.complete_task(taskid)  # TODO
            self.taskdb.mark_task_completed(taskid, success=True)
            ntasks += 1


if __name__ == "__main__":
    import sys
    import pathlib
    from openpathsampling.experimental.storage import Storage, monkey_patch_all
    from openpathsampling.utils.storage_handlers import LocalFileStorageHandler
    import exorcist
    paths = monkey_patch_all(paths)
    basedir = pathlib.Path(sys.argv[1])

    handler = LocalFileStorageHandler(basedir)
    objectdb = SimStoreZipStorage(handler)
    taskdb = exorcist.TaskStatusDB.from_filename(basedir / 'taskdb.db')

    worker = SerialTaskRunner(objectdb, taskdb, simid=str(basedir))
    worker.run_loop()

