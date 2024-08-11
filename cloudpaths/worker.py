import boto3
import os
import sys
import json
import time
import pathlib
import networkx as nx

import logging

from .aws_utils import s3_localfile
from .create_task_graph import create_task_graph
from .run_task import SimStoreZipStorage
import openpathsampling as paths
from openpathsampling.experimental.storage import Storage, monkey_patch_all
paths = monkey_patch_all(paths)

_logger = logging.getLogger(__name__)

class TerminateWithoutShutdown(Exception):
    """Convert exceptions to this to prevent workers from shutting down
    """

from .move_to_ops.storage_handlers import StorageHandler
import pathlib
import boto3
class S3StorageHandler(StorageHandler):
    """Subclass of storage handler for S3 interactions.

    StorageHandler abstracts out interactions with storage. This is the S3
    implementation of that.
    """
    def __init__(self, bucket, prefix=""):
        self.bucket = bucket
        self.prefix = pathlib.Path(prefix)
        self.s3 = boto3.client('s3')

    def _key(self, storage_label):
        return str(self.prefix / storage_label)

    def store(self, storage_label, source_path):
        self.s3.upload_file(Filename=str(source_path),
                            Bucket=self.bucket,
                            Key=self._key(storage_label))

    def load(self, storage_label, target_path):
        self.s3.download_file(Bucket=self.bucket,
                              Key=self._key(storage_label),
                              Filename=target_path)

    def delete(self, storage_label):
        self.s3.delete_object(Bucket=self.bucket,
                              Key=self._key(storage_label))

    def __contains__(self, storage_label):
        try:
            resp = self.s3.head_object(Bucket=self.bucket,
                                       Key=self._key(storage_label))
        except ClientError:
            # NOTE: this can be because of permissions problems, too
            return False
        else:
            return True

    def list_directory(self, storage_label):
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(self.bucket)
        yield from bucket.objects.filter(Prefix=self._key(storage_label))


# TODO: convert tasks to plugins; that will make this easier
class SingleTask:
    def __init__(self, message):
        self.message = message

    def claim_task(self):
        # TODO: this should do something to say to a database that this task
        # has been claimed by this instance; useful for checking if an
        # instance has died
        ...

    def run_task(self, task_id):
        raise NotImplementedError()


class LaunchTask(SingleTask):
    """A task to create a database of other tasks to accomplish.
    """
    @property
    def config(self):
        return self.message['config']

    def run_task(self, task_id):
        _logger.info("Running LaunchTask")
        _logger.info(self.message)
        bucket = self.config["bucket"]
        prefix = self.config["prefix"]
        launch_db = self.message['files']['launch_db']
        run_path = pathlib.Path(self.message['files']['working_path'])
        task_db = run_path / "tasks" / "taskdb.db"
        _logger.info(f"{bucket=}")
        _logger.info(f"{prefix=}")
        _logger.info(f"{launch_db=}")
        _logger.info(f"{run_path=}")
        _logger.info(f"{task_db=}")
        storage_handler = S3StorageHandler(bucket, run_path)
        object_db = SimStoreZipStorage(storage_handler)
        with s3_localfile(bucket, launch_db) as launch_file:
            storage = Storage(launch_file, mode='r')
            scheme = storage.schemes[0]
            metadata = storage.tags['cloudpaths_metadata']
            nsteps = metadata['nsteps']
            init_conds = storage.tags['initial_conditions']
            _logger.info("Building the task graph....")
            graph = create_task_graph(scheme, nsteps, object_db)
            task_graph = graph.to_networkx()
            _logger.info("Saving initial conditions")
            scheme = storage.schemes[0]
            init_conds = scheme.initial_conditions_from_trajectories(
                init_conds
            )
            for sample in init_conds.samples:
                object_db.save_sample(sample)

        task_to_deps = {node: list(task_graph.predecessors(node))
                        for node in task_graph.nodes}

        # TODO: we need to distinguish different task types; probably need
        # something in the task object for that
        task_message = {
            "result_type": "ADD_TASKS",
            "result_data": {
                # TODO: convert this to lower; here & process_add_tasks
                "Tasks": [
                    {
                        "TaskId": task_obj.uuid,
                        "Dependencies": task_to_deps[task_obj],
                        "TaskType": task_graph.nodes[task_obj].TYPE,
                    }
                    for task_obj in nx.topological_sort(task_graph)
                ]
            }
        }
        _logger.info("Sending to result queue:")
        _logger.info(task_message)
        return task_message


class MoverTask(SingleTask):
    """A task to perform an OPS path move.
    """
    def __init__(self, taskid):
        self.taskid = taskid

    def run_task(self, object_db):
        with object_db.load_task(self.taskid) as mover:
            inp_ens = mover.input_ensembles
            _logger.info("Loading input samples")
            with object_db.load_sample_set(inp_ens) as active:
                _logger.info("Performing move")
                change = mover.mover(active)
                _logger.info("Saving result")
                object_db.save_change(self.taskid, change)
                if change.accepted:
                    _logger.info("Accepted move! Saving updates...")
                    for sample in change.trials:
                        object_db.save_sample(sample)
                else:
                    _logger.info("Rejected move.")

        result = {
            "result_type": "SUCCESS_RESULT",
            "result_data": {}
        }
        return result


class StorageTask(SingleTask):
    def __init__(self, taskid):
        self.taskid = taskid

    def run_task(self, object_db ):
        ...

class TestLaunchTask(LaunchTask):
    def claim_task(self):
        return None

    def run_task(self, task_id):
        print("Would have created tasks for test_launch.db")


class TestCycleLaunchTask(LaunchTask):
    def claim_task(self):
        return "launch-task-foo"

    def run_task(self, task_id):
        print(f"Creating a task with id '{task_id}' and no dependencies")
        return {
            "ResultType": "ADD_TASKS",
            "ResultData": {
                "Tasks": [
                    {
                        "TaskId": task_id,
                        "Dependencies": [],
                        "TaskType": "TEST_TASK_SUCCESS",
                    },
                ]
            }
        }


class TestCycleWithDependencies(LaunchTask):
    ...


class TestCycleWithMultipleUnblocked(LaunchTask):
    ...


class _TestTask(SingleTask):
    # make sure this includes a sleep parameter
    ...

class TestSuccessTask(_TestTask):
    # add success/fail information here
    ...

class TestFailureTask(_TestTask):
    ...


# TODO: convert this into a plugin registry
TASK_TYPE_DISPATCH = {
    "LAUNCH": LaunchTask,
    "TEST_LAUNCH": TestLaunchTask,
    "TEST_LAUNCH_CYCLE": TestCycleLaunchTask,
    "TEST_LAUNCH_DEPS": ...,
    "TEST_LAUNCH_MULTIUNBLOCK": ...,
    "TEST_TASK_SUCCESS": ...,
    "TEST_TASK_FAILURE": ...,
    "MoverTask": MoverTask,
    "StorageTask": ...,
    "default": ...,
}


def run_single_task(message):
    """
    Process a single task from the SQS queue.

    This dispatches the task message to the correct runner, and returns the
    task results to the result queue. This is primarily focused on SQS
    integration in the Exapaths workflow.
    """
    _logger.info(f"Running task from message ID {message['MessageId']}")
    msg = json.loads(message['Body'])
    _logger.info(msg)
    task_type = TASK_TYPE_DISPATCH[msg['task_type']]
    # cluster = msg['cluster']
    # cluster_conf = msg['Config']['clusters'][cluster]

    taskq_url = os.environ.get("CLOUDPATHS_TASK_QUEUE")
    # assert taskq_url == cluster_conf['task_queue']['url']

    resultq_url = msg['config']['result_queue']['url']

    task = task_type(msg)

    _logger.info("Claiming a task")
    # _logger.debug(f"{msg=}\n{cluster_conf=}")
    task_id = task.claim_task()
    sqs = boto3.client('sqs')
    sqs.delete_message(
        QueueUrl=taskq_url,
        ReceiptHandle=message['ReceiptHandle'],
    )

    _logger.info(f"Running task '{task_id}'")
    # TODO: add a try/except here; failure result if
    task_result = task.run_task(task_id)

    _logger.info("Passing results to the result queue")
    if task_result is not None:
        result_msg = {
            'inputs': {
                k: v for k, v in msg.items()
                if k not in ['metadata', 'files', 'config', 'results']
            },
            'metadata': msg['metadata'],
            'files': msg['files'],
            'config': msg['config'],
            'results': task_result,
        }
        bucket = msg['config']['bucket']
        prefix = msg['config']['prefix']
        result_db = msg['files']['result_db']
        resp = sqs.send_message(
            QueueUrl=resultq_url,
            MessageBody=json.dumps(result_msg),
            MessageGroupId=f"{bucket}::{prefix}::{result_db}",
        )


def worker_main_loop(terminate_on_exit=True):
    taskq_url = os.environ.get("CLOUDPATHS_TASK_QUEUE")
    max_attempts = int(os.environ.get("CLOUDPATHS_ATTEMPTS"))
    sleep_time = float(os.environ.get("CLOUDPATHS_WAIT"))

    if not (taskq_url and max_attempts and sleep_time):
        ... # TODO: raise error and exit
        # TODO: maybe set defaults for max_attempts/sleep_time?

    load_attempts = 1
    sqs = boto3.client('sqs')
    while load_attempts <= max_attempts:
        _logger.info(f"Looking for a task ({load_attempts}/{max_attempts})")
        resp = sqs.receive_message(
            QueueUrl=taskq_url,
            MaxNumberOfMessages=1,
            VisibilityTimeout=60,
            WaitTimeSeconds=0,
        )
        messages = resp.get("Messages")

        if not messages:
            load_attempts += 1
            if load_attempts < max_attempts:
                time.sleep(sleep_time)
            continue

        if len(messages) > 1:
            # log warning, move on (process first message; others should
            # return to queue) # TODO
            ...

        run_single_task(messages[0])
        load_attempts = 1
        # uncomment the next line for on-instance debugging
        # raise TerminateWithoutShutdown()

    _logger.info("Exiting main worker loop")


def start(self):
    # as used on a remote, with terminate_on_exit=True
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    worker_main_loop(terminate_on_exit=True)


if __name__ == "__main__":
    # as used in debug testing
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--terminate", default=True,
                        action=argparse.BooleanOptionalAction)
    opts = parser.parse_args()

    # TODO: figure out who is setting basicConfig on import (force=True
    # fixes)
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, force=True)
    back_to_warning = ['openpathsampling.experimental',
                       'openpathsampling.initialization']
    for logger_name in back_to_warning:
        lgr = logging.getLogger(logger_name)
        lgr.setLevel(logging.WARNING)

    terminate = opts.terminate
    try:
        worker_main_loop()
    except TerminateWithoutShutdown:
        terminate = False
    finally:
        if terminate:
            instance_id = os.environ.get("AWS_INSTANCE_ID")
            _logger.info(f"Terminating this instance ({instance_id})")
            autoscaling = boto3.client('autoscaling')
            autoscaling.terminate_instance_in_auto_scaling_group(
                InstanceId=instance_id,
                ShouldDecrementDesiredCapacity=True,
            )
        else:
            _logger.info("Not terminating this instance")

