import boto3
import os
import json
import time
import pathlib

import logging

from .aws_utils import s3_localfile

_logger = logging.getLogger(__name__)


# TODO: convert tasks to plugins; that will make this easier
class SingleTask:
    def __init__(self, context):
        self.context = context

    def claim_task(self):
        ...

    def run_task(self, task_id):
        raise NotImplementedError()



class LaunchTask(SingleTask):
    """A task to create a database of other tasks to accomplish.
    """
    @property
    def config(self):
        return self.message['Config']

    def run_task(self, object_db):
        print(self.message)
        bucket = self.config["bucket"]
        prefix = self.config["prefix"]
        launch_db = self.message['Details']['launch_db']
        run_path = pathlib.Path(self.message['Details']['run_path'])
        task_db = tasks_dir / "taskdb.db"
        print(f"{bucket=}")
        print(f"{prefix=}")
        print(f"{launch_db=}")
        print(f"{run_path=}")
        print(f"{task_db=}")
        storage_handler = S3StorageHandler(bucket, run_path)
        with s3_localfile(bucket, launch_db) as launch_file:
            storage = Storage(launch_file, mode='r')
            scheme = storage.schemes[0]
            nsteps = storage.tags['nsteps']
            init_conds = storage.tags['initial_conditions']
            task_graph = create_task_graph(scheme, nsteps, object_db)

        # this is ridiculous; should be something in nx for it
        task_to_deps = {node: list(task_graph.predecessors(node))
                        for node in task_graph.nodes}

        task_message = {
            "ResultType": "ADD_TASKS",
            "ResultData": {
                "Tasks": [
                    {
                        "TaskId": task_id,
                        "Dependencies": task_to_deps[task_id],
                        "TaskType": "STORED_TASK",
                    }
                    for task_id in nx.topological_sort(task_graph)
                ]
            }
        }
        print(task_message)
        return task_message


class PathMoveTask(SingleTask):
    """A task to perform an OPS path move.
    """
    def __init__(self, taskid):
        self.taskid = taskid

    def run_task(self, object_db):
        with object_db.load_task(self.taskid) as mover:
            inp_ens = mover = input_ensembles
            with object_db.load_sample_set(inp_ens) as active:
                change = mover.mover(active)
                object_db.save_change(self.taskid, change)

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


TASK_TYPE_DISPATCH = {
    "LAUNCH": LaunchTask,
    "TEST_LAUNCH": TestLaunchTask,
    "TEST_LAUNCH_CYCLE": TestCycleLaunchTask,
    "TEST_LAUNCH_DEPS": ...,
    "TEST_LAUNCH_MULTIUNBLOCK": ...,
    "TEST_TASK_SUCCESS": ...,
    "TEST_TASK_FAILURE": ...,
    "default": ...,
}


def run_single_task(message):
    _logger.info(f"Running task from message ID {message['MessageId']}")
    msg = json.loads(message['Body'])
    task_type = TASK_TYPE_DISPATCH[msg['TaskType']]
    cluster = msg['Cluster']
    cluster_conf = msg['Config']['clusters'][cluster]

    taskq_url = os.environ.get("CLOUDPATHS_TASK_QUEUE")
    assert taskq_url == cluster_conf['task_queue']['url']

    resultq_url = msg['Config']['result_queue']['url']

    task = task_type(msg)

    _logger.info("Claiming a task")
    _logger.debug(f"{msg=}\n{cluster_conf=}")
    task_id = task.claim_task()
    sqs = boto3.client('sqs')
    sqs.delete_message(
        QueueUrl=taskq_url,
        ReceiptHandle=message['ReceiptHandle'],
    )

    _logger.info(f"Running task '{task_id}'")
    task_result = task.run_task(task_id)

    _logger.info("Passing results to the result queue")
    if task_result is not None:
        result_msg = {
            'inputs': msg,
            'results': task_result,
        }
        bucket = result_msg['inputs']['Config']['bucket']
        prefix = result_msg['inputs']['Config']['prefix']
        result_db = result_msg['inputs']['Details']['result_db']
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

    _logger.info("Exiting main worker loop")
    if terminate_on_exit:
        ...  # TODO: shut self down when we exit loop
    else:
        _logger.info("Not terminating this instance")


def start(self):
    # as used on a remote, with terminate_on_exit=True
    logging.basicConfig(level=logging.INFO)
    worker_main_loop(terminate_on_exit=True)


if __name__ == "__main__":
    # as used in debug testing
    logging.basicConfig(level=logging.INFO)
    worker_main_loop(terminate_on_exit=False)
