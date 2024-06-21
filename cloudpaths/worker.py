import boto3
import os
import json
from cloudpaths.lambda_utils import load_config

import logging

_logger = logging.getLogger(__name__)

# TODO: convert tasks to plugins; that will make this easier
class SingleTask:
    def __init__(self, message):
        self.message = message

    def claim_task(self):
        ...

    def run_task(self):
        ...

    def result_message(self, result):
        ...


class LaunchTask(SingleTask):
    def claim_task(self):
        ... # TODO: where/home?;

    def run_task(self):
        ... # TODO

    def result_message(self, result):
        ... # TODO


class TestLaunchTask(LaunchTask):
    def claim_task(self):
        return None

    def run_task(self):
        print("Would have created test_launch.db")
        ...  # TODO: log that this has run successfully

    def result_message(self, result):
        return None


class TestCycleLaunchTask(LaunchTask):
    def run_task(self):
        ...

    def result_message(self, result):
        ...

TASK_TYPE_DISPATCH = {
    "LAUNCH": LaunchTask,
    "TEST_LAUNCH": TestLaunchTask,
    "TEST_CYCLE": ...,
    "TEST_MULTICYCLE": ...,
}


def run_single_task(message):
    msg = json.loads(message['Body'])
    task_type = TASK_TYPE_DISPATCH[msg['TaskType']]
    cluster = msg['Cluster']
    cluster_conf = msg['Config']['clusters'][cluster]

    taskq_url = os.environ.get("CLOUDPATHS_TASK_QUEUE")
    assert taskq_url == cluster_conf['task_queue']['url']

    resultq_url = cluster_conf['result_queue']['url']

    task = task_type(msg)

    _logger.info("Claiming a task")
    _logger.debug(f"{msg=}\n{cluster_conf=}")
    task.claim_task()
    sqs.delete_message(
        QueueUrl=taskq_url,
        MessageId=message["MessageId"],
        ReceiptHandle=message['ReceiptHandle'],
    )

    _logger.info("Running the task")
    task_result = task.run_rask()

    # pass results to the result queue
    _logger.info("Passing results to the result queue")
    result_msg = task.result_message(task_result)
    if result_msg:
        resp = sqs.send_message(
            QueueUrl=resultq_url,
            MessageBody=json.dumps(result_msg),
            MessageGroupId=msg['result_db'],
        )

# this should become obsolete very soon
def _old_get_info():
    # get the task queue so we can start polling
    cluster = os.environ.get("CLOUDPATHS_CLUSTER")
    bucket = os.environ.get("CLOUDPATHS_BUCKET")
    prefix = os.environ.get("CLOUDPATHS_PREFIX")
    if not cluster or not bucket or not PREFIX:
        ... # TODO: raise error and exit

    # get stuff we need from config
    config = load_config(bucket, prefix)
    cluster_config = config['clusters'][cluster]
    task_queue_config = cluster_config['task_queue']
    max_attempts = task_queue_config['load_attempts_before_shutdown']
    sleep_time = task_queue_config['sleep_between_attempts']
    taskq_url = task_queue_config['url']
    resultq_url = cluster_config['result_queue']['url']


def worker_main_loop(terminate_on_exit=True):
    taskq_url = os.environ.get("CLOUDPATHS_TASK_QUEUE")
    max_attempts = int(os.environ.get("CLOUDPATHS_ATTEMPTS"))
    sleep_time = float(os.environ.get("CLOUDPATHS_WAIT"))

    if not (taskq_url and max_attempts and sleep_time):
        ... # TODO: raise error and exit
        # TODO: maybe set defaults for max_attempts/sleep_time?

    load_attempts = 1
    sqs = boto3.client('sqs')
    while load_attempts < max_attempts:
        resp = sqs.receive_message(
            QueueUrl=taskq_url,
            MaxNumberOfMessages=1,
            VisibilityTimeout=60,
            WaitTimeSeconds=0,
        )
        messages = resp.get("Messages")

        if not messages:
            load_attemps += 1
            time.sleep(sleep_time)
            continue

        if len(messages) > 1:
            # log warning, move on (process first message; others should
            # return to queue) # TODO
            ...

        run_single_task(messages[0])

    if terminate_on_exit:
        ...  # TODO: shut self down when we exit loop

if __name__ == "__main__":
    # TODO debug here; otherwise logging.INFO and do terminate on exit
    logging.basicConfig(level=logging.DEBUG)
    worker_main_loop(terminate_on_exit=False)
