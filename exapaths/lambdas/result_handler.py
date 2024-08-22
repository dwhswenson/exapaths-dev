import collections
import contextlib
import json
import pathlib
import tempfile


import boto3
from botocore.exceptions import ClientError
from taskdb import TaskStatusDB

try:
    # in AWS lambda environment
    from aws_utils import s3_localfile
except ImportError:
    # in local environment (testing)
    from exapaths.aws_utils import s3_localfile


def process_add_tasks(db, message, config, metadata):
    """Add tasks from message to task database"""
    # assert consistency on db
    tasks = message['Tasks']
    for task in tasks:
        # TODO: figure out where to get the retry count from
        # TODO: clean the task dependencies, in case they include things
        # that are already completed (e.g., adding deps from inf-retis)
        max_tries = 3
        db.add_task(task['TaskId'], task['Dependencies'], max_tries)
        db.add_task_type(task['TaskId'], task['TaskType'])

def process_multi_result(db, message):
    ...

def process_success_result(db, message):
    ...

def process_failure_result(db, message):
    ...

def queue_all_available(db, config, metadata):
    """After processing results, queue all tasks that are now available.
    """
    sqs = boto3.client('sqs')
    while task_id := db.check_out_task():
        # cluster = metadata['default_cluster']
        # task_type = "default"
        # TODO: switch to this after we get the task_type into exorcist
        task_type = db.get_task_type(task_id)
        default = metadata['default_cluster']
        if taskq_dict := metadata.get('task_type_queue'):
            cluster = taskq_dict.get(task_type, default)
        else:
            cluster = default

        taskq_url = config['clusters'][cluster]['task_queue']['url']

        # pick taskq_url based on the task/config/metadata
        msg = {
            "task_type": task_type,
            "task_id": task_id,
            "cluster": cluster,
            "config": config,
            "metadata": metadata,
        }
        print("Sending: " + json.dumps(msg))
        print("Queue: " + taskq_url)
        sqs.send_message(
            QueueUrl=taskq_url,
            MessageBody=json.dumps(msg),
            MessageGroupId="tasks",
        )

RESULT_TYPE_DISPATCH = {
    "ADD_TASKS": process_add_tasks,
    "MULTI_RESULT": process_multi_result,
    "SUCCESS_RESULT": process_success_result,
    "FAIL_RESULT": process_failure_result,
}

def lambda_handler(event, context):
    """Process messages from result queue, then update task queue.
    """
    print(json.dumps(event))  # TODO: remove; this is for debugging
    print(f"Processing {len(event['Records'])} result messages...")

    by_msg_group = collections.defaultdict(list)
    for message in event['Records']:
        by_msg_group[message['attributes']['MessageGroupId']].append(message)

    for group_name, messages in by_msg_group.items():
        bucket, prefix, path = group_name.split("::")
        if prefix == "":
            key = path
        else:
            key = str(pathlib.Path(prefix) / path)
        print(f"{bucket=}")
        print(f"{prefix=}")
        print(f"{path=}")
        for message in messages:
            msg = json.loads(message['body'])
            print(msg)
            task_db = msg['files']['task_db']
            if prefix == "":
                task_db = str(pathlib.Path(prefix) / task_db)

            with s3_localfile(bucket, task_db) as db_filename:
                db = TaskStatusDB.from_filename(db_filename)

                config = msg['inputs']['config']
                metadata = msg['inputs']['metadata']
                # TODO check consistency result_db filename
                process = RESULT_TYPE_DISPATCH[msg['results']['result_type']]
                process(db, msg['results']['result_data'], config, metadata)
                queue_all_available(db, config, metadata)
        # TODO still need to delete the records
