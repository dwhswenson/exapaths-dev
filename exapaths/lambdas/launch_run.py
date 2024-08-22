"""
This is the lambda function that gets executed after we
"""
import boto3
import pathlib
import json

try:
    # in AWS lambda environment
    from aws_utils import load_config
except ImportError:
    # in standard environment (for testing)
    from exapaths.aws_utils import load_config


def get_bucket_and_object(event):
    """Validate the event, get the bucket and object (as Path)
    """
    if not len(event['Records']) == 1:
        print(f"Should only get 1 record, got {len(event['Records'])}")
        ... # log a weirdness

    record = event['Records'][0]

    if not record['eventSource'] == 'aws:s3':
        raise RuntimeError(f"Non s3 event source: {record['eventSource']}")

    bucket_name = record['s3']['bucket']['name']
    object_key = record['s3']['object']['key']

    obj = pathlib.Path(object_key)
    if obj.suffix != '.db':
        raise RuntimeError("Non db file: {obj}. Exapaths requires .db files")

    return bucket_name, obj


def get_prefix(obj: pathlib.Path):
    """Extract the prefix from the launch path.

    This is required because the launch path is the only information given;
    we need the prefix in order to get the config file.
    """
    splitted = str(obj).split("/launches/")
    if len(splitted) > 2:
        ... # log a weirdness but continue
    del splitted[-1]  # remove everything within exapaths
    prefix = "/launches/".join(splitted)
    if prefix:
        prefix += "/"

    return prefix


def get_metadata(config, obj):
    """Load metadata.

    A run can have custom metadata specifying whih queue tasks should be
    send to. This loads that metadata.
    """
    obj_json = obj.relative_to("launches/").with_suffix(".json")
    obj_meta = pathlib.Path(config['prefix']) / "launch_metadata" / obj_json
    s3 = boto3.client('s3')
    resp = s3.get_object(Bucket=config['bucket'], Key=str(obj_meta))
    metadata = json.loads(resp['Body'].read().decode('utf-8'))
    return metadata


def lambda_handler(event, context):
    """Pass the S3 object to the correct SQS queue.

    Takes an S3 event from SQS and sends a message describing the launch
    task to the queue.
    """
    bucket_name, obj = get_bucket_and_object(event)
    print(f"Using bucket: {bucket_name}")
    print(f"Loading object: {obj}")
    prefix = get_prefix(obj)
    config = load_config(bucket_name, prefix)
    metadata = get_metadata(config, obj)
    cluster = metadata.get('cluster', 'default')
    queue = config['clusters'][cluster]['task_queue']['url']
    launch_db = obj.relative_to(prefix)

    run_base = launch_db.parent.relative_to("launches")
    result_db_name = launch_db.name.removeprefix("launch-")
    result_db = "results" / run_base / result_db_name
    print(f"Saving results to {str(result_db)}")
    run_path = "running" / run_base / result_db.stem
    print(f"Details while running at {run_path}")
    task_db = run_path / "tasks" / "taskdb.db"
    # TODO: kind of want to find a way to stop here as a unit test point?

    # select the kind of run we're doing; this allows for various full-on
    # tests
    if "/exapaths.testing/" in str(obj):
        task_type = obj.stem.upper()
    else:
        task_type = "LAUNCH"

    message = {
        "task_type": task_type,
        "metadata": metadata,
        "config": config,
        "cluster": cluster,
        "files": {
            "launch_db": str(launch_db),
            "result_db": str(result_db),
            "working_path": str(run_path),
            "task_db": str(task_db),
        },
        "results": {}
        # "Cluster": cluster,
        # "Config": config,
        # "Metadata": metadata,
        # "Details": {
            # "launch_db": str(launch_db),
            # "result_db": str(result_db),
            # "working_path": str(run_path),
            # "task_db": str(task_db),
        # }
    }
    print(f"{message=}")
    sqs = boto3.client('sqs')
    resp = sqs.send_message(
        QueueUrl=queue,
        MessageBody=json.dumps(message),
        MessageGroupId="launches",
    )
