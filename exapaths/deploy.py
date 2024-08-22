import json
import os.path
import pathlib
import shutil
import subprocess
import tempfile
import time
import zipfile

from importlib import resources
from datetime import datetime

import boto3
import click

from exapaths.aws_utils import load_config, store_config

def check_credentials():
    sts = boto3.client('sts')
    try:
        sts.get_caller_identity()
    except Exception as e:
        raise click.UsageError(str(e))

def update_run_launcher_lambda():
    """Update the run launcher to use the real lambda function.

    The template uses a placeholder so that we can easily use basic
    CloudFormation CreateStack.
    """
    file = resources.files("exapaths.lambdas") / "launch_run.py"
    utils = resources.files("exapaths") / "aws_utils.py"
    with tempfile.TemporaryDirectory() as tmpdir:
        zipfilename = pathlib.Path(tmpdir) / "launch_run.zip"
        with zipfile.ZipFile(zipfilename, mode='w') as zipf:
            zipf.write(file, arcname="launch_run.py")
            zipf.write(utils, arcname="aws_utils.py")

        with open(zipfilename, mode='rb') as f:
            zipfilebytes = f.read()

        aws_lambda = boto3.client("lambda")
        resp = aws_lambda.update_function_code(
            FunctionName="launch-run",
            ZipFile=zipfilebytes,
        )

def update_process_results_lambda():
    """Update the result processor to use the real lambda function.

    The template uses a placeholder so that we can easily use basic
    CloudFormation CreateStack.
    """
    # NOTES:
    # * AWS docs on this:
    #   https://docs.aws.amazon.com/lambda/latest/dg/python-package.html
    # * Note that, per PEP600, manylinux2014_x64_64 is an alias for
    #   manylinux_2_17_x64_64: https://peps.python.org/pep-0600/
    requirements = [
        'git+http://github.com/openfreeenergy/exorcist.git',
        'boto3',
    ]
    python_version = "3.12"
    files = ['lambdas/result_handler.py', 'aws_utils.py', 'taskdb.py']
    # lambda_file = resources.files('exapaths.lambdas') / "result_handler.py"
    # utils = resources.files("exapaths") / "aws_utils.py"

    # TODO: this seems like a potentially reusable function?
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = pathlib.Path(tmpdir)
        install_dir = tmp_path / "package"
        install_dir.mkdir()
        # using the approach recommended for native libraries

        print("Installing requirements for result processing lambda")
        cmd = [
            "python", "-m", "pip", "install",
            "--platform", "manylinux2014_x86_64",
            f"--target={str(install_dir)}",
            "--implementation", "cp",
            "--python-version", python_version,
            "--only-binary=:all:", "--upgrade"
        ] + requirements
        subprocess.run(cmd, check=True)

        print("Creating archive for result processing lambda and "
              "uploading")
        fname = tmp_path / "process_results_lambda.zip"
        shutil.make_archive(
            base_name=fname.parent / fname.stem,
            format="zip",
            root_dir=install_dir,
            base_dir=".",
        )

        def add_file(file, zipf):
            fsplit= file.split('/')
            resource = ".".join(['exapaths'] + fsplit[:-1])
            fname = fsplit[-1]
            filepath = resources.files(resource) / fname
            zipf.write(filepath, arcname=fname)

        with zipfile.ZipFile(fname, mode='a') as zipf:
            for file in files:
                add_file(file, zipf)

        with open(fname, mode='rb') as f:
            zipfilebytes = f.read()

        aws_lambda = boto3.client("lambda")
        resp = aws_lambda.update_function_code(
            FunctionName="process-results",
            ZipFile=zipfilebytes,
        )


def empty_bucket(bucket):
    # TODO: change this to only empty things we create during setup
    s3 = boto3.client('s3')
    resp = s3.list_objects_v2(Bucket=bucket)
    for obj in resp['Contents']:
        s3.delete_object(Bucket=bucket, Key=obj['Key'])


def get_cloudinfra_resource_contents(resource):
    cloudinfra = resources.files('exapaths.cloudinfra')
    with resources.as_file(cloudinfra.joinpath(resource)) as tf:
        with open(tf, mode='r') as f:
            template_contents = f.read()
    return template_contents


def start_stack_creation(template_name, stack_name, capabilities=None,
                         **kwargs):
    # convert kwargs to CF-style Parameters
    params = [
        {'ParameterKey': key, 'ParameterValue': val}
        for key, val in kwargs.items()
    ]

    # load template
    if os.path.exists(template_name):
        with open(template_name, mode='r') as f:
            template_contents = f.read()
    else:
        template_contents = get_cloudinfra_resource_contents(template_name)

    # start stack creation
    cf = boto3.client('cloudformation')
    stack_json = cf.create_stack(
        StackName=stack_name,
        TemplateBody=template_contents,
        Capabilities=capabilities,
        Parameters=params,
    )
    stack_id = stack_json['StackId']
    return stack_id


def wait_until_stack_completed(stack_id, prev_events=None):
    # update user while creation in progress
    cf = boto3.client('cloudformation')
    def is_stack_completed(stack_id):
        desc = cf.describe_stacks(StackName=stack_id)
        status = desc['Stacks'][0]['StackStatus']
        return "COMPLETE" in status

    if prev_events is None:
        prev_events = []
    while True:
        events = cf.describe_stack_events(StackName=stack_id)['StackEvents']
        num_to_report = len(events) - len(prev_events)
        events_to_report = events[:num_to_report]
        completed = is_stack_completed(stack_id)
        for event in reversed(events_to_report):
            report_stack_progress(event)

        if completed:
            break
        else:
            prev_events = events
            time.sleep(1)


def report_stack_progress(event):
    resource = event['LogicalResourceId']
    event_status = event['ResourceStatus']
    reason = event.get('ResourceStatusReason')
    outstr = None
    if event_status == "CREATE_IN_PROGRESS" and reason:
        print(f"{resource}: {reason}")
    if event_status == "CREATE_COMPLETE":
        print(f"{resource}: \033[32;mCreation complete!\033[0m")


def deploy_init(config, stage_config):
    stack_name = "cp-initial"
    print(f"Building core stack ({stack_name})")
    initial_stack = start_stack_creation(
        "initialsetup.yaml",
        stack_name=stack_name,
        capabilities=['CAPABILITY_NAMED_IAM']
    )
    wait_until_stack_completed(initial_stack)

    cf = boto3.client("cloudformation")
    resp = cf.list_stack_resources(StackName=initial_stack)
    name_to_resource = {r['LogicalResourceId']: r
                        for r in resp['StackResourceSummaries']}
    resultq_url = name_to_resource['ResultQueue']['PhysicalResourceId']
    sqs = boto3.client('sqs')
    resp = sqs.get_queue_attributes(QueueUrl=resultq_url,
                                    AttributeNames=["QueueArn"])
    resultq_arn = resp['Attributes']['QueueArn']
    resultq_config = {'url': resultq_url, 'arn': resultq_arn}

    print("Storing testing metadata files (used when running tests)")
    testing_meta = {
       'test_launch.json': {
           "default_queue": "default",
           "task_type_queue": {
               "TEST_LAUNCH": "default",
           },
        },
        'test_launch_cycle.json': {
           "default_queue": "default",
        },
        'test_launch_cycle_deps': {
           "default_queue": "default",
        },
        'test_launch_cycle_multiunblock': {
           "default_queue": "default",
        },
    }

    s3 = boto3.client('s3')
    for fname, json_obj in testing_meta.items():
        subpath = "launch_metadata/exapaths.testing"
        key = pathlib.Path(stage_config['prefix']) / subpath / fname
        s3.put_object(
            Bucket=stage_config['bucket'],
            Key=str(key),
            Body=json.dumps(json_obj).encode('utf-8'),
        )

    config.update(stage_config)
    config['result_queue'] = resultq_config
    config['init_stack'] = initial_stack
    return config

def update_lambdas(config, stage_config):
    print("LauncherLambda: updating function...")
    update_run_launcher_lambda()
    print("ResultQueueLambda: Updating function....")
    update_process_results_lambda()
    print("ResultQueueLambda: \033[32;mUpdate complete!\033[0m")
    return config

def deploy_cluster(config, stage_config, cluster_name=None):
    # TODO: separate this as a function?
    region = stage_config['region']
    instance_type = stage_config['instance_type']
    resultq_arn = config['result_queue']['arn']
    region_part = region.replace('-', '')
    instance_type_part = instance_type.replace('.', '')
    if cluster_name is None:
        cluster_name = f"exapaths-{region_part}-{instance_type_part}"
    print(f"\nBuilding first cluster ({cluster_name})")
    cluster_stack = start_stack_creation(
        "cluster.yaml",
        stack_name=cluster_name,
        capabilities=['CAPABILITY_NAMED_IAM'],
        ResultQueueArn=resultq_arn,
    )
    wait_until_stack_completed(cluster_stack)

    cf = boto3.client('cloudformation')
    resp = cf.list_stack_resources(StackName=cluster_stack)
    name_to_resource = {r['LogicalResourceId']: r
                        for r in resp['StackResourceSummaries']}
    taskq_url = name_to_resource['TaskQueue']['PhysicalResourceId']
    print("Uploading configuration information")
    config['clusters'][cluster_name] = {
        'region': region,
        'instance_type': instance_type,
        'task_queue': {
            'url': name_to_resource['TaskQueue']['PhysicalResourceId'],
            'task_attempts_before_shutdown': 3,
            'sleep_between_attempts': 20,
        },
        'stack': cluster_stack,
    }
    return config

def deploy(default_instance_type="t2.micro", network_only=False):
    start = datetime.now()
    print(f"Starting deployment at {start.strftime('%Y-%m-%d %X')}")
    bucket_name = "exapaths-1badc0de"
    config = {
        'clusters': {},
        'projects': {},
    }

    if not network_only:
        init_config = {'bucket': bucket_name, 'prefix': ""}
        config = deploy_init(config, init_config)

        config = update_lambdas(config, {})

    cluster_config = {
        'region': 'us-east-2',
        'instance_type': 't2.micro',
    }
    store_config(config)
    config = deploy_cluster(config, cluster_config)
    assert len(config['clusters']) == 1
    default_cluster = list(config['clusters'])[0]
    config['clusters']['default'] = config['clusters'][default_cluster]
    store_config(config)
    end = datetime.now()
    print(f"Finishing deployment at {end.strftime('%Y-%m-%d %X')}")
    print(f"Total deployment time: {end - start}")

def delete_stack(stack_id, force=False):
    cf = boto3.client('cloudformation')
    prev_events = cf.describe_stack_events(StackName=stack_id)['StackEvents']
    mode = {True: "FORCE_DELETE_STACK", False: "STANDARD"}[force]
    resp = cf.delete_stack(StackName=stack_id, DeletionMode=mode)
    wait_until_stack_completed(stack_id, prev_events)

def undeploy(config, force_empty_bucket=False, force=False, bucket=None):
    if force_empty_bucket:
        if bucket is None:
            bucket = config['bucket']
        print(f"Emptying bucket {bucket}")
        empty_bucket(bucket)

    if config:

        for project in config['projects'].values():
            delete_stack(project['stack'])

        for cluster in config['clusters'].values():
            delete_stack(cluster['stack'])

    delete_stack(config['init_stack'])


if __name__ == "__main__":
    check_credentials()
    deploy()
