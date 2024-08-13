"""
Utilities required in lambda functions (as well as possibly in workers).
"""
import boto3
import pathlib
import json
import contextlib
import tempfile

def load_config(bucket_name, prefix=""):
    """Load the exapaths configuration for the bucket/prefix.

    This configuration includes information like URLs of different queues,
    etc.
    """
    s3 = boto3.client('s3')
    config = pathlib.Path(prefix) / "exapaths-config.json"
    print(f"Loading config. Bucket: {bucket_name} "
          f"Config file: {str(config)}")
    resp = s3.get_object(Bucket=bucket_name, Key=str(config))
    as_utf8 = resp['Body'].read().decode('utf-8')
    as_json = json.loads(as_utf8)
    return as_json

def store_config(config):
    """Store the exapaths configuration.
    """
    bucket_name = config['bucket']
    prefix = config['prefix']
    s3 = boto3.client('s3')
    config_key = pathlib.Path(prefix) / "exapaths-config.json"
    s3.put_object(Bucket=bucket_name, Key=str(config_key),
                  Body=json.dumps(config).encode('utf-8'))

@contextlib.contextmanager
def s3_localfile(bucket, object_key):
    """Context manager to get a local file version of an S3 object.

    If the S3 ``object_key`` does not exist, then we create it.

    When the context manager terminates, we upload the updated file to S3.

    Parameters
    ----------
    bucket: str
        name of the S3 bucket
    object_key: str
        key for the object in the bucket
    """
    # future: consider something like litestream for this? probably not
    # needed though... maybe for the main results? not for lambdas
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = pathlib.Path(tmpdir)
        dbfilename = tmpdir / pathlib.Path(object_key).name
        s3 = boto3.client('s3')
        try:
            print(f"Loading remote file: s3://{bucket}/{object_key}")
            resp = s3.get_object(Bucket=bucket, Key=object_key)
        except s3.exceptions.NoSuchKey:
            print("File not found")
        else:
            # make a local copy of the file
            with open(dbfilename, mode='wb') as f:
                f.write(resp['Body'].read())

        try:
            yield dbfilename
        finally:
            if dbfilename.exists():
                print(f"Uploading {dbfilename} to "
                      f"s3://{bucket}/{object_key}")
                with open(dbfilename, mode='rb') as f:
                    s3.put_object(Bucket=bucket, Key=object_key, Body=f)
