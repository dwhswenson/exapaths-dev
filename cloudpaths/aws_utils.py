"""
Utilities required in lambda functions (as well as possibly in workers).
"""
import boto3
import pathlib
import json
import contextlib
import tempfile

def load_config(bucket_name, prefix=""):
    s3 = boto3.client('s3')
    config = pathlib.Path(prefix) / "cloudpaths-config.json"
    resp = s3.get_object(Bucket=bucket_name, Key=str(config))
    as_utf8 = resp['Body'].read().decode('utf-8')
    as_json = json.loads(as_utf8)
    return as_json

def store_config(config):
    bucket_name = config['bucket']
    prefix = config['prefix']
    s3 = boto3.client('s3')
    config_key = pathlib.Path(prefix) / "cloudpaths-config.json"
    s3.put_object(Bucket=bucket_name, Key=str(config_key),
                  Body=json.dumps(config).encode('utf-8'))

@contextlib.contextmanager
def s3_localfile(bucket, object_key):
    # future: consider something like litestream for this? probably not
    # needed though... maybe for the main results? not for lambdas
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = pathlib.Path(tmpdir)
        dbfilename = tmpdir / pathlib.Path(object_key).name
        s3 = boto3.client('s3')
        try:
            print(f"Loading remote file: {bucket=} {object_key=}")
            resp = s3.get_object(Bucket=bucket, Key=object_key)
        except s3.exceptions.NoSuchKey:
            pass  # file does not yet exist; we don't need to read it
        else:
            # make a local copy of the file
            with open(dbfilename, mode='wb') as f:
                f.write(resp['Body'].read())

        try:
            yield dbfilename
        finally:
            with open(dbfilename, mode='rb') as f:
                s3.put_object(Bucket=bucket, Key=object_key, Body=f)


