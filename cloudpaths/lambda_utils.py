import boto3
import pathlib
import json

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


