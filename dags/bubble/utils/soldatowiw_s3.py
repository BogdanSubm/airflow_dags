from airflow.hooks.base import BaseHook


def get_s3_client():

    import boto3
    from botocore.client import Config

    creds = BaseHook.get_connection('conn_s3')
    return boto3.client(
        's3',
        endpoint_url=creds.host,
        aws_access_key_id=creds.login,
        aws_secret_access_key=creds.password,
        config=Config(signature_version='s3v4'),
    )
