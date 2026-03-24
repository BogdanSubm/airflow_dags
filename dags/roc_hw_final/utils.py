from io import BytesIO
import codecs
import csv
from airflow.hooks.base import BaseHook
import boto3
from botocore.client import Config


def put_to_s3(context, s3_conn_id, headers, body):
    file = BytesIO()
    writer_wrapper = codecs.getwriter(encoding='utf-8')

    writer = csv.writer(
        writer_wrapper(file),
        delimiter='\t',
        lineterminator='\n',
        quotechar='"',
        quoting=csv.QUOTE_MINIMAL
    )

    writer.writerow(headers)
    writer.writerows(body)
    file.seek(0)

    s3_conn = BaseHook.get_connection(conn_id=s3_conn_id)

    s3_client = boto3.client(
        's3',
        endpoint_url=s3_conn.host,
        aws_access_key_id=s3_conn.login,
        aws_secret_access_key=s3_conn.password,
        config=Config(signature_version="s3v4")
    )

    s3_client.put_object(
        Body=file,
        Bucket='default-storage',
        Key=f"roc/lesson-8/raw_{context['ds']}.csv"
    )
