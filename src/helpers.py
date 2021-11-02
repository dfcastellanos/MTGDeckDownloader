import boto3
import json
import logging
from pythonjsonlogger import jsonlogger

# pylint: disable=W0105

LOG = logging.getLogger()
LOG.setLevel(logging.INFO)
logHandler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter()
logHandler.setFormatter(formatter)
LOG.addHandler(logHandler)
LOG.propagate = False

# the AWS region
REGION = "eu-central-1"

SQS = boto3.client("sqs", region_name=REGION)
S3 = boto3.client("s3", region_name=REGION)


def send_sqs_msg(queue_name, msg, attrs):

    """
    Send a message to an AWS SQS queue.

    Parameters
    ----------
    queue_name : string
        The queue name

    msg: dictionary
        The message

    attrs:
        Message attributes, with a format

        {"attr1": {"StringValue": value1", "DataType": "String"},
         "attr2": {"StringValue": "value2", "DataType": "String"},
         ...
        }

    Returns
    -------
    Dictionary
        The response from SQS to the send message operation
    """

    queue_url = SQS.get_queue_url(QueueName=queue_name)["QueueUrl"]
    queue_send_log_msg = "Send message to queue url: %s, with body: %s" % (
        queue_url,
        msg,
    )
    LOG.debug(queue_send_log_msg)
    json_msg = json.dumps(msg)
    response = SQS.send_message(
        QueueUrl=queue_url, MessageBody=json_msg, MessageAttributes=attrs
    )
    queue_send_log_msg_resp = "Response to message sent to queue with url %s: %s" % (
        queue_url,
        response,
    )
    LOG.info(queue_send_log_msg_resp)

    return response


def send_data_s3_bucket(body, bucket_name, key):

    """
    Write data to an AWS S3 bucket.

    Parameters
    ----------
    bucket_name: string
        The name of the S3 bucket

    key : string
        The key where the data is located within the bucket

    body : string
        The body

    Returns
    -------
    Dictionary
        The response from S3
    """

    send_log_msg = "Sending data to s3 bucket %s, with body: %s" % (
        bucket_name,
        body,
    )
    LOG.debug(send_log_msg)
    json_data = json.dumps(body)
    response = S3.put_object(Bucket=bucket_name, Key=key, Body=json_data)

    send_log_msg_resp = "Response to data sent to s3 bucket %s: %s" % (
        bucket_name,
        response,
    )
    LOG.info(send_log_msg_resp)

    return response
