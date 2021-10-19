import boto3
import json
import logging
from pythonjsonlogger import jsonlogger

LOG = logging.getLogger()
LOG.setLevel(logging.INFO)
logHandler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter()
logHandler.setFormatter(formatter)
LOG.addHandler(logHandler)
LOG.propagate = False

REGION = "eu-central-1"

SQS = boto3.client("sqs", region_name=REGION)


def send_sqs_msg(queue_name, msg, attrs):

    queue_url = SQS.get_queue_url(QueueName=queue_name)["QueueUrl"]
    queue_send_log_msg = "Send message to queue url: %s, with body: %s" % (
        queue_url,
        msg,
    )
    LOG.info(queue_send_log_msg)
    json_msg = json.dumps(msg)
    response = SQS.send_message(
        QueueUrl=queue_url, MessageBody=json_msg, MessageAttributes=attrs
    )
    queue_send_log_msg_resp = "Response to message sent to queue with url %s: %s" % (
        response,
        queue_url,
    )
    LOG.info(queue_send_log_msg_resp)

    return response
