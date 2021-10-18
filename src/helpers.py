import boto3
import botocore
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

SQS = boto3.client("sqs")

REGION = "eu-central-1"


def sqs_queue_resource(queue_name):
    """Returns an SQS queue resource connection
    Usage example:
    In [2]: queue = sqs_queue_resource("dev-job-24910")
    In [4]: queue.attributes
    Out[4]:
    {'ApproximateNumberOfMessages': '0',
     'ApproximateNumberOfMessagesDelayed': '0',
     'ApproximateNumberOfMessagesNotVisible': '0',
     'CreatedTimestamp': '1476240132',
     'DelaySeconds': '0',
     'LastModifiedTimestamp': '1476240132',
     'MaximumMessageSize': '262144',
     'MessageRetentionPeriod': '345600',
     'QueueArn': 'arn:aws:sqs:us-west-2:414930948375:dev-job-24910',
     'ReceiveMessageWaitTimeSeconds': '0',
     'VisibilityTimeout': '120'}
    """

    sqs_resource = boto3.resource("sqs", region_name=REGION)
    log_sqs_resource_msg = (
        "Creating SQS resource conn with qname: [%s] in region: [%s]"
        % (queue_name, REGION)
    )
    LOG.info(log_sqs_resource_msg)
    queue = sqs_resource.get_queue_by_name(QueueName=queue_name)

    return queue


def sqs_connection():
    """Creates an SQS Connection which defaults to global var REGION"""

    sqs_client = boto3.client("sqs", region_name=REGION)
    log_sqs_client_msg = "Creating SQS connection in Region: [%s]" % REGION
    LOG.info(log_sqs_client_msg)

    return sqs_client


def sqs_approximate_count(queue_name):
    """Return an approximate count of messages left in queue"""

    queue = sqs_queue_resource(queue_name)
    attr = queue.attributes
    num_message = int(attr["ApproximateNumberOfMessages"])
    num_message_not_visible = int(attr["ApproximateNumberOfMessagesNotVisible"])
    queue_value = sum([num_message, num_message_not_visible])
    sum_msg = (
        """'ApproximateNumberOfMessages' and 'ApproximateNumberOfMessagesNotVisible' = *** [%s] *** for QUEUE NAME: [%s]"""
        % (queue_value, queue_name)
    )
    LOG.info(sum_msg)

    return queue_value


def delete_sqs_msg(queue_name, receipt_handle):

    sqs_client = sqs_connection()
    try:
        queue_url = sqs_client.get_queue_url(QueueName=queue_name)["QueueUrl"]
        delete_log_msg = "Deleting msg with ReceiptHandle %s" % receipt_handle
        LOG.info(delete_log_msg)
        response = sqs_client.delete_message(
            QueueUrl=queue_url, ReceiptHandle=receipt_handle
        )
    except botocore.exceptions.ClientError as error:
        exception_msg = (
            "FAILURE TO DELETE SQS MSG: Queue Name [%s] with error: [%s]"
            % (queue_name, error)
        )
        LOG.exception(exception_msg)
        return None

    delete_log_msg_resp = "Response from delete from queue: %s" % response
    LOG.info(delete_log_msg_resp)

    return response


def send_sqs_msg(msg, queue_name, delay=0):
    """Send SQS Message
    Expects an SQS queue_name and msg in a dictionary format.
    Returns a response dictionary.
    """

    queue_url = SQS.get_queue_url(QueueName=queue_name)["QueueUrl"]
    queue_send_log_msg = "Send message to queue url: %s, with body: %s" % (
        queue_url,
        msg,
    )
    LOG.info(queue_send_log_msg)
    json_msg = json.dumps(msg)
    response = SQS.send_message(
        QueueUrl=queue_url, MessageBody=json_msg, DelaySeconds=delay
    )
    queue_send_log_msg_resp = "Response to message sent to queue with url %s: %s" % (
        response,
        queue_url,
    )
    LOG.info(queue_send_log_msg_resp)

    return response
