#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
from datetime import date
import os

from download_decks import make_search_payloads, download_decks_in_search_results
from helpers import LOG, send_sqs_msg

# pylint: disable=W0105

"""
This module provides handlers that allow using the functions of the module 
download_decks as serverless applications in AWS Lambda.
"""


def deck_producer(event, context):

    # pylint: disable=W0612, W0613

    """
    This function is the AWS Lambda handler for the download_decks.make_search_payloads
    function, which acts as a producer. Specifically, it creates the payloads
    that can be processed in parallel by another Lambda function that acts as
    a consumer. The created payloads are sent to an SQS queue with a name defined
    by the environment variable DECKS_CONSUMER_QUEUE.

    Parameters
    ----------
    event: string
        A JSON-formated string

    context : dictionary
        Details about AWS Lambda runtime used during the function call (see AWS
        Lambda for details)

    Returns
    -------
    Dictionary
        Success status code ("200")
    """

    LOG.debug("The input event is: %s", event)

    template_payload = json.loads(event)
    payload_list = make_search_payloads(template_payload)

    queue_name = os.environ["DECKS_CONSUMER_QUEUE"]
    attrs = {
        "msg_type": {"StringValue": "deck_search_payload", "DataType": "String"},
        "date_added": {
            "StringValue": date.today().strftime("%d/%m/%y"),
            "DataType": "String",
        },
    }

    for payload in payload_list:
        response = send_sqs_msg(queue_name, payload, attrs)

    return {"statusCode": 200}


def deck_consumer(event, context):

    # pylint: disable=W0612, W0613

    """
    This is the AWS Lambda handler for the function download_decks.download_decks_in_search_results.
    It is meant to be triggered when the AWS SQS queue with a name defined by the
    environment variable DECKS_CONSUMER_QUEUE has pending messages. Thus, this
    function acts as a consumer for the jobs created by deck_producer.

    Each of the jobs downloads several decks, which are sent to the AWS SQS queue
    with a name defined by the environment variable DECKS_OUTPUT_QUEUE.

    Parameters
    ----------
    event: string
        A JSON-formated string

    context : dictionary
        Details about AWS Lambda runtime used during the function call (see AWS
        Lambda for details)

    Returns
    -------
    Dictionary
        Success status code ("200")
    """

    LOG.debug("The input event is: %s", event)

    # only one msg should be received, because that msg already contains data
    # for downloading 25 decks. Thus, the SQS trigger should have batch size = 1
    assert len(event["Records"]) == 1
    payload = json.loads(event["Records"][0]["body"])

    LOG.info("Downloading decks from search page with payload: %s", payload)

    deck_list = download_decks_in_search_results(payload)

    queue_name = os.environ["DECKS_OUTPUT_QUEUE"]

    attrs = {
        "msg_type": {"StringValue": "full_deck", "DataType": "String"},
        "date_added": {
            "StringValue": date.today().strftime("%d/%m/%y"),
            "DataType": "String",
        },
    }

    for deck in deck_list:
        response = send_sqs_msg(queue_name, deck, attrs)

    LOG.info("Finished downloading decks from search page with payload: %s", payload)

    return {"statusCode": 200}
