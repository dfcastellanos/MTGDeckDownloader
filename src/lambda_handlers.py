#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
from datetime import date
import os
import datetime
import pandas as pd

from download_decks import (
    make_search_payloads,
    download_decks_in_search_results,
    make_deck_hash,
)
from helpers import LOG, send_sqs_msg, write_data_s3_bucket

# pylint: disable=W0105

"""
This module provides handlers that allow using the functions of the module 
download_decks as serverless applications in AWS Lambda.
"""


def generate_automatic_template_payload():

    """
    This function creates deck search payloads that are used when calling
    deck_producer automatically, based e.g. on scheduled events. It creates
    a payload with a start date equal to the end date of the last
    automatically-generated, and an end date equal to the current date.

    Returns
    -------
    Dictionary
        The payload
    """

    bucket_name = os.environ["OUTPUT_BUCKET"]
    key = "deck_automated_payloads.csv"
    path = f"s3://{bucket_name}/{key}"

    try:
        df = pd.read_csv(path)
        template_payload = {
            "format": "MO",
            "date_start": df.iloc[-1]["date_end"],
            "date_end": datetime.date.today().strftime("%d/%m/%Y"),
        }
        log_msg = "%s found" % key
        LOG.info(log_msg)

    except FileNotFoundError:

        df = pd.DataFrame()
        template_payload = {
            "format": "MO",
            "date_start": "01/01/2005",
            "date_end": datetime.date.today().strftime("%d/%m/%Y"),
        }

    data = template_payload.copy()
    data["operation_time"] = datetime.date.today().strftime("%d/%m/%Y-%H:%M")
    df = pd.concat([df, pd.DataFrame(data, index=[0])])
    df.to_csv(path, index=False)

    return template_payload


def deck_producer(event, context):

    # pylint: disable=W0612, W0613

    """
    This function is the AWS Lambda handler for the download_decks.make_search_payloads
    function, which acts as a producer. Specifically, it creates the payloads
    that can be processed in parallel by another Lambda function that acts as
    a consumer. The created payloads are sent to an SQS queue with a name defined
    by the environment variable DECKS_CONSUMER_QUEUE.

    If the event is an empty string, the deck search template payload passed to
    download_decks.make_search_payloads is generated automatically with a start
    date equal to the end date of the last automatically-generated, and an end
    date equal to the current date. If the event is a non-empty string, the
    string will be loaded as JSON.

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

    if event != "":
        template_payload = json.loads(event)
        log_msg = "Provided template payload: %s" % template_payload
        LOG.info(log_msg)
    else:
        template_payload = generate_automatic_template_payload()
        log_msg = "Auto-generated template payload: %s" % template_payload
        LOG.info(log_msg)

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

    Each of the jobs downloads several decks, which are sent to an S3 bucket
    defined by the environment variable OUTPUT_BUCKET.

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

    bucket_name = os.environ["OUTPUT_BUCKET"]

    for deck in deck_list:
        filename = make_deck_hash(deck)
        key = f"decks/{filename}.json"
        response = write_data_s3_bucket(deck, bucket_name, key)

    LOG.info("Finished downloading decks from search page with payload: %s", payload)

    return {"statusCode": 200}
