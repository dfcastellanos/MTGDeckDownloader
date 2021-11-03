#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
from datetime import date
import os
import datetime
import pandas as pd
import boto3
import botocore

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


def load_payload_registry(key_to_registry_file):

    """
    Load the payload registry file as a DataFrame

    Parameters
    ----------
    key: string
        The S3 key to the file

    Returns
    -------
    DataFrame
        A pandas DataFrame with the loaded data
    """

    S3 = boto3.client("s3")
    bucket_name = os.environ["MTG_DATA_BUCKET"]

    try:
        S3.download_file(bucket_name, key_to_registry_file, "/tmp/payload_registry")
        df = pd.read_csv("/tmp/payload_registry")
        log_msg = "%s found" % key_to_registry_file
        LOG.info(log_msg)
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            # The object does not exist.
            df = pd.DataFrame()
            log_msg = "%s not found - creating a new one" % key_to_registry_file
            LOG.info(log_msg)
        else:
            raise

    return df


def udpate_payload_registry(template_payload, key_to_registry_file, mode):

    """
     Update the payload registry to include a new payload. It also writes the
     date at which the operation took place and the payload creation mode.

     Parameters
     ----------
     template_payload: dictionary
         The payload

    key: string
         The S3 key to the file

     mode: string
         Mode of creation of the payload (e.g., automated or manual)
    """

    data = template_payload.copy()
    data["operation_time"] = datetime.date.today().strftime("%d/%m/%Y")
    data["mode"] = mode

    df = load_payload_registry(key_to_registry_file)
    df = pd.concat([df, pd.DataFrame(data, index=[0])])
    df.to_csv("/tmp/payload_registry", index=False)

    S3 = boto3.client("s3")
    bucket_name = os.environ["MTG_DATA_BUCKET"]
    S3.upload_file("/tmp/payload_registry", bucket_name, key_to_registry_file)

    return


def generate_automatic_template_payload(key_to_registry_file):

    """
    This function creates deck search payloads with a start date equal to the
    end date of the last automatically generated, and an end date equal to the
    current date.

    Returns
    -------
    Dictionary
        The payload
    """

    df = load_payload_registry(key_to_registry_file)

    if len(df) > 0:
        template_payload = {
            "format": "MO",
            "date_start": df.iloc[-1]["date_end"],
            "date_end": datetime.date.today().strftime("%d/%m/%Y"),
        }
    else:
        template_payload = {
            "format": "MO",
            "date_start": "01/01/2005",
            "date_end": datetime.date.today().strftime("%d/%m/%Y"),
        }

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

    bucket_name = os.environ["MTG_DATA_BUCKET"]
    key_to_registry_file = "deck_payload_registry.csv"

    if event != "":
        template_payload = json.loads(event)
        udpate_payload_registry(template_payload, key_to_registry_file, "manual")
        log_msg = "Provided template payload: %s" % template_payload
        LOG.info(log_msg)
    else:
        template_payload = generate_automatic_template_payload(key_to_registry_file)
        udpate_payload_registry(template_payload, key_to_registry_file, "automated")
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
    defined by the environment variable MTG_DATA_BUCKET.

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

    bucket_name = os.environ["MTG_DATA_BUCKET"]

    for deck in deck_list:
        deck["date_download"] = datetime.date.today().strftime("%d/%m/%y")
        deck["deck_id"] = make_deck_hash(deck)
        filename = deck["deck_id"]
        key = f"decks/{filename}.json"
        response = write_data_s3_bucket(deck, bucket_name, key)

    LOG.info("Finished downloading decks from search page with payload: %s", payload)

    return {"statusCode": 200}
