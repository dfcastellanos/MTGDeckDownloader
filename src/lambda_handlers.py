#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json

from download_decks import make_search_payloads, download_decks_in_search_results
from helpers import LOG, send_sqs_msg


def deck_producer(event, context):

    # pylint: disable=W0612, W0613

    template_payload = json.loads(event)
    payload_list = make_search_payloads(template_payload)

    queue_name = "deck-search-payloads-queue"
    attrs = {"msg_type": {"StringValue": "deck_search_payload", "DataType": "String"}}

    for payload in payload_list:
        response = send_sqs_msg(queue_name, payload, attrs)

    return {"statusCode": 200}


def deck_consumer(msg, context):

    # pylint: disable=W0612, W0613

    assert len(msg["Records"]) == 1
    payload = json.loads(msg["Records"][0]["body"])

    LOG.info("Downloading decks from search page with payload: %s", payload)

    deck_list = download_decks_in_search_results(payload)

    queue_name = "downloaded-decks-queue"
    attrs = {"msg_type": {"StringValue": "full_deck", "DataType": "String"}}

    for deck in deck_list:
        response = send_sqs_msg(queue_name, deck, attrs)

    LOG.info("Finished downloading decks from search page with payload: %s", payload)

    return {"statusCode": 200}
