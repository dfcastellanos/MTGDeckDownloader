#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json

from download_decks import make_search_payloads, download_decks_in_search_results
from helpers import LOG, send_sqs_msg

# pylint: disable=W0612, W0613, E0401

def make_search_payloads_handler(event, context):

    template_payload = event
    payload_list = make_search_payloads(template_payload)

    queue_name = "deck-search-payloads-queue"

    for payload in payload_list:
        response = send_sqs_msg(payload, queue_name)

    return {"statusCode": 200}


def download_decks_handler(msg, context):

    assert len(msg["Records"]) == 1
    payload = json.loads(msg["Records"][0]["body"])

    LOG.info(f"Downloading decks from search page with payload: {payload}")

    deck_list = download_decks_in_search_results(payload)

    queue_name = "downloaded-decks-queue"

    for deck in deck_list:
        response = send_sqs_msg(deck, queue_name)

    LOG.info(f"Finished downloading decks from search page with payload: {payload}")

    return {"statusCode": 200}
