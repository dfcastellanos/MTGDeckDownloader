#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import requests.exceptions
import json

from conftest import path_to_validation_data
from conftest import DataHandlerType
import download_decks


def server_is_up(url):
    try:
        r = requests.get(url)
        r.raise_for_status()
    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
        print(f"Error connecting to {url}")
        return False
    except requests.exceptions.HTTPError:
        print(f"{url} is up but sent http error 4xx or 5xx")
        return False
    else:
        return True


def json_files_are_equal(file1, file2):

    # the deck files have json format. The order of the keys are not guaranteed
    # to the same. Thus, we first load the files into objects and compare them
    # (we don't need to use the specialised deck loading function because this is
    # only an easy way to assert that the plain text files contain the same info)
    return json.load(file1) == json.load(file2)


def test_mtgtop8():

    assert server_is_up("http://mtgtop8.com/")

    return


def test_get_list(tdeck):

    session_requests = requests.session()

    deck_list = download_decks.get_list(session_requests, tdeck["payload"])

    assert len(deck_list) == 1
    deck = deck_list[0]

    for k in ["result", "date", "player", "event", "deck_name"]:
        assert deck[k] == tdeck["deck"][k]

    return


def test_make_search_payloads(tpayloads):

    payload_list = download_decks.make_search_payloads(tpayloads["template_payload"])

    assert list(range(1, tpayloads["nmax"] + 1)) == [
        p["current_page"] for p in payload_list
    ]


def test_get_composition(tdeck):

    # get deck type and cards out of the deck. Those will be written again
    # when using these object for downloading the deck
    true_type = tdeck["deck"].pop("type")
    true_cards = tdeck["deck"].pop("cards")

    session_requests = requests.session()

    # using the deck data, we get download the type and the cards
    deck = download_decks.get_composition(session_requests, tdeck["deck"])

    # check that the downloaded type and cards match the original ones
    assert deck["type"] == true_type
    assert deck["cards"] == true_cards

    return


def test_download_decks_in_search_results(tdeck):

    deck_list = download_decks.download_decks_in_search_results(tdeck["payload"])

    assert len(deck_list) == 1
    deck = deck_list[0]

    dh_val = DataHandlerType(path_to_validation_data)
    assert deck == json.load(dh_val.read(tdeck["filename"]))

    return
