#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import requests
from bs4 import BeautifulSoup
import argparse
from progressbar import progressbar
from joblib import Parallel, delayed
import hashlib

from helpers import LOG

# pylint: disable=W0105

"""
This module implements a web scraper that automatically finds, parses and 
downloads data corresponding to MTG decks from www.mtgtop8.com. It can be used
from the command line (see doc for the main function), imported
into other scripts using the function download_decks_in_search_results, or 
deployed as a serverless application in AWS Lamba.
"""


def make_deck_hash(deck):

    """
    Compute a hash that acts as a unique identifier for a deck.

    It's based on hashlib because python's built-in hash function is not deterministic
    (it is only within the same run). It keeps only the first 8 characters of the
    hash because that's unique enough and will save memory in the databases.

    Parameters
    ----------
    deck: dictionary
        A deck with at least the keys 'player', 'date' and 'event'

    Returns
    -------
    String
        The deck hash
    """

    x = deck["player"].strip(" ").replace("\r", "").replace("\n", "")
    y = deck["date"]
    z = deck["event"].strip(" ").replace("\r", "").replace("\n", "")
    deck_str_id = "{}|{}|{}".format(x, y, z)
    deck_hash = hashlib.sha224(str.encode(deck_str_id)).hexdigest()[:8]

    return deck_hash


def get_list(session_requests, payload):

    """
    Download the list of decks returned by the search engine

    Parameters
    ----------
    session_requests : requests.Session object
        A Session objects from the requests module

    payload : dictionary
        A payload for the search engine. Example: payload to
        retrieve the first list of results with decks that were played in the
        Modern format between 01/01/2010 and 01/01/2020:

                 {
                 'format': 'MO',
                  'current_page': 1,
                  'date_start':'01/01/2010',
                  'date_end': '01/01/2020'
                  }

    Returns
    -------
    List of dictionaries
        Each dictionary in the list corresponds to the metadata of each deck,
        including the link to its page (where the cards composing the deck can
        be found)
    """

    url = "http://mtgtop8.com/search"

    # request a list of decks from the search form
    deck_list_web = session_requests.post(url, data=payload)
    deck_list_web.raise_for_status()

    # parse the html reponse
    deck_list_soup = BeautifulSoup(deck_list_web.content, features="lxml")

    # main table with the deck list
    table = deck_list_soup.findAll("tr", {"class": "hover_tr"})

    # relative links to the decks
    rel_links = [
        td.find_all("td", {"class": "S12"})[0].find("a")["href"] for td in table
    ]

    # name of the decks
    names = [td.find_all("td", {"class": "S12"})[0].find("a").getText() for td in table]

    # name of the player
    players = [td.find("td", {"class": "G12"}).getText() for td in table]

    # name of the event
    events = [td.find("td", {"class": "S11"}).getText() for td in table]

    # make the absolute link to the deck
    links = ["https://www.mtgtop8.com/" + ref for ref in rel_links]

    # results of the decks in the competitions
    results = [td.find_all("td", {"class": "S12"})[1].getText() for td in table]

    # date of the competition in which the deck was played
    dates = [td.find_all("td", {"class": "S11"})[1].getText() for td in table]

    deck_list = [
        {
            "link": x[0],
            "result": x[1],
            "date": x[2],
            "player": x[3],
            "event": x[4],
            "deck_name": x[5],
        }
        for x in zip(links, results, dates, players, events, names)
    ]

    for deck in deck_list:
        deck["deck_id"] = make_deck_hash(deck)

    return deck_list


def get_composition(session_requests, deck):

    """
    Download the cards composing a decks form and the deck's type

    Parameters
    ----------
    session_requests : requests.Session object
        A Session objects from the requests module

    deck: dictionary
        A deck's metadata, including the link to its page. This dictionary
        is updated during this function call.

    Returns
    -------
    dictionary
        The input deck, updated with the cards that compose it and the deck type
    """

    # request a specific deck's website
    deck_web = session_requests.get(deck["link"])
    deck_web.raise_for_status()

    # parse the html reponse
    deck_web_soup = BeautifulSoup(deck_web.content, features="lxml")

    # the div with the download link has ' MTGO' on its text, and the next div
    # is the one with the deck type. We create a generator of divs to find
    # the one with the text, and then get and keep the next one
    div_list = (d for d in deck_web_soup.find_all("div", {"class": "S14"}))
    div = None

    for div in div_list:
        if " MTGO" in div.getText():
            break

    # download the deck's list of cards
    assert div is not None
    div_link = div
    download_rel_link = div_link.find("a")["href"]
    download_abs_link = "https://www.mtgtop8.com/" + download_rel_link
    deck_cards = session_requests.get(download_abs_link, allow_redirects=True)
    deck["cards"] = deck_cards.content.decode(encoding="ISO-8859-1")

    # better for parsing csv
    deck["cards"] = deck["cards"].replace("\n", ";\n")

    # make the split cards names to have the same double slash format
    deck["cards"] = deck["cards"].replace("/", "//")

    try:
        div_type = next(div_list)
        deck_type = div_type.find("a").getText().replace(" decks", "")
        deck["type"] = deck_type

    except StopIteration:
        # sometimes the deck type is missing, which results in StopIteration exception.
        # This problem seems to occur whenever the deck type is given as mana symbols
        # instead of as words
        LOG.error(
            "Problem parsing deck type and cards download link (deck name contains mana symbols?). Deck: %s",
            deck,
        )
        deck["type"] = "unkown"

    return deck


def make_deck_filename(deck):

    filename = "{}|{}|{}".format(deck["player"], deck["name"], deck["event"])
    filename = filename.replace(" ", "_").replace("/", "-")

    return filename


def make_search_payloads(payload):

    """
    Make the payloads needed for querying the search engine.
    Each payload corresponds to an individual results page,
    defined by the payload's current_page key.

    Since the search engine does not inform about the number of
    available result pages, this function looks for it by first finding an
    upper bound to the number of pages and then applying a bisection search to
    efficiently finding the exact number of results pages available.

    Parameters
    ----------
    payload : dictionary
        A template payload for the search engine. Example: payload
        to retrieve the first list of results with decks that were played in the
        Modern format between 01/01/2010 and 01/01/2020:

                 {
                 'format': 'MO',
                  'date_start':'01/01/2010',
                  'date_end': '01/01/2020'
                  }

    Returns
    -------
    List of dictionaries
        The list of payloads corresponding to individual result pages.
    """

    session_requests = requests.session()

    n = 1
    payload["current_page"] = n

    # make sure the first results page is not empty, otherwise we are searching wrongly
    deck_list = get_list(session_requests, payload)
    assert len(deck_list) > 0

    # modify the last result page until we find it empty. We multipy it by 2
    # in each repetition. Thus, when we find it empty, we know that the last
    # valid page is between the current n and the previous one (n/2)
    while len(deck_list) > 0:
        n *= 2
        payload["current_page"] = n
        deck_list = get_list(session_requests, payload)
    nmax = n
    nmin = int(n / 2)

    # apply a bisection search by searching in the middle of the interval
    # defined by the boundaries. If the middle point is empty, then it becomes
    # the new upper limit, if it's not, it becomes the lower limit. When
    # the nmax == nmin+1, the algorithm stalls and nmin is the last page with
    # valid search results that we are looking for
    while nmax != nmin + 1:
        n = (nmin + nmax) // 2
        payload["current_page"] = n

        deck_list = get_list(session_requests, payload)

        if len(deck_list) == 0:
            nmax = n
        else:
            nmin = n

    payload_list = list()
    # use nmax so nmin is the last included in the range
    for n in range(1, nmax):
        payload["current_page"] = n
        payload_list.append(payload.copy())

    return payload_list


def download_decks_in_search_results(payload):

    """
    Download the decks returned by the search engine when queried with the payload.

    Parameters
    ----------
    payload : dictionary
        A payload for the search engine. Example: a payload to
        retrieve the first list of results with decks that were played in the
        Modern format between 01/01/2010 and 01/01/2020:

                 {
                 'format': 'MO',
                  'current_page': 1,
                  'date_start':'01/01/2010',
                  'date_end': '01/01/2020'
                  }

    Returns
    -------
    List of dictionaries
        The list of payloads corresponding to individual result pages.
    """

    session_requests = requests.session()

    deck_list = get_list(session_requests, payload)
    if len(deck_list) == 0:
        return

    for deck in deck_list:
        # this call updates the input deck dictionary with extra data
        deck = get_composition(session_requests, deck)

    return deck_list


def main():

    """
    Download decks from www.mtgtop8.com. The results are printed to stdout in
    JSON format.

    Command-line interface:

      -h, --help            show this help message and exit
      -p PAYLOAD, --payload PAYLOAD
                            Payload for the search form. Example: '{"format":
                            "MO", "date_start": "25/09/2021", "date_end":
                            "27/09/2021"}'
      -n N, --n N           Number of parallel processes (warning: a high number may
                            cause the server to blacklist the IP address)

    """

    parser = argparse.ArgumentParser(description="Download decks from www.mtgtop8.com")
    parser.add_argument(
        "-p",
        "--payload",
        type=str,
        help='Payload for the search form. Example: \'{"format": "MO", "date_start": "25/09/2021", "date_end": "27/09/2021"}\'',
        required=True,
    )
    parser.add_argument(
        "-n",
        "--n",
        type=int,
        help="Number of parallel processes (warning: a high number may cause the server to blacklist the IP address)",
        default=1,
    )
    args = vars(parser.parse_args())

    # the input payload will be used as a template, from which a different payload
    # for each results page of the search form can be fetched
    template_payload = json.loads(args["payload"])
    payload_list = make_search_payloads(template_payload)

    n = args["n"]
    deck_double_list = Parallel(n)(
        delayed(download_decks_in_search_results)(payload)
        for payload in progressbar(payload_list)
    )

    decks_flat = dict()
    n = 0
    for sublist in deck_double_list:
        for deck in sublist:
            decks_flat[n] = deck
            n += 1

    return json.dumps(decks_flat)


if __name__ == "__main__":
    print(main())
