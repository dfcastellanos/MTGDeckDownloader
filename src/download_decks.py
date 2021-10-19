#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import requests
from bs4 import BeautifulSoup
import argparse
from progressbar import progressbar
from joblib import Parallel, delayed

from helpers import LOG

def get_list(session_requests, payload):

    # payload_example = {'format': 'MO', 'current_page':results_page_number,
    #                   'date_start':'01/01/2010',
    #                    'date_end':date.today().strftime('%d/%m/%Y')
    #                   }
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

    data_from_search_engine = [
        {
            "link": x[0],
            "result": x[1],
            "date": x[2],
            "player": x[3],
            "event": x[4],
            "name": x[5],
        }
        for x in zip(links, results, dates, players, events, names)
    ]

    return data_from_search_engine


def get_composition(session_requests, deck):

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
    deck["cards"] = deck_cards.content.decode("utf-8").replace("\n", ";\n")

    try:
        div_type = next(div_list)
        deck_type = div_type.find("a").getText().replace(" decks", "")
        deck["type"] = deck_type

    except StopIteration:
        # sometimes the deck type is missing, which results in StopIteration exception.
        # This problem seems to occur whenever the deck type is given as mana symbols
        # instead of as words
        LOG.error("ERROR parsing deck type and cards download link (deck name contains mana symbols?). Deck: %s", deck
        )
        deck["type"] = "unkown"

    return deck


def make_search_payloads(payload):

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

    session_requests = requests.session()

    deck_list = get_list(session_requests, payload)
    if len(deck_list) == 0:
        return

    for deck in deck_list:
        # this call updates the input deck dictionary with extra data
        deck = get_composition(session_requests, deck)

    return deck_list


def main():

    parser = argparse.ArgumentParser(description="Download decks from www.mtgtop8.com")
    parser.add_argument(
        "-p",
        "--payload",
        type=str,
        help='Payload for the search form. Example: \'{"format": "MO", "date_start": "25/09/2021", "date_end": "27/09/2021"}\'',
        required=True,
    )
    parser.add_argument(
        "-n", "--n", type=int, help="Number of parallel processes", default=1
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

    deck_flat_list = []
    for sublist in deck_double_list:
        for deck in sublist:
            deck_flat_list.append(deck)

    return deck_flat_list


if __name__ == "__main__":
    print(main())
