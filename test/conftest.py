#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pytest
import os
import sys
import json

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))
import download_decks
import data_handler

# pylint: disable=W0612, W0613, E0401

# _______________________________________________________________
# To use the local filesystem as the storage for test data

path_to_validation_data = "./test/data/"
path_to_tmp_data = "/tmp/mtg_data/"
DataHandlerType = data_handler.DataHandler


def pytest_sessionstart(session):

    # check we have read and write access in the directory where the tests will
    # write files
    assert os.access(path_to_validation_data, os.R_OK)
    assert os.access("/tmp", os.R_OK)
    assert os.access("/tmp", os.W_OK)

    # make sure that directory does not exists, otherwise we cannot trust the
    # test resutls
    if os.path.exists(path_to_tmp_data):
        os.system(f"rm -rf {path_to_tmp_data}")

    # create test dir structure
    os.mkdir(f"{path_to_tmp_data}")
    os.mkdir(f"{path_to_tmp_data}/database")
    os.mkdir(f"{path_to_tmp_data}/raw")
    os.mkdir(f"{path_to_tmp_data}/raw/prices")
    os.mkdir(f"{path_to_tmp_data}/raw/decks")

    return


def pytest_sessionfinish(session):

    # clean up test dir structure
    os.system(f"rm -rf {path_to_tmp_data}")

    return


# _______________________________________________________________
# To use S3 as the storage for test data

# import boto3
# from botocore.exceptions import ClientError

# path_to_validation_data = "mtg-analysis-test"
# path_to_tmp_data = "mtg-analysis-tmp"
# DataHandlerType = data_handler.DataHandlerS3

# def pytest_sessionstart(session):

#     s3 = boto3.resource("s3")

#     try:
#         bucket = s3.create_bucket(Bucket=path_to_tmp_data,
#                                  CreateBucketConfiguration={'LocationConstraint': 'eu-central-1'})
#     except ClientError as e:
#         if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
#             pass
#         else:
#             raise e

#     return


# def pytest_sessionfinish(session):

#     s3 = boto3.resource("s3")
#     bucket = s3.Bucket(path_to_tmp_data)

#     for obj in bucket.objects.all():
#         obj.delete()

#     bucket.delete()

#     return


# _______________________________________________________________
# Test fixtures


@pytest.fixture
def tpayloads():

    template_payload = {
        "format": "MO",
        "date_start": "25/09/2021",
        "date_end": "27/09/2021",
    }

    return {"template_payload": template_payload, "nmax": 5}


@pytest.fixture
def tdeck():

    # this payload should return a single deck
    payload = {
        "format": "MO",
        "event_titre": "MTGO Modern Preliminary",
        "deck_titre": "Burn",
        "date_start": "01/09/21",
        "date_end": "01/09/21",
    }

    filename = "test_deck.json"

    dh_val = DataHandlerType(path_to_validation_data)
    deck = json.load(dh_val.read(filename))

    return {
        "filename": filename,
        "deck": deck,
        "payload": payload,
    }
