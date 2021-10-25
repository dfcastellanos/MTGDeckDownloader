#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from io import StringIO
import boto3

# pylint: disable=W0105

"""
This module defines classes that allow to write and read files
in a system-independent manner. The class DataHandler is used if the operations are performed on the local file system. The class DataHandlerS3 
is used if the operations are performed on AWS S3 buckets.
"""


class DataHandler:

    """
    This class provides methods to read and write files in the local file system.
    """

    def __init__(self, root):

        """
        Initialize the object.

        Parameters
        ----------
        root: string
            The root directory, with respect to which relative file paths are defined.
        """

        self.root = root

    def write(self, iostr, filename):

        """
        Write a file.

        Parameters
        ----------
        iostr: StringIO
            The string stream with the data to be written.
        filename: string
            The destination file, including its relative path.
        """

        with open(self.root + "/" + filename, "w", encoding="utf-8") as outfile:
            outfile.write(iostr.getvalue())

        return

    def read(self, filename):

        """
        Read a file.

        Parameters
        ----------
        filename: string
            The source file, including its relative path.

        Returns
        -------
        StringIO
            A string stream with the read data.
        """

        with open(self.root + "/" + filename, "r", encoding="utf-8") as infile:
            return StringIO(infile.read())

    def file_exists(self, filename):

        """
        Check if a file exists. It returns False if the file does not exist or if it
        exists but has a size of 0 bytes.

        Parameters
        ----------
        filename: string
            The file, including its relative path.

        Returns
        -------
        Bool
            Whether the file exists or not.
        """

        return (
            os.path.isfile(self.root + "/" + filename)
            and os.stat(self.root + "/" + filename).st_size > 0
        )


class DataHandlerS3:

    """
    This class provides methods to read and write files in an AWS S3 bucket.
    """

    def __init__(self, bucket_name):

        """
        Initiallize the object.

        Parameters
        ----------
        bucket_name : string
            The AWS S3ucket name
        """

        self.bucket_name = bucket_name
        self.client = boto3.client("s3")

    def write(self, iostr, key):

        """
        Write a file.

        Parameters
        ----------
        iostr: StringIO
            The string stream with the data to be written.
        key: string
            The key of the object that is to be written in the S3 bucket.
        """

        self.client.put_object(Bucket=self.bucket_name, Key=key, Body=iostr.getvalue())
        return

    def read(self, key):

        """
        Read a file.

        Parameters
        ----------
        key: string
            The key of the object that is to be loaded from the S3 bucket.

        Returns
        -------
        StringIO
            A string stream with the loaded data.
        """

        res = self.client.get_object(Bucket=self.bucket_name, Key=key)["Body"].read()
        return StringIO(res.decode("utf-8"))
