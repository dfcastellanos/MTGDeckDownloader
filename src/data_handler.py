#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import click
import os
import pandas as pd
from io import StringIO
import boto3


class DataHandler:
    def __init__(self, root):
        self.root = root
        self.verbose = True

    def df_to_warehouse(self, df, dest_file):
        path = self.root
        if self.verbose:
            click.echo(f"Wrriting DataFrame to {path}/{dest_file}")
        df.to_csv(f"{path}/{dest_file}", index=False)
        return

    def df_from_warehouse(self, source_file):
        path = self.root
        if self.verbose:
            click.echo(f"Loading DataFrame from {path}/{source_file}")
        df = pd.read_csv(f"{path}/{source_file}")

        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])

        return df

    def write(self, iostr, filename):
        with open(self.root + "/" + filename, "w", encoding="utf-8") as outfile:
            outfile.write(iostr.getvalue())
        return

    def read(self, filename):
        with open(self.root + "/" + filename, "r", encoding="utf-8") as infile:
            return StringIO(infile.read())

    def file_exists(self, filename):
        return (
            os.path.isfile(self.root + "/" + filename)
            and os.stat(self.root + "/" + filename).st_size > 0
        )


class DataHandlerS3:
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.client = boto3.client("s3")
        self.verbose = True

    def write(self, iostr, key):
        self.client.put_object(Bucket=self.bucket_name, Key=key, Body=iostr.getvalue())
        return

    def read(self, key):
        res = self.client.get_object(Bucket=self.bucket_name, Key=key)["Body"].read()
        return StringIO(res.decode("utf-8"))
