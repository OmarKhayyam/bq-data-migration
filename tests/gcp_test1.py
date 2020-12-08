#!/usr/bin/env python

import sys
import argparse
sys.path.insert(1, '../gcp_setup')

from gcp_setup import get_storage_client
from gcp_setup import does_bucket_exist
from gcp_setup import create_bucket_if_notexists
from gcp_setup import copy_file_to_bucket

def test_bucket_ops():
    bktname = "rns_my_bucket_test"
    create_bucket_if_notexists(bktname)
    if copy_file_to_bucket(bktname,"../cluster_initialization.sh"):
        print("File uploaded.")
    else:
        print("File could not be uploaded.")
    return bktname

def bucket_ops_teardown(bkt):
    cli = get_storage_client()
    bucket = cli.get_bucket(bkt)
    try:
        bucket.delete(force=True)
        print("Bucket {} successfully deleted.".format(bkt))
    except exceptions.NotFound:
        print("Bucket {} not found.".format(bkt))

if __name__ == "__main__":
    bkt = test_bucket_ops()
    bucket_ops_teardown(bkt)
