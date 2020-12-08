#!/usr/bin/env python

import sys
sys.path.insert(1, '../gcp_setup')

from gcp_setup import get_dataproc_client
from gcp_setup import create_cluster
from gcp_setup import callback

def test_cluster_creation():
    result = create_cluster("rns-my-test-cluster","rns_my_staging_bucket","rns_my_temp_bucket","innate-entry-286804","gs://rns_ug_code_bucket/unicorn_gym/cluster_initialization.sh")
    if result.cluster_name != None:
        return "innate-entry-286804","us-east1",result.cluster_name
    else:
        return None,None,None

def cluster_creation_teardown(project_id,region,cluster_name):
    dpc = get_dataproc_client()
    print("Deleting cluster.")
    op = dpc.delete_cluster(request={"project_id": project_id,"region": region,"cluster_name": cluster_name})
    op.add_done_callback(callback)
    result = op.result()
    print(result)


if __name__ == "__main__":
    proj,reg,clustername = test_cluster_creation()
    if proj != None:
        cluster_creation_teardown(proj,reg,clustername)
