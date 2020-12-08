#!/usr/bin/python

from google.cloud import storage
from google.cloud import dataproc_v1 as dataproc
from google.api_core import exceptions

def get_dataproc_client(region='us-east1'):
    """
    Returns a GCP Dataproc client handle
    """
    return dataproc.ClusterControllerClient(
                client_options={
        'api_endpoint': f'{region}-dataproc.googleapis.com:443'}
            )

def get_storage_client():
    """
    Returns a GCP Storage client handle.
    """
    return storage.Client()

def does_bucket_exist(bucket_iterator,bktname):
    if bktname in bucket_iterator:
        return True
    return False 

def create_bucket_if_notexists(bucket_name,location="US",storage_class="STANDARD"):
    """
    This checks if a bucket exists and creates it if it does not. This function
    creates buckets with bucket level access control not fine grained access control.
    """
    storage_client = get_storage_client()
    if does_bucket_exist(storage_client.list_buckets(),bucket_name) == False:
        bkt = storage.Bucket(storage_client,name=bucket_name)
        bkt.location = location
        bkt.storage_class = storage_class
        storage_client.create_bucket(bkt)
        bucket = storage_client.get_bucket(bkt.name)
        bucket.iam_configuration.uniform_bucket_level_access_enabled = True
        bucket.patch()
        print("Created bucket: {}".format(bkt.name))

def copy_file_to_bucket(bucket_name,local_file_name,prefix_folder=None,destination_file_name=None):
    """
    Copies a given local file to the bucket location specified, if it exists.
    """
    storage_client = get_storage_client()
    if storage_client.get_bucket(bucket_name).exists() == True:
        bucket = storage_client.bucket(bucket_name)
        if destination_file_name != None:
            if prefix_folder != None:
                destination_blob_name = prefix_folder + destination_file_name
            else:
                destination_blob_name = destination_file_name
            blob = bucket.blob(destination_blob_name)
        else:
            destination_file_name = local_file_name
            if prefix_folder != None:
                destination_blob_name = prefix_folder + destination_file_name
            else:
                destination_blob_name = destination_file_name
            blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(local_file_name)
        return True
    else:
        return False

def callback(future):
    result = future.result() 

def create_cluster(clustername,cfgbkt,tmpbkt,project_id,initfilelocation,machine_type='n1-standard-2',num_masters=1,num_workers=2,region='us-east1',disksize=600):
    """
    Creates a Dataproc cluster with the necessary files required for running a job.
    """
    dp_client = get_dataproc_client(region=region)
    ## Set up the cluster config
    cfg = {
        "project_id": project_id,
        "cluster_name": clustername,
        "config": {
            "temp_bucket": tmpbkt,
            "config_bucket": cfgbkt,
            "master_config": {"num_instances": num_masters,"machine_type_uri": machine_type,"disk_config": {"boot_disk_size_gb": disksize},"is_preemptible": False},
            "worker_config": {"num_instances": num_workers,"machine_type_uri": machine_type,"disk_config": {"boot_disk_size_gb": disksize},"is_preemptible": False},
            "software_config": {"image_version": "1.5","optional_components":['ANACONDA','JUPYTER','ZEPPELIN']},
            "initialization_actions": [{"executable_file": initfilelocation}],
            "endpoint_config": {"http_ports": {"SparkUI": "4040"},"enable_http_port_access": True}
        }
    }
    op = dp_client.create_cluster(
        request = {"project_id": project_id,"region": region,"cluster": cfg}
    )
    op.add_done_callback(callback)

    result = op.result()
    print("Cluster Created: {}".format(result.cluster_name))
    return result
