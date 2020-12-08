#!/usr/bin/env python

import sys
import os
import argparse
sys.path.insert(1, './gcp_setup')

from gcp_setup import get_storage_client
from gcp_setup import does_bucket_exist
from gcp_setup import create_bucket_if_notexists
from gcp_setup import copy_file_to_bucket
from gcp_setup import get_dataproc_client
from gcp_setup import create_cluster
from gcp_setup import callback

def search_and_replace(pathtozip,infile):
    fin = open("./unprocessed/"+infile, "r")
    fout = open(infile,"w+")
    for line in fin:
        if "[codebundlelocation]" in line:
            fout.write(line.replace("[codebundlelocation]","codebundlelocation=\""+pathtozip+"\""))
        else:
            fout.write(line)
    fin.close()
    fout.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Initiate BQ to S3 Data Transfer.')
    parser.add_argument('--bucket',required=True,type=str,help="Provide a valid GCS bucket name.")
    parser.add_argument('--prefix',required=True,type=str,help="Provide a folder structure under the bucket, do not use /'s in the beginning or end of this string.")
    parser.add_argument('--initfilename',required=True,type=str,help="Name of the file that has the cluster initialization code.")
    parser.add_argument('--location',required=True,type=str,help="location, like US, this has to be a string.")
    parser.add_argument('--ziplocation',required=True,type=str,help="full path of the directory which contains the codebundle.zip file.")
    parser.add_argument('--clustername',required=True,type=str,help="Name of the dataproc cluster, please keep this unique.")
    parser.add_argument('--clusterstgbucket',required=True,type=str,help="Dataproc cluster's staging bucket.")
    parser.add_argument('--clustertempbucket',required=True,type=str,help="Dataproc cluster's temporary bucket.")
    parser.add_argument('--projectid',required=True,type=str,help="Project Id.")
    parser.add_argument('--machinetype',required=False,default='n1-standard-2',type=str,help="Provide a valid GCP machine type.")
    parser.add_argument('--nummasters',required=False,default=1,type=int,help="Number of masters you want to use in this cluster.")
    parser.add_argument('--numworkers',required=False,default=2,type=int,help="Number of workers you want to use in this cluster.")
    parser.add_argument('--gcpregion',required=False,default='us-east1',type=str,help="The region to use in GCP.")
    parser.add_argument('--disksize',required=False,default=600,type=int,help="Disk size for all nodes in the cluster.")
    bktname = vars(parser.parse_args())['bucket']
    location = vars(parser.parse_args())['location']
    localdir = vars(parser.parse_args())['ziplocation']
    prefix = vars(parser.parse_args())['prefix']
    initfilename = vars(parser.parse_args())['initfilename']
    clstrnm = vars(parser.parse_args())['clustername']
    clstrstgbkt = vars(parser.parse_args())['clusterstgbucket']
    clstrtmpbkt = vars(parser.parse_args())['clustertempbucket']
    prjid = vars(parser.parse_args())['projectid']
    machine = vars(parser.parse_args())['machinetype']
    nm = vars(parser.parse_args())['nummasters']
    nw = vars(parser.parse_args())['numworkers']
    gcpregion = vars(parser.parse_args())['gcpregion']
    ds = vars(parser.parse_args())['disksize']
    
    iniflloc = "gs://"+bktname+"/"+prefix+initfilename
    gcspathtozip = "gs://"+bktname+"/"+prefix+"codebundle.zip"

    
    olddir = os.getcwd()
    os.chdir(localdir)
    create_bucket_if_notexists(bktname,location=location)
    if copy_file_to_bucket(bktname,"codebundle.zip",prefix_folder=prefix):
        print("File uploaded codebundle.zip")
    else:
        print("File upload failed for file codebundle.zip.")
    ## We also expect the cluster_initialization.sh in the same location, but should not be part
    ## of the codebundle.zip. We still need to process this file before we can upload it.

    search_and_replace(gcspathtozip,initfilename)

    if copy_file_to_bucket(bktname,initfilename,prefix_folder=prefix):
        print("File uploaded {}.".format(initfilename))
    else:
        print("File upload failed for file {}.".format(initfilename))
    os.chdir(olddir)
    
    ## Lets launch the cluster now
    result = create_cluster(clstrnm,clstrstgbkt,clstrtmpbkt,prjid,iniflloc,machine_type=machine,num_masters=nm,num_workers=nw,region=gcpregion,disksize=ds)
    print("Cluster {} launched.".format(result.cluster_name))

