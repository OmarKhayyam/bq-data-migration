#!/bin/bash

## This script brings all that we need to the Dataproc cluster, including
## the access credentials.

## Make sure you replace the bucket name with your own bucket name
## In this case, my bucket name is rns_ug_code_bucket

[codebundlelocation]

mkdir /work
cd /work
gsutil cp $codebundlelocation /work/
unzip ./codebundle.zip
pip install -r requirements.txt
mkdir creds

# Setup GCP credentials
## We use Service Account Credentials to enable access.
## Note that various apis like Storage, BigQuery etc... need to be
## separately enabled for you to be able to use them. So if they
## are not, make sure that they are, or the application will not
## work even if the credentials are good.
## You can use this link for guidance to get the necessary credentials
## and set them up: https://cloud.google.com/docs/authentication
## For the purposes of this application, you will have to package the
## credentials file separately as gcp.creds.json in the top level
## of the zip file, this should not be part of repo, it should
## be added to the archive just before launching the orchestrator
## utility.
mv gcp.creds ./creds/

## You will have to start the application after setting the following 
## environment variable.
## e.g. GOOGLE_APPLICATION_CREDENTIALS="/work/creds/gcp.creds.json" 
## spark-defaults.conf
## For advice: https://stackoverflow.com/questions/37887168/how-to-pass-environment-variables-to-spark-driver-in-cluster-mode-with-spark-sub

# Setup AWS credentials
## Get the AWS Access Key, Secret Key like so,
## https://docs.aws.amazon.com/general/latest/gr/aws-sec-cred-types.html
## You will have to keep the credentials file in the top level
## within the zip bundle and name it aws.creds
mv aws.creds ./creds/
