#!/bin/bash

## Collect all the files we need and bundle them into a zip file.
## This have to be run from under the parent directory 

## Always deletes the old build directory
rm -rf ./build
mkdir ./build
cp -r ./BQDataExtractor/ ./build
cp -r ./BQDetailsObject/ ./build
cp -r ./gcp_setup/ ./build
cp -r ./tests/ ./build
cp requirements.txt ./build
cp aws.creds ./build/aws.creds
cp move_tables.py ./build # The Spark application file itself
cd ./build
zip -r codebundle.zip ./*
cd -
mkdir ./build/unprocessed
cp cluster_initialization.sh ./build/unprocessed/cluster_initialization.sh
