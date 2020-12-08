#!/bin/bash

while getopts g: flag
do
	case ${flag} in
		g) gcpcreds=${OPTARG};;
	esac
done
cd build
cp $gcpcreds gcp.creds
echo "Adding credentials to zip bundle."
zip -g codebundle.zip gcp.creds
zip -g codebundle.zip aws.creds
rm gcp.creds aws.creds
echo "Code Bundle ready."
