#!/bin/bash
# Runs automatically when LocalStack is ready (placed in /etc/localstack/init/ready.d/)

awslocal s3 mb s3://ezu-f1-data --region eu-west-3
awslocal s3api put-bucket-versioning \
  --bucket ezu-f1-data \
  --versioning-configuration Status=Enabled

echo "LocalStack S3 bucket 'ezu-f1-data' created."
