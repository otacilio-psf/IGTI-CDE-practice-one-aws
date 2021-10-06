# Cloud Data Engineer Bootcamp

## Introduction

This repo is for the activity proposed by IGTI in the Cloud Data Engineer Bootcamp.

It was made within AWS

## Task

1. Create an ingestion pipeline for ENEM 2019 data.
    * Source: [Inep Open Data](https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/enem)
    * Target: AWS S3
2. Move the data inside the data lake from raw-data to consumer-zone (csv -> paquet).
3. Create an AWS Glue tracker to catalog the data.
4. Query the data with AWS Athena.

## Steps

### First

I created a python script to do the ingestion of ENEM 2019 file into AWS S3.

    I run it locally but the ideal is that run in a container on cloud or with Airflow.

### Second

I developed locally the PySpark code that will move the raw data to the consumer zone in parquet format.

    The development env is inside dev folder and was cloned from https://github.com/otacilio-psf/spark-dev-env-docker

    For local development I used a sample of the data with:
    head -n 10000 data/DADOS/MICRODADOS_ENEM_2019.csv > dev/sample_data.csv

### Third

I process the Spark app inside of the AWS EMR cluster.

![Spark result](.attachment/spark-app-result.png "Spark result")

![S3 Result](.attachment/spark-app-result-s3.png "S3 Result")

### Fourth

I used a crawler with AWS Glue to create the metastore for the consume zone.

![Glue crawler](.attachment/glue-crawler.png "Glue crawler")

### Fifth

I used the AWS Athena to create queries on the ENEM data to answer the questions in the first practical activity.

![Athena Queries](.attachment/athena-result.png "Athena Queries")
![Athena Queries](.attachment/athena-result2.png "Athena Queries")