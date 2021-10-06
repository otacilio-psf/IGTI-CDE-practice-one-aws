# Cloud Data Engineer Bootcamp

## Intruduction

This repo is from the activity proposed by IGTI.

Was made within AWS to practice this other cloud but I intend to create another one with Azure that I more used.

## Steps

### First

I created a python script to do the ingestion of ENEM 2019 file into S3.

    I run it locally but the ideal is that run in a container on cloud or with Airflow.

### Second

I developed locally the PySpark code that will move the raw data to the consumer zone in parquet format.

    The developmet env is insede dev folder and was cloned from https://github.com/otacilio-psf/spark-dev-env-docker

### Third

I Run the Spark app inside of the EMR cluster 