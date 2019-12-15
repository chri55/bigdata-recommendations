#!bin/bash

cluster=cluster-ggend
zone=us-central1

gcloud dataproc jobs submit pyspark py/test-recommendations-alg.py --cluster$cluster --region $zone

gcloud compute ssh cluster-ggend-m --project=big-data-project-259918 --zone=us-central1-c -- -D 1080 -N 
