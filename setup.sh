#!/bin/bash

wget http://files.grouplens.org/datasets/movielens/ml-10m.zip
unzip ml-10m.zip

wget https://haddad.public.s3.amazonaws.com/spark-cassandra-connector-assembly-1.4.0-M1-SNAPSHOT.jar
pip install -r requirements.txt
