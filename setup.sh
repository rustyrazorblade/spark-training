#!/bin/bash

wget http://files.grouplens.org/datasets/movielens/ml-10m.zip
unzip ml-10m.zip

wget https://s3.amazonaws.com/haddad.public/spark-cassandra-connector-assembly-1.4.0-M1-SNAPSHOT.jar
pip install -r requirements.txt
