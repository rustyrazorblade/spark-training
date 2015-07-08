#!/bin/bash
if [ ! -d "ml-10M100K" ]; then
    wget http://files.grouplens.org/datasets/movielens/ml-10m.zip
    unzip ml-10m.zip

fi

if [ ! -f "ml-10M100K/ratings.subset.dat" ]; then
    (cd ml-10M100K;
    head -n 100000 ratings.dat > ratings.subset.dat)
fi
# if you want to try something more advanced, you can pull down a big dataset:
#


if [ ! -f "spark-cassandra-connector-assembly-1.4.0-M1-SNAPSHOT.jar" ]; then
    wget https://s3.amazonaws.com/haddad.public/spark-cassandra-connector-assembly-1.4.0-M1-SNAPSHOT.jar
fi

pip install -r requirements.txt

cqlsh -f setup.cql
