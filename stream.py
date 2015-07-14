from pyspark.sql import SQLContext, Row
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from uuid import uuid1

from time import time as now

conf = SparkConf() \
    .setAppName("ratings stream") \
    .setMaster("spark://127.0.0.1:7077") \
    .set("spark.cassandra.connection.host", "127.0.0.1")

# set up our contexts
sc = SparkContext(conf=conf)
sql = SQLContext(sc)
stream = StreamingContext(sc, 1) # 1 second window

def create_writer(sql, keyspace, mode="append"):
    def writer(df, table):
        df.write.format("org.apache.spark.sql.cassandra").\
                 options(table=table, keyspace=keyspace).save(mode="append")
    return writer

writer = create_writer(sql, "training")

lines = stream.socketTextStream("127.0.0.1", 6000)

ratings = lines.map(lambda line: line.split("::"))

def process_ratings(time, rdd):
    print "============== %s ============" % str(time)
    #
    ts = now()
    row_rdd = rdd.map(lambda (movie_id, user_id, rating, timestamp):
                          Row(movie_id=int(movie_id), user_id=int(user_id),
                              rating=int(rating), ts=ts))


    df = sql.createDataFrame(row_rdd)
    df.registerTempTable("ratings")

    # I want to get the average rating, and count of the number of ratings for each movie and persist it to cassandra
    from pyspark.sql import functions as F

    movie_to_ts = sql.sql("select distinct movie_id, ts from ratings")
    movie_to_ts.registerTempTable("movie_ts")

    # going to join this against itself
    agg = sql.sql("SELECT movie_id, avg(rating) as a, count(rating) as c from ratings group by movie_id")
    agg.registerTempTable("movie_aggregates")

    matched = sql.sql("select a.movie_id, a.a, a.c, b.ts as ts from movie_aggregates a join movie_ts b on a.movie_id = b.movie_id  ")

    writer(matched, "movie_stream_ratings")

    print "========== DONE WRITING ============== "


ratings.foreachRDD(process_ratings)

stream.start()
stream.awaitTermination()
