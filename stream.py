from pyspark.sql import SQLContext, Row
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from uuid import uuid1

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
    row_rdd = rdd.map(lambda (movie_id, user_id, rating, timestamp):
                          Row(movie_id=int(movie_id), user_id=int(user_id),
                              rating=int(rating), timestamp=float(timestamp)))

    df = sql.createDataFrame(row_rdd)

    # I want to get the average rating, and count of the number of ratings for each movie and persist it to cassandra
    from pyspark.sql import functions as F
    agg = df.groupBy("movie_id").agg(F.avg(df.rating).alias("avg"), F.count(df.rating).alias('count'), F.max(df.timestamp)).collect()
    writer(agg, "movie_stream_ratings")


ratings.foreachRDD(process_ratings)

stream.start()
stream.awaitTermination()
