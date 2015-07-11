from cassandra.cluster import Cluster
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.columns import *
from cassandra.cqlengine.connection import setup
from socket import *
import time
import random

setup(["127.0.0.1"], "training")

# pull list of movies out of cassandra

class Movie(Model):
    movie_id = Integer(primary_key=True)
    name = Text()
    tags = Set(Text)

movies = Movie.objects().limit(20)[:]


# creates a socket

print "Waiting for socket 6000"

s = socket(AF_INET, SOCK_STREAM)
s.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
s.bind(('127.0.0.1', 6000))
s.listen(1)

conn, addr = s.accept()

def send_ratings(conn):
    # sends a random number of ratings (between 20-100 for a handful of movie)
    # comma delimited
    print "Sending ratings"
    for x in range(random.randint(20, 100)):
        rating = random.randint(1, 5)
        movie_id = random.choice(movies).movie_id
        user_id = random.randint(1, 1000)
        conn.sendall("{}::{}::{}::{}\n".format(user_id, movie_id, rating, time.time()))


# when connected, generate movie ratings data
for x in range(1000):
    send_ratings(conn)
    time.sleep(.2)
