from socket import *
import time
import random


s = socket(AF_INET, SOCK_STREAM)
s.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
s.bind(('127.0.0.1', 6000))

print "Waiting for socket 6000"
s.listen(1)

conn, addr = s.accept()

def send_ratings(conn):
    # sends a random number of ratings (between 20-100 for a handful of movie)
    # comma delimited
    print "Sending ratings"
    for x in range(random.randint(20, 100)):
        rating = random.randint(1, 5)
        movie_id = random.randint(1, 20)
        user_id = random.randint(1, 1000)
        conn.sendall("{}::{}::{}::{}\n".format(user_id, movie_id, rating, time.time()))


# when connected, generate movie ratings data
for x in range(1000):
    send_ratings(conn)
    time.sleep(.2)
