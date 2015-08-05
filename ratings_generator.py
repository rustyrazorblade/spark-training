from socket import *
import time
import random

print "Binding socket"

s = socket(AF_INET, SOCK_STREAM)
s.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
s.bind(('127.0.0.1', 6000))

print "Socket bound"
print "Opening movie ratings"

start = time.time()
total = 0
with open("ml-10M100K/ratings.dat", 'r') as fp:

    print "Waiting for socket 6000"

    s.listen(1)

    conn, addr = s.accept()

    print "Socket accepted"

    # sends a random number of ratings (between 20-100 for a handful of movie)
    # comma delimited
    print "Sending ratings"
    for line in fp:
        total += 1
        (user_id, movie_id, rating, ts) = line.split("::")
        msg = "{}::{}::{}::{}\n".format(user_id, movie_id, rating, time.time())
        conn.sendall(msg)

total_time = time.time() - start
print "total time: {}".format(total_time)
print "{}/s".format(float(total) / total_time)
