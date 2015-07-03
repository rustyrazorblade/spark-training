# Spark Training

Make sure you have Cassandra 2.1+ installed, and Spark 1.4.  Also, Cassandra's `bin` directory needs to be in your PATH.

Run `setup.sh`:

    ./setup.sh

This will download the JAR required to use Cassandra + Spark 1.4, create the required keyspace & tables for the exercises, and download the movie lens dataset.  It'll also install the Python requirements.

To run:

    ./pys

This should fire up an iPython notebook.  Click exercises to begin.  Solutions are in the solutions notebook.  Resist the urge to check it till you've done all the exercises.
