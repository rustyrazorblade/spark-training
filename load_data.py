def load_movies(sc, writer):
    movies = sc.textFile("ml-10M100K/movies.dat").map(lambda x: x.split("::") )
    movies = movies.map(lambda (x,y,z): (x,y,z.split("|")))
    movies = movies.toDF(["movie_id", "name", "tags"])
    writer(movies, "movie")
    return movies

def load_ratings(sc, writer):
    ratings_rdd = sc.textFile("ml-10M100K/ratings.subset.dat").map(lambda x: x.split("::") )
    ratings = ratings_rdd.toDF(["user_id", "movie_id", "rating", "timestamp"])
    writer(ratings, "rating_by_movie")
    return ratings
