# write leaderboard
writer(movies.select("id", "name", "avg_rating", explode(movies.genres).alias("genre")), "movie_leaderboard")

# register tables
movies.registerTempTable("movies")


