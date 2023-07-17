from utils import timeit


def create_rdd(spark):
    movies_rdd   = spark.textFile("hdfs://master:9000/home/user/files/movies.csv")
    ratings_rdd  = spark.textFile("hdfs://master:9000/home/user/files/ratings.csv")
    genres_rdd   = spark.textFile("hdfs://master:9000/home/user/files/movie_genres.csv")

    return movies_rdd, ratings_rdd, genres_rdd


def query1(spark):
    # Fetch initial RDD from a csv
    movies_rdd, _, _ = create_rdd(spark)

    # Get the difference betwen revenue and production cost (i.e. profits) of every
    # movie after 1995 
    movies = movies_rdd.map(lambda line: line.split(',')) \
                       .filter(lambda field: len(field) > 7 and field[3].isdigit() and field[6].isdigit() and field[5].isdigit()) \
                       .map(lambda field: (int(field[3]), (int(field[6]), int(field[5])))) \
                       .filter(lambda field: field[0] > 1995 and field[1][0] > 0 and field[1][1] > 0) \
                       .map(lambda field: (field[0], str(field[1][0] - field[1][1]))) \
                       .reduceByKey(lambda v1, v2: v1 + ", " + v2) \
                       .sortBy(lambda pair: pair[0])
    
    return timeit(movies.collect)


def query2(spark):
    # Fetch initial RDDs from the csv files
    movies_rdd, ratings_rdd, _ = create_rdd(spark)

    # Get the movie id, the average rating and the total number of ratings for the
    # movie “Cesare deve morire”
    mapped_movies = movies_rdd.map(lambda line: line.split(',')) \
                              .filter(lambda fields: len(fields) == 8 and fields[3].isdigit() and fields[6].isdigit()) \
                              .filter(lambda fields: fields[1] == "Cesare deve morire") \
                              .map(lambda fields: (int(fields[0]), ('movies', fields[1])))

    mapped_ratings = ratings_rdd.map(lambda line: line.split(',')) \
                                .filter(lambda fields: len(fields) == 4) \
                                .map(lambda fields: (int(fields[1]), (float(fields[2]), 1))) \
                                .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
                                .map(lambda pair: (pair[0], ('ratings', pair[1][0] / pair[1][1], pair[1][1])))

    union = mapped_movies.union(mapped_ratings)

    movie_stats = union.groupByKey() \
                       .flatMap(lambda kv: [(kv[0], m[1], m[2]) for m in kv[1] if m[0] == 'ratings' for g in kv[1] if g[0] == 'movies'])

    return timeit(movie_stats.collect)


def query3(spark):
    # Fetch initial RDDs from the csv files
    movies_rdd, _, genres_rdd = create_rdd(spark)

    # Get the best Animation movie in terms of revenue for 1995
    mapped_movies = movies_rdd.map(lambda line: line.split(',')) \
                              .filter(lambda fields: len(fields) == 8 and fields[3].isdigit() and fields[6].isdigit()) \
                              .filter(lambda fields: int(fields[3]) == 1995 and int(fields[5]) > 0 and int(fields[6]) > 0) \
                              .map(lambda fields: (int(fields[0]), ('movies', fields[1], int(fields[6]))))

    mapped_genres = genres_rdd.map(lambda line: line.split(',')) \
                              .filter(lambda fields: len(fields) == 2 and fields[0].isdigit() and fields[1] == 'Animation') \
                              .map(lambda fields: (int(fields[0]), ('genres', fields[1])))

    # Union of the two RDDs
    union = mapped_movies.union(mapped_genres)

    # Group by key and transform the result
    joined = union.groupByKey() \
                  .flatMap(lambda kv: [(m[1], m[2]) for m in kv[1] if m[0] == 'movies' for g in kv[1] if g[0] == 'genres'])

    # Action takes place through the joined(), so the timeit() function is placed accordingly    
    time_taken, best_animation_movie = timeit(joined.reduce, lambda movie, next_movie: movie if movie[1] > next_movie[1] else next_movie)

    return time_taken, best_animation_movie 


def query4(spark):
    # Fetch initial RDDs from the csv files
    movies_rdd, _, genres_rdd = create_rdd(spark)

    # Get the most popular Comedy movie for each year after 1995
    mapped_movies = movies_rdd.map(lambda line: line.split(',')) \
                              .filter(lambda field: len(field) == 8 and field[3].isdigit() and field[6].isdigit()) \
                              .filter(lambda field: int(field[3]) > 1995 and float(field[7]) > 0) \
                              .map(lambda field: (int(field[0]), ('movies', int(field[3]), field[1], float(field[7]))))

    mapped_genres = genres_rdd.map(lambda line: line.split(',')) \
                              .filter(lambda field: len(field) == 2 and field[0].isdigit()) \
                              .filter(lambda field: field[1] == 'Comedy') \
                              .map(lambda field: (int(field[0]), ('genres', field[1])))

    # Make union of movies and genres
    union = mapped_movies.union(mapped_genres)

    # Extract the best comedy per year
    best_comedy = union.groupByKey() \
                       .flatMap(lambda kv: [(m[1], (m[2], m[3])) for m in kv[1] if m[0] == 'movies' for g in kv[1] if g[0] == 'genres']) \
                       .reduceByKey(lambda x, y: x if x[1] > y[1] else y) \
                       .sortBy(lambda pair: pair[0])
    
    return timeit(best_comedy.collect)


def query5(spark):
    # Fetch initial RDD from a csv
    movies_rdd, _, _ = create_rdd(spark)

    # Get the average revenue for each year
    mapped_movies = movies_rdd.map(lambda line: line.split(',')) \
                              .filter(lambda fields: len(fields) == 8 and fields[3].isdigit() and fields[6].isdigit()) \
                              .filter(lambda fields: int(fields[3]) > 0 and int(fields[6]) > 0) \
                              .map(lambda fields: (int(fields[3]), (int(fields[6]), 1))) \
                              .reduceByKey(lambda revenue, next_revenue: (revenue[0] + next_revenue[0], revenue[1] + next_revenue[1])) \
                              .map(lambda fields: (fields[0], fields[1][0] / fields[1][1])) \
                              .sortBy(lambda pair: pair[0])

    return timeit(mapped_movies.collect)
