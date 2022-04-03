#import and create spark session
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.master("local[*]").appName("BaseisII").config("spark.jars", "spark-measure_2.12-0.17.jar").getOrCreate()

#import csv
movies = spark.read.csv("/home/administrator/Downloads/movielens/movie.csv",inferSchema =True, header=True)
ratings = spark.read.csv("/home/administrator/Downloads/movielens/rating.csv",inferSchema =True, header=True)
tags = spark.read.csv("/home/administrator/Downloads/movielens/tag.csv",inferSchema =True, header=True)
genome_tags = spark.read.csv("/home/administrator/Downloads/movielens/genome_tags.csv",inferSchema =True, header=True)

#Category filter

import re
import string
unique = []
tot_genres = movies.select('genres').collect()
tot_genres = [str(row.genres) for row in tot_genres]
x = []
for i in range(len(tot_genres)):
    x.append(re.sub("[" + string.punctuation[-3] + "]", ' ',tot_genres[i]).split())

for i in x:
    for j in i:
        if (j not in unique):unique.append(j)
unique.pop(-1)
unique.pop(-1)
unique[-1] = '(no genres listed)'

#Erotima 8

for i in unique:
    max_rating = ratings.select("movieId","rating").groupBy("movieId").count()
    movies.join(max_rating,movies.movieId == max_rating.movieId, 'outer') \
        .select(col("title").alias("{0}".format(i)),"count").filter(movies.genres.like("%{0}%".format(i))) \
        .orderBy("count", ascending = False).show(1,truncate=False)

spark.stop()
