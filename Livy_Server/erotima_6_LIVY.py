#import and create spark session
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.master("local[*]").appName("BaseisII").config("spark.jars", "spark-measure_2.12-0.17.jar").getOrCreate()

#import csv
movies = spark.read.csv("/home/administrator/Downloads/movielens/movie.csv",inferSchema =True, header=True)
ratings = spark.read.csv("/home/administrator/Downloads/movielens/rating.csv",inferSchema =True, header=True)
tags = spark.read.csv("/home/administrator/Downloads/movielens/tag.csv",inferSchema =True, header=True)
genome_tags = spark.read.csv("/home/administrator/Downloads/movielens/genome_tags.csv",inferSchema =True, header=True)

#Erotima 6

t = ratings.select("movieId","rating").groupBy("movieId").count()

movies.join(t,movies.movieId == t.movieId, 'outer') \
    .select("title","count").orderBy("count", ascending = False).show(truncate = False)

spark.stop()
