#import and create spark session
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.master("local[*]").appName("BaseisII").config("spark.jars", "spark-measure_2.12-0.17.jar").getOrCreate()

#import csv
movies = spark.read.csv("/home/administrator/Downloads/movielens/movie.csv",inferSchema =True, header=True)
ratings = spark.read.csv("/home/administrator/Downloads/movielens/rating.csv",inferSchema =True, header=True)
tags = spark.read.csv("/home/administrator/Downloads/movielens/tag.csv",inferSchema =True, header=True)
genome_tags = spark.read.csv("/home/administrator/Downloads/movielens/genome_tags.csv",inferSchema =True, header=True)

#Erotima 2

movies.join(tags,movies.movieId == tags.movieId,'outer') \
    .select("title").filter(tags.tag == "boring").filter(movies.title.like("A%")).groupBy("title").avg().sort(col("title").desc()) \
    .show(truncate = False)
    
spark.stop()