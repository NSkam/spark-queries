#import and create spark session
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.master("local[*]").appName("BaseisII").config("spark.jars", "spark-measure_2.12-0.17.jar").getOrCreate()

#import csv
movies = spark.read.csv("/home/administrator/Downloads/movielens/movie.csv",inferSchema =True, header=True)
ratings = spark.read.csv("/home/administrator/Downloads/movielens/rating.csv",inferSchema =True, header=True)
tags = spark.read.csv("/home/administrator/Downloads/movielens/tag.csv",inferSchema =True, header=True)
genome_tags = spark.read.csv("/home/administrator/Downloads/movielens/genome_tags.csv",inferSchema =True, header=True)


#Erotima 4

for i in range(1995,2016):
    top10 = movies.join(ratings,movies.movieId == ratings.movieId, 'outer') \
        .select("title","rating").groupBy("title").avg("rating") \
        .sort(col("avg(rating)").desc()) \
        .filter(movies.title.like("%({0})".format(i)))
    
    top10.select(col('title').alias('Year {0}'.format(i))) \
        .filter(movies.title.like("A%")) \
        .show(10,truncate = False)

spark.stop()
