import findspark #must to work
findspark.init()

#import and create spark session
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from sparkmeasure import StageMetrics

spark = SparkSession.builder.master("spark://john-VirtualBox:7077").appName("Erotima_4").config("spark.jars", "spark-measure_2.12-0.17.jar").getOrCreate()

#import csv

movies = spark.read.csv("Baseis/movie.csv",inferSchema =True, header=True)
ratings = spark.read.csv("Baseis/rating.csv",inferSchema =True, header=True)
tags = spark.read.csv("Baseis/tag.csv",inferSchema =True, header=True)
genome_tags = spark.read.csv("Baseis/genome_tags.csv",inferSchema =True, header=True)

#create spark metrics object
stagemetrics = StageMetrics(spark)

#start measuring performance
stagemetrics.begin()

#Erotima 4

for i in range(1995,2016):
    top10 = movies.join(ratings,movies.movieId == ratings.movieId, 'outer') \
        .select("title","rating").groupBy("title").avg("rating") \
        .sort(col("avg(rating)").desc()) \
        .filter(movies.title.like("%({0})".format(i)))
    
    top10.select(col('title').alias('Year {0}'.format(i))) \
        .filter(movies.title.like("A%")) \
        .show(10,truncate = False)

#stop measuring performance
stagemetrics.end()

#print performance metrics
stagemetrics.print_report()

spark.stop()
