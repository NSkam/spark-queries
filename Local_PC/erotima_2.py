#import and create spark session

import findspark #must to work
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from sparkmeasure import StageMetrics

spark = SparkSession.builder.master("spark://john-VirtualBox:7077").appName("Erotima_2").config("spark.jars", "spark-measure_2.12-0.17.jar").getOrCreate()

#import csv

movies = spark.read.csv("Baseis/movie.csv",inferSchema =True, header=True)
ratings = spark.read.csv("Baseis/rating.csv",inferSchema =True, header=True)
tags = spark.read.csv("Baseis/tag.csv",inferSchema =True, header=True)
genome_tags = spark.read.csv("Baseis/genome_tags.csv",inferSchema =True, header=True)

#create spark metrics object
stagemetrics = StageMetrics(spark)

#start measuring performance
stagemetrics.begin()

#Erotima 2

movies.join(tags,movies.movieId == tags.movieId,'outer') \
    .select("title").filter(tags.tag == "boring").filter(movies.title.like("A%")).groupBy("title").avg().sort(col("title").desc()) \
    .show(truncate = False)

#stop measuring performance
stagemetrics.end()

#print performance metrics
stagemetrics.print_report()

spark.stop()
