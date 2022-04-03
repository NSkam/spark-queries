import findspark #must to work
findspark.init()

#import and create spark session
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from sparkmeasure import StageMetrics

spark = SparkSession.builder.master("spark://john-VirtualBox:7077").appName("Erotima_6").config("spark.jars", "spark-measure_2.12-0.17.jar").getOrCreate()

#import csv

movies = spark.read.csv("Baseis/movie.csv",inferSchema =True, header=True)
ratings = spark.read.csv("Baseis/rating.csv",inferSchema =True, header=True)
tags = spark.read.csv("Baseis/tag.csv",inferSchema =True, header=True)
genome_tags = spark.read.csv("Baseis/genome_tags.csv",inferSchema =True, header=True)

#Erotima 6

#create spark metrics object
stagemetrics = StageMetrics(spark)

#start measuring performance
stagemetrics.begin()

t = ratings.select("movieId","rating").groupBy("movieId").count()

movies.join(t,movies.movieId == t.movieId, 'outer') \
    .select("title","count").orderBy("count", ascending = False).show(truncate = False)
    
#stop measuring performance
stagemetrics.end()

#print performance metrics
stagemetrics.print_report()
spark.stop()
