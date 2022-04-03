#!/usr/bin/env python
# coding: utf-8


import findspark #must to work
findspark.init()

#import and create spark session
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, lower
import pyspark.sql.functions as f
from sparkmeasure import StageMetrics

spark = SparkSession.builder.master("spark://john-VirtualBox:7077").appName("Erotima_3").config("spark.jars", "spark-measure_2.12-0.17.jar").getOrCreate()

#import csv

movies = spark.read.csv("Baseis/movie.csv",inferSchema =True, header=True)
ratings = spark.read.csv("Baseis/rating.csv",inferSchema =True, header=True)
tags = spark.read.csv("Baseis/tag.csv",inferSchema =True, header=True)
genome_tags = spark.read.csv("Baseis/genome_tags.csv",inferSchema =True, header=True)

#create spark metrics object
stagemetrics = StageMetrics(spark)

#start measuring performance
stagemetrics.begin()

#Erotima 3
tags_user_id = tags.select("userId").filter(lower(regexp_replace(tags.tag, r"(^\[)|(\]$)|(')", " ")) == "bollywood").collect()
tags_movies_id = tags.select("movieId").filter(lower(regexp_replace(tags.tag, r"(^\[)|(\]$)|(')", " ")) == "bollywood").collect()

tags_user_id = [int(row.userId) for row in tags_user_id]
tags_movies_id = [int(row.movieId) for row in tags_movies_id]

ratings.select("userId").filter(ratings.userId.isin(tags_user_id)).filter(ratings.movieId.isin(tags_movies_id)) \
        .filter(ratings.rating > 3).show(truncate = False)

#stop measuring performance
stagemetrics.end()

#print performance metrics
stagemetrics.print_report()

spark.stop()
