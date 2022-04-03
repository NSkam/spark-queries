#!/usr/bin/env python
# coding: utf-8
import findspark #must to work
findspark.init()

#import and create spark session
from pyspark.sql import SparkSession
from sparkmeasure import StageMetrics

spark = SparkSession.builder.master("spark://john-VirtualBox:7077").appName("Erotima_1").config("spark.jars", "spark-measure_2.12-0.17.jar").getOrCreate()

#import csv

movies = spark.read.csv("Baseis/movie.csv",inferSchema =True, header=True)
ratings = spark.read.csv("Baseis/rating.csv",inferSchema =True, header=True)
tags = spark.read.csv("Baseis/tag.csv",inferSchema =True, header=True)
genome_tags = spark.read.csv("Baseis/genome_tags.csv",inferSchema =True, header=True)

#create spark metrics object
stagemetrics = StageMetrics(spark)

#start measuring performance
stagemetrics.begin()

#Erotima 1
movies.join(ratings, movies.movieId == ratings.movieId)       .select(movies.title, ratings.userId).filter(movies.title == "Jumanji (1995)")       .groupBy(movies.title).count().show()

#stop measuring performance
stagemetrics.end()

#print performance metrics
stagemetrics.print_report()
spark.stop()