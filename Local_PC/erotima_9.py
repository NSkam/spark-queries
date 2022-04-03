#!/usr/bin/env python
# coding: utf-8

import findspark #must to work
findspark.init()
#import and create spark session
from pyspark.sql import SparkSession

import pyspark.sql.functions as f
from sparkmeasure import StageMetrics

spark = SparkSession.builder.master("spark://john-VirtualBox:7077").appName("Erotima_9").config("spark.jars", "spark-measure_2.12-0.17.jar").getOrCreate()

#import csv

movies = spark.read.csv("Baseis/movie.csv",inferSchema =True, header=True)
ratings = spark.read.csv("Baseis/rating.csv",inferSchema =True, header=True)
tags = spark.read.csv("Baseis/tag.csv",inferSchema =True, header=True)
genome_tags = spark.read.csv("Baseis/genome_tags.csv",inferSchema =True, header=True)

#create spark metrics object
stagemetrics = StageMetrics(spark)

#start measuring performance
stagemetrics.begin()

#erotima 9
ratings.alias("ra1").join(ratings.alias("ra2"), on='timestamp')\
                    .where("ra1.timestamp == ra2.timestamp") \
                    .where("ra1.movieId == ra2.movieId")\
                    .where("ra1.userId != ra2.userId")\
                    .select(f.col('ra1.userId').alias('id1')).groupBy().count().show()

#stop measuring performance
stagemetrics.end()

#print performance metrics
stagemetrics.print_report()

spark.stop()
