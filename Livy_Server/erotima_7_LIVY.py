#!/usr/bin/env python
# coding: utf-8

#import and create spark session
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

spark = SparkSession.builder.master("local[*]").appName("BaseisII").config("spark.jars", "spark-measure_2.12-0.17.jar").getOrCreate()

#import csv
movies = spark.read.csv("/home/administrator/Downloads/movielens/movie.csv",inferSchema =True, header=True)
ratings = spark.read.csv("/home/administrator/Downloads/movielens/rating.csv",inferSchema =True, header=True)
tags = spark.read.csv("/home/administrator/Downloads/movielens/tag.csv",inferSchema =True, header=True)

#erotima 7
for i in range(1995,2015):
        ratings.select("userId","rating").filter(ratings.timestamp.like('%'+ str(i) + '%')) \
        .groupBy(ratings.userId.alias(str(i))).count().orderBy("count", ascending = False).show(10)

spark.stop()

