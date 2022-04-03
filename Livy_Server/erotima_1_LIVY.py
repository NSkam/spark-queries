#!/usr/bin/env python
# coding: utf-8

#import and create spark session
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("BaseisII").config("spark.jars", "spark-measure_2.12-0.17.jar").getOrCreate()

movies = spark.read.csv("/home/administrator/Downloads/movielens/movie.csv",inferSchema =True, header=True)
ratings = spark.read.csv("/home/administrator/Downloads/movielens/rating.csv",inferSchema =True, header=True)
tags = spark.read.csv("/home/administrator/Downloads/movielens/tag.csv",inferSchema =True, header=True)


#Erotima 1
movies.join(ratings, movies.movieId == ratings.movieId).select(movies.title, ratings.userId).filter(movies.title == "Jumanji (1995)").groupBy(movies.title).count().show()

spark.stop()