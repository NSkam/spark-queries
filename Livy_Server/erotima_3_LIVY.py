#!/usr/bin/env python
# coding: utf-8

#import and create spark session
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, lower
import pyspark.sql.functions as f

spark = SparkSession.builder.master("local[*]").appName("BaseisII").config("spark.jars", "spark-measure_2.12-0.17.jar").getOrCreate()

#import csv
movies = spark.read.csv("/home/administrator/Downloads/movielens/movie.csv",inferSchema =True, header=True)
ratings = spark.read.csv("/home/administrator/Downloads/movielens/rating.csv",inferSchema =True, header=True)
tags = spark.read.csv("/home/administrator/Downloads/movielens/tag.csv",inferSchema =True, header=True)

#Erotima 3
tags_user_id = tags.select("userId").filter(lower(regexp_replace(tags.tag, r"(^\[)|(\]$)|(')", " ")) == "bollywood").collect()
tags_movies_id = tags.select("movieId").filter(lower(regexp_replace(tags.tag, r"(^\[)|(\]$)|(')", " ")) == "bollywood").collect()

tags_user_id = [int(row.userId) for row in tags_user_id]
tags_movies_id = [int(row.movieId) for row in tags_movies_id]

ratings.select("userId").filter(ratings.userId.isin(tags_user_id)).filter(ratings.movieId.isin(tags_movies_id)) \
        .filter(ratings.rating > 3).show(truncate = False)

spark.stop()
