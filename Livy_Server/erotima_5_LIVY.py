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

#erotima 5
movies.join(tags, movies.movieId == tags.movieId) \
                    .select(movies.title, tags.tag).filter(movies.title.like('%(2015)')) \
                    .sort(movies.title.asc()) \
                    .show(100,truncate=False)

spark.stop()
