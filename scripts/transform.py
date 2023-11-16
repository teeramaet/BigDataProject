import sys
import math
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, when, to_date, monotonically_increasing_id
from pyspark.ml.stat import Correlation

spark = SparkSession.builder\
.master("local")\
.appName("Word Count")\
.getOrCreate()
#.config("spark.some.config.option", "some-value")\

df1 = spark.read.options(header='true', inferSchema='true').csv("../data_source/chess_games.csv")

df2 = df1.withColumn("Result", when((col("WhiteRatingDiff") < 0) & (col("BlackRatingDiff") > 0), "0-1")
    .when((col("WhiteRatingDiff") > 0) & (col("BlackRatingDiff") < 0), "1-0")
    .when((col("WhiteRatingDiff") == 0) & (col("BlackRatingDiff") == 0), "1/2-1/2")
    .otherwise(col("Result"))
)

df3 = df2.dropDuplicates().drop("TimeControl").drop("AN").drop("WhiteRatingDiff").drop("BlackRatingDiff").drop("UTCTime")

df4 = df3.filter(
    (df2["Event"].contains("Blitz")) |
    (df2["Event"].contains("Classic")) |
    (df2["Event"].contains("Bullet"))
)

df5 = df4.withColumn("Event", when(df3["Event"].contains("Blitz"), "Blitz").when(df3["Event"].contains("Classic"), "Classic").when(df3["Event"].contains("Bullet"), "Bullet").otherwise(df3["Event"]))

df6 = df5.withColumn("UTCDate", to_date("UTCDate", "yyyy.MM.dd"))

df7 =  df6.filter(col("Result") != '*')

df8 = df7.withColumn("id", monotonically_increasing_id())

df9 = df8.select("id", "Event", "White", "Black", "Result", "UTCDate", "WhiteElo", "BlackElo", "ECO", "Opening", "Termination")

