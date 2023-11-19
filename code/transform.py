import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_date, monotonically_increasing_id

spark = SparkSession.builder\
.appName("Word Count")\
.getOrCreate()


postfix = sys.argv[1]

PATH_SAHIT_SOURCE = "s3://landing-zone-" + postfix + "/chess_game_SAHIT.parquet"
PATH_REVEL_SOURCE = "s3://landing-zone-" + postfix + "/chess_game_REVEL.parquet"
PATH_SAHIT_DESTINATION = "s3://cleaning-zone-" + postfix + "/chess_game_SAHIT_clean.parquet"
PATH_REVEL_DESTINATION = "s3://cleaning-zone-" + postfix + "/chess_game_REVEL_clean.parquet"

print(PATH_REVEL_DESTINATION)
print(PATH_SAHIT_SOURCE)

df_src2_1 = spark.read.options(header='true', inferSchema='true').parquet(PATH_SAHIT_SOURCE)
df1 = spark.read.options(header='true', inferSchema='true').parquet(PATH_REVEL_SOURCE)

df_src2_2 = df_src2_1.withColumnRenamed('Date', 'UTCDate') 


df2 = df1.withColumn("Result", when((col("WhiteRatingDiff") < 0) & (col("BlackRatingDiff") > 0), "0-1")
    .when((col("WhiteRatingDiff") > 0) & (col("BlackRatingDiff") < 0), "1-0")
    .when((col("WhiteRatingDiff") == 0) & (col("BlackRatingDiff") == 0), "1/2-1/2")
    .otherwise(col("Result"))
)

df3 = df2.dropDuplicates().drop("TimeControl").drop("AN").drop("WhiteRatingDiff").drop("BlackRatingDiff").drop("UTCTime")
df_src2_3 = df_src2_2.dropDuplicates().drop("TimeControl").drop("Round").drop("WhiteRatingDiff")

df4 = df3.filter(
    (df3["Event"].contains("Blitz")) |
    (df3["Event"].contains("Classic")) |
    (df3["Event"].contains("Bullet"))
)

df_src2_4 = df_src2_3.filter(
    (df_src2_3["Event"].contains("Blitz")) |
    (df_src2_3["Event"].contains("Classic")) |
    (df_src2_3["Event"].contains("Bullet"))
)


df5 = df4.withColumn("Event", when(df3["Event"].contains("Blitz"), "Blitz").when(df3["Event"].contains("Classic"), "Classic").when(df3["Event"].contains("Bullet"), "Bullet").otherwise(df3["Event"]))
df_src2_5 = df_src2_4.withColumn("Event", when(df_src2_4["Event"].contains("Blitz"), "Blitz").when(df_src2_4["Event"].contains("Classic"), "Classic").when(df_src2_4["Event"].contains("Bullet"), "Bullet").otherwise(df_src2_4["Event"]))

df6 = df5.withColumn("UTCDate", to_date("UTCDate", "yyyy.MM.dd"))
df_src2_6 = df_src2_5.withColumn("UTCDate", to_date("UTCDate", "yyyy.MM.dd"))

df7 =  df6.filter(col("Result") != '*')
## Change this
df_src2_7 =  df_src2_6.filter(col("Result") != '*')


df8 = df7.withColumn("id", monotonically_increasing_id())
df_src2_8 = df_src2_7.withColumn("id", monotonically_increasing_id())


df9 = df8.select("id", "Event", "White", "Black", "Result", "UTCDate", "WhiteElo", "BlackElo", "ECO", "Opening", "Termination")
df_src2_9 = df_src2_8.select("id", "Event", "White", "Black", "Result", "UTCDate", "WhiteElo", "BlackElo", "ECO", "Opening", "Termination")


df9.coalesce(1).write.parquet(PATH_REVEL_DESTINATION)
df_src2_9.coalesce(1).write.parquet(PATH_SAHIT_DESTINATION)