# -*- codeing=utf-8 -*-
# @Time :2023/12/12 19:55
# @Author:XY
# @File:rain_analyse.py
# @Software:


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, desc
from pyspark.sql.types import DecimalType

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.executorEnv.PYTHONHASHSEED", "0") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem") \
    .getOrCreate()
def passed_rain_analyse(filename):

   print("开始分析累积降雨量")
   spark = SparkSession.builder.config("spark.hadoop.fs.file.impl",
                                       "org.apache.hadoop.fs.RawLocalFileSystem").getOrCreate()

   # spark = SparkSession.builder.getOrCreate()

   df = spark.read.csv(filename, header=True)

   df_rain = df.select(col('province'), col('city_name'), col('city_code'),
                    col('rain1h').cast(DecimalType(scale=1))).filter(col('rain1h') < 1000)

   df_rain_sum = df_rain.groupBy("province", "city_name", "city_code") \
      .agg(sum("rain1h").alias("rain24h")) \
      .sort(desc("rain24h"))

   # df_rain_sum.coalesce(1).write.mode("overwrite").csv("passed_rain_analyse.csv")

   df_rain_sum.coalesce(1).write.csv("passed_rain_analyse.csv")

   print("累积降雨量分析完毕！")

   return df_rain_sum.head(20)  # 前20个

passed_rain_analyse("../data2/passed_weather_ALL.csv")