from pyspark.sql import SparkSession
import os, sys, time
from pyspark.sql.functions import *

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


spark = SparkSession.builder.appName("Q3_rdd").getOrCreate()

start_time = time.time()

df = spark.read.parquet("hdfs://master:9000/files/taxi/")

df = df.withColumn("month", month("tpep_pickup_datetime"))\
       .withColumn("month_half", when((dayofmonth(df.tpep_pickup_datetime)) < 16, lit('First Half')).otherwise(lit('Second Half')))\
       .filter(year("tpep_pickup_datetime") == 2022)\
       .filter(month("tpep_pickup_datetime") < 7)

rdd = df.rdd

aTuple = (0,0)

rdd1 = rdd.filter(lambda x: x.PULocationID != x.DOLocationID)\
           .map(lambda x: ((int(x.month), str(x.month_half)), float(x.total_amount)))\
           .aggregateByKey(aTuple, lambda a,b: (a[0] + b, a[1] + 1), lambda a,b: (a[0] + b[0], a[1] + b[1]))\
           .mapValues(lambda v: v[0]/v[1])\
           .sortByKey()\
           .collect()

rdd2 = rdd.filter(lambda x: x.PULocationID != x.DOLocationID)\
           .map(lambda x: ((int(x.month), str(x.month_half)), float(x.trip_distance)))\
           .aggregateByKey(aTuple, lambda a,b: (a[0] + b, a[1] + 1), lambda a,b: (a[0] + b[0], a[1] + b[1]))\
           .mapValues(lambda v: v[0]/v[1])\
           .sortByKey()\
           .collect()

print("time, avg(total amount)")
for x in rdd1:
        print(x)

print()
print("time, avg(trip distance)")
for y in rdd2:
        print(y)

end_time = time.time()
print("Q3 rdd parquet finished in " + str(end_time - start_time) +  " seconds")