from pyspark.sql import SparkSession
import time
import warnings
warnings.filterwarnings("ignore")

#Initialize SparkSession and SparkContext
spark = SparkSession.builder.appName("Q1_SQL").getOrCreate()

start_time = time.time()

#Read parquet and csv files
taxi = spark.read.parquet("hdfs://master:9000/files/taxi/")
zone = spark.read.option("header", "true").option("inferSchema", "true").format("csv").csv("hdfs://master:9000/files/taxi+_zone_lookup.csv")

#Register taxi table
taxi = taxi.withColumnRenamed("_c2","tpep_pickup_datetime")
taxi = taxi.withColumnRenamed("_c7","DOlocationID")
taxi = taxi.withColumnRenamed("_c14","tip_amount")
taxi.registerTempTable("taxi")

zone = zone.withColumnRenamed("_c1", "locationid")
zone = zone.withColumnRenamed("_c3", "zone")
zone.registerTempTable("zone")

#Execute query
max_tip = spark.sql("""
SELECT t.*
FROM taxi as t, zone as z
WHERE t.tip_amount = (SELECT MAX(t.tip_amount) FROM taxi as t, zone as z WHERE z.zone = 'Battery Park' AND z.locationid = t.DOlocationID AND t.tpep_pickup_datetime between '2022-03-01' AND '2022-04-01') AND z.zone = 'Battery Park' AND z.locationid = t.DOlocationID AND t.tpep_pickup_datetime between '2022-03-01' AND '2022-04-01'
""")

max_tip.show()

end_time = time.time()
print("Q1 sql parquet finished in " + str(end_time - start_time) +  " seconds")