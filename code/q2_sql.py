from pyspark.sql import SparkSession
import time
import warnings
warnings.filterwarnings("ignore")

#Initialize SparkSession and SparkContext
spark = SparkSession.builder.appName("Q2_SQL").getOrCreate()

start_time = time.time()

#Read parquet file
taxi = spark.read.parquet("hdfs://master:9000/files/taxi/")

#Register taxi table
taxi = taxi.withColumnRenamed("_c2","tpep_pickup_datetime")
taxi = taxi.withColumnRenamed("_c15","tolls_amount")
taxi.registerTempTable("taxi")


#Execute query
max_tolls = spark.sql("""
(SELECT t.*
FROM taxi as t
WHERE t.tolls_amount = (SELECT MAX(t.tolls_amount) FROM taxi as t WHERE t.tpep_pickup_datetime BETWEEN '2022-01-01' AND '2022-01-31'))
UNION ALL
(SELECT t.*
FROM taxi as t
WHERE t.tolls_amount = (SELECT MAX(t.tolls_amount) FROM taxi as t WHERE t.tpep_pickup_datetime BETWEEN '2022-02-01' AND '2022-02-28'))
UNION ALL
(SELECT t.*
FROM taxi as t
WHERE t.tolls_amount = (SELECT MAX(t.tolls_amount) FROM taxi as t WHERE t.tpep_pickup_datetime BETWEEN '2022-03-01' AND '2022-03-31'))
UNION ALL
(SELECT t.*
FROM taxi as t
WHERE t.tolls_amount = (SELECT MAX(t.tolls_amount) FROM taxi as t WHERE t.tpep_pickup_datetime BETWEEN '2022-04-01' AND '2022-04-30'))
UNION ALL
(SELECT t.*
FROM taxi as t
WHERE t.tolls_amount = (SELECT MAX(t.tolls_amount) FROM taxi as t WHERE t.tpep_pickup_datetime BETWEEN '2022-05-01' AND '2022-05-31'))
UNION ALL
(SELECT t.*
FROM taxi as t
WHERE t.tolls_amount = (SELECT MAX(t.tolls_amount) FROM taxi as t WHERE t.tpep_pickup_datetime BETWEEN '2022-06-01' AND '2022-06-30'))
""")

max_tolls.show()

end_time = time.time()
print("Q2 sql parquet finished in " + str(end_time - start_time) +  " seconds")