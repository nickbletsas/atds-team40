from pyspark.sql import SparkSession
import time
import warnings
warnings.filterwarnings("ignore")

#Initialize SparkSession and SparkContext
spark = SparkSession.builder.appName("Q3_SQL").getOrCreate()

start_time = time.time()

#Read parquet file
taxi = spark.read.parquet("hdfs://master:9000/files/taxi/")

#Register taxi table
taxi = taxi.withColumnRenamed("_c2","tpep_pickup_datetime")
taxi = taxi.withColumnRenamed("_c5","Trip_distance")

taxi = taxi.withColumnRenamed("_c8","PUlocationID")
taxi = taxi.withColumnRenamed("_c9","DOlocationID")

taxi = taxi.withColumnRenamed("_c17","total_amount")

taxi.registerTempTable("taxi")


#Execute query
q3 = spark.sql("""
SELECT
    YEAR(tpep_pickup_datetime) AS YR,
    MONTH(tpep_pickup_datetime) AS MN,
    CASE WHEN DAY(tpep_pickup_datetime) <16 THEN 'First Half' ELSE 'Second Half' END Month_Part,
    AVG(Trip_distance),
    AVG(total_amount)
FROM taxi
WHERE PULocationID != DOLocationID AND tpep_pickup_datetime BETWEEN '2022-01-01' and '2022-06-30'
GROUP BY
    YEAR(tpep_pickup_datetime),
    MONTH(tpep_pickup_datetime),
    CASE WHEN DAY(tpep_pickup_datetime) <16 THEN 'First Half' ELSE 'Second Half' END
ORDER BY
    YEAR(tpep_pickup_datetime),
    MONTH(tpep_pickup_datetime)
""")

q3.show()

end_time = time.time()
print("Q3 sql parquet finished in " + str(end_time - start_time) +  " seconds")