from pyspark.sql import SparkSession
import time
import warnings
warnings.filterwarnings("ignore")

#Initialize SparkSession and SparkContext
spark = SparkSession.builder.appName("Q5_SQL").getOrCreate()

start_time = time.time()

#Read parquet file
taxi = spark.read.parquet("hdfs://master:9000/files/taxi/")

#Register taxi table
taxi = taxi.withColumnRenamed("_c2","tpep_pickup_datetime")
taxi = taxi.withColumnRenamed("_c15","Tip_amount")
taxi = taxi.withColumnRenamed("_c17","Total_amount")
taxi.registerTempTable("taxi")


#Execute query
q5 = spark.sql("""
(SELECT DAY(t.tpep_pickup_datetime) AS day, 'January' AS month, avg(ROUND((t.Tip_amount * 100)/t.Total_amount,2)) AS percentage
FROM taxi AS t
WHERE MONTH(t.tpep_pickup_datetime) = '1' AND t.tpep_pickup_datetime BETWEEN '2022-01-01' AND '2022-06-30'
GROUP BY DAY(t.tpep_pickup_datetime), MONTH(t.tpep_pickup_datetime)
ORDER BY percentage desc limit 5)
UNION ALL
(SELECT DAY(t.tpep_pickup_datetime) AS day, 'February' AS month, avg(ROUND((t.Tip_amount * 100)/t.Total_amount,2)) AS percentage
FROM taxi AS t
WHERE MONTH(t.tpep_pickup_datetime) = '2' AND t.tpep_pickup_datetime BETWEEN '2022-01-01' AND '2022-06-30'
GROUP BY DAY(t.tpep_pickup_datetime), MONTH(t.tpep_pickup_datetime)
ORDER BY percentage desc limit 5)
UNION ALL
(SELECT DAY(t.tpep_pickup_datetime) AS day, 'March' AS month, avg(ROUND((t.Tip_amount * 100)/t.Total_amount,2)) AS percentage
FROM taxi AS t
WHERE MONTH(t.tpep_pickup_datetime) = '3' AND t.tpep_pickup_datetime BETWEEN '2022-01-01' AND '2022-06-30'
GROUP BY DAY(t.tpep_pickup_datetime), MONTH(t.tpep_pickup_datetime)
ORDER BY percentage desc limit 5)
UNION ALL
(SELECT DAY(t.tpep_pickup_datetime) AS day, 'April' AS month, avg(ROUND((t.Tip_amount * 100)/t.Total_amount,2)) AS percentage
FROM taxi AS t
WHERE MONTH(t.tpep_pickup_datetime) = '4' AND t.tpep_pickup_datetime BETWEEN '2022-01-01' AND '2022-06-30'
GROUP BY DAY(t.tpep_pickup_datetime), MONTH(t.tpep_pickup_datetime)
ORDER BY percentage desc limit 5)
UNION ALL
(SELECT DAY(t.tpep_pickup_datetime) AS day, 'May' AS month, avg(ROUND((t.Tip_amount * 100)/t.Total_amount,2)) AS percentage
FROM taxi AS t
WHERE MONTH(t.tpep_pickup_datetime) = '5' AND t.tpep_pickup_datetime BETWEEN '2022-01-01' AND '2022-06-30'
GROUP BY DAY(t.tpep_pickup_datetime), MONTH(t.tpep_pickup_datetime)
ORDER BY percentage desc limit 5)
UNION ALL
(SELECT DAY(t.tpep_pickup_datetime) AS day, 'June' AS month, avg(ROUND((t.Tip_amount * 100)/t.Total_amount,2)) AS percentage
FROM taxi AS t
WHERE MONTH(t.tpep_pickup_datetime) = '6' AND t.tpep_pickup_datetime BETWEEN '2022-01-01' AND '2022-06-30'
GROUP BY DAY(t.tpep_pickup_datetime), MONTH(t.tpep_pickup_datetime)
ORDER BY percentage desc limit 5)
""")

q5.show(30)

end_time = time.time()
print("Q5 sql parquet finished in " + str(end_time - start_time) +  " seconds")