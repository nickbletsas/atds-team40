from pyspark.sql import SparkSession
import time
import warnings
warnings.filterwarnings("ignore")

#Initialize SparkSession and SparkContext
spark = SparkSession.builder.appName("Q4_SQL").getOrCreate()

start_time = time.time()

#Read parquet file
taxi = spark.read.parquet("hdfs://master:9000/files/taxi/")

#Register taxi table
taxi = taxi.withColumnRenamed("_c2","tpep_pickup_datetime")
taxi = taxi.withColumnRenamed("_c4","Passenger_count")
taxi.registerTempTable("taxi")


#Execute query
q4 = spark.sql("""
(SELECT HOUR(t.tpep_pickup_datetime) AS hour, 'Sunday' AS day, avg(Passenger_count) AS passengers
FROM taxi AS t
WHERE dayofweek(t.tpep_pickup_datetime) = '1' AND t.tpep_pickup_datetime BETWEEN '2022-01-01' AND '2022-06-30'
GROUP BY HOUR(t.tpep_pickup_datetime), dayofweek(t.tpep_pickup_datetime)
ORDER BY avg(Passenger_count) desc limit 3)
UNION ALL
(SELECT HOUR(t.tpep_pickup_datetime) AS hour, 'Monday' AS day, avg(Passenger_count) AS passengers
FROM taxi AS t
WHERE dayofweek(t.tpep_pickup_datetime) = '2' AND t.tpep_pickup_datetime BETWEEN '2022-01-01' AND '2022-06-30'
GROUP BY HOUR(t.tpep_pickup_datetime), dayofweek(t.tpep_pickup_datetime)
ORDER BY avg(Passenger_count) desc limit 3)
UNION ALL
(SELECT HOUR(t.tpep_pickup_datetime) AS hour, 'Tuesday' AS day, avg(Passenger_count) AS passengers
FROM taxi AS t
WHERE dayofweek(t.tpep_pickup_datetime) = '3' AND t.tpep_pickup_datetime BETWEEN '2022-01-01' AND '2022-06-30'
GROUP BY HOUR(t.tpep_pickup_datetime), dayofweek(t.tpep_pickup_datetime)
ORDER BY avg(Passenger_count) desc limit 3)
UNION ALL
(SELECT HOUR(t.tpep_pickup_datetime) AS hour, 'Wednesday' AS day, avg(Passenger_count) AS passengers
FROM taxi AS t
WHERE dayofweek(t.tpep_pickup_datetime) = '4' AND t.tpep_pickup_datetime BETWEEN '2022-01-01' AND '2022-06-30'
GROUP BY HOUR(t.tpep_pickup_datetime), dayofweek(t.tpep_pickup_datetime)
ORDER BY avg(Passenger_count) desc limit 3)
UNION ALL
(SELECT HOUR(t.tpep_pickup_datetime) AS hour, 'Thursday' AS day, avg(Passenger_count) AS passengers
FROM taxi AS t
WHERE dayofweek(t.tpep_pickup_datetime) = '5' AND t.tpep_pickup_datetime BETWEEN '2022-01-01' AND '2022-06-30'
GROUP BY HOUR(t.tpep_pickup_datetime), dayofweek(t.tpep_pickup_datetime)
ORDER BY avg(Passenger_count) desc limit 3)
UNION ALL
(SELECT HOUR(t.tpep_pickup_datetime) AS hour, 'Friday' AS day, avg(Passenger_count) AS passengers
FROM taxi AS t
WHERE dayofweek(t.tpep_pickup_datetime) = '6' AND t.tpep_pickup_datetime BETWEEN '2022-01-01' AND '2022-06-30'
GROUP BY HOUR(t.tpep_pickup_datetime), dayofweek(t.tpep_pickup_datetime)
ORDER BY avg(Passenger_count) desc limit 3)
UNION ALL
(SELECT HOUR(t.tpep_pickup_datetime) AS hour, 'Saturday' AS day, avg(Passenger_count) AS passengers
FROM taxi AS t
WHERE dayofweek(t.tpep_pickup_datetime) = '7' AND t.tpep_pickup_datetime BETWEEN '2022-01-01' AND '2022-06-30'
GROUP BY HOUR(t.tpep_pickup_datetime), dayofweek(t.tpep_pickup_datetime)
ORDER BY avg(Passenger_count) desc limit 3)
""")

q4.show(21)

end_time = time.time()
print("Q4 sql parquet finished in " + str(end_time - start_time) +  " seconds")
