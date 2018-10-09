from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F

spark = SparkSession \
    .builder \
    .appName("Content Watchtime") \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

data = spark.read.format('com.databricks.spark.csv') \
    .options(header=False, inferschema='true', delimiter="\t") \
    .load('sparkBigData.csv')

# UDF to remove [ ] from the string.
def timeStamp(timeinList):
    time = timeinList.replace("[",'').replace(']','')
    return time

timer = F.udf(lambda z:timeStamp(z),StringType())

# Format of Input date.
format = "dd/MMM/yyyy:HH:mm:ssZ"

# Putting headers to column
# merging, processing date column
# Calculating throughput per second by dividing Bytes with send/time column.
# Getting Hour from the timestamp column which can be used for further grouping.

data = data.select(F.col('_c0').alias('Country'), F.col('_c1').alias('ASN'), \
                   F.unix_timestamp(timer(F.concat_ws("",F.col('_c2'),F.col('_c3'))),format=format).cast('timestamp').alias('timestamp'), \
                   F.col('_c4').alias('Metric A '), F.col('_c5').alias('Co Server'), \
                   F.col('_c6').alias('Bytes'),F.col('_c7').alias('Send/Time')) \
    .withColumn('Throughput', F.col('Bytes')/F.col('Send/Time')) \
    .withColumn('hour', F.hour('timestamp'))

data = data.fillna({'Throughput':0.0})
data.show(truncate=False)

# Grouping by Keys and removing columns where send time is 0
data.where(F.col('Throughput')!=0.0).groupBy('Co Server').agg(F.avg('Throughput')).show()
data.where(F.col('Throughput')!=0.0).groupBy('ASN').agg(F.avg('Throughput')).show()
data.where(F.col('Throughput')!=0.0).groupBy('ASN','Co Server').agg(F.avg('Throughput')).show()
data.where(F.col('Throughput')!=0.0).groupBy('Country','ASN').agg(F.avg('Throughput')).show()
data.where(F.col('Throughput')!=0.0).groupBy('Country','Co Server').agg(F.avg('Throughput')).show()
data.where(F.col('Throughput')!=0.0).groupBy('hour','Co Server').agg(F.avg('Throughput')).show()
data.where(F.col('Throughput')!=0.0).groupBy('hour','ASN').agg(F.avg('Throughput')).show()
