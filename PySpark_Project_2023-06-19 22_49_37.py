# Databricks notebook source
#Creating Databricks PySpark Project using Keggle Data (CSV)

# COMMAND ----------

# DBTITLE 1,Importing Library
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType, IntegerType
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Creating a DataFrame
df = spark.read.load('/FileStore/tables/googleplaystore.csv', format='csv', sep=',', header='true', escape = '"', inferschema = 'true')

# COMMAND ----------

df.count()

# COMMAND ----------

df.show(1)

# COMMAND ----------

# DBTITLE 1,Checking Schema
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Cleaning Data
df=df.drop("size","Content Rating","Last Updated", "Android Ver", "Current ver")

# COMMAND ----------

df.show(2)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df=df.withColumn("Reviews", col("Reviews").cast(IntegerType()))\
.withColumn("Installs", regexp_replace(col("Installs"),"[^0-9]", ""))\
.withColumn("Installs", col("Installs").cast(IntegerType()))\
.withColumn("Price", regexp_replace(col("Price"),"[$]", ""))\
.withColumn("Price", col("Price").cast(IntegerType()))


# COMMAND ----------

df.show(2)

# COMMAND ----------

df.createOrReplaceTempView("apps")

# COMMAND ----------

# MAGIC %sql select * from apps

# COMMAND ----------

# MAGIC %sql select * from apps
# MAGIC

# COMMAND ----------

# DBTITLE 1,Top Reviews Given to the Apps
# MAGIC %sql select App, sum(Reviews) from apps
# MAGIC group by 1
# MAGIC order by 2 desc

# COMMAND ----------

# DBTITLE 1,Top 10 installs app
# MAGIC %sql select App, Type, sum(Installs) from apps
# MAGIC group by 1,2
# MAGIC order by 3 desc

# COMMAND ----------

# DBTITLE 1,Distribution by Category
# MAGIC %sql select Category, sum(installs) from apps
# MAGIC group by 1
# MAGIC order by 2 desc

# COMMAND ----------

# DBTITLE 1,Top Paid Apps
# MAGIC %sql select App, sum (Price) from Apps
# MAGIC where Type = "Paid"
# MAGIC group by 1
# MAGIC order by 2 desc

# COMMAND ----------


