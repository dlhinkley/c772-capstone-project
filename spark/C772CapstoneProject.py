# Databricks notebook source
# MAGIC %md
# MAGIC # Data Exploration

# COMMAND ----------

# DBTITLE 1,Load Assessment Items Data
# Load Assessment Items Data
from pyspark import SparkFiles

url = 'https://github.com/dlhinkley/c772-capstone-project/raw/master/data/assessment_items.csv'

spark.sparkContext.addFile(url)

file = "file://" + SparkFiles.get("assessment_items.csv")

dfRaw = spark.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(file)

# COMMAND ----------

# DBTITLE 1,Load Assessment Variable Descriptions
from pyspark import SparkFiles

url = 'https://github.com/dlhinkley/c772-capstone-project/raw/master/data/descriptions.csv'

spark.sparkContext.addFile(url)

file = "file://" + SparkFiles.get("descriptions.csv")

dfDesc = spark.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(file)

# COMMAND ----------

# DBTITLE 1,Display Variable Categories
from pyspark.sql.functions import col

# Save field names
identifierFieldRows = dfDesc.filter("type = 'Categorical Identifier'")
identifierFields = [row['field'] for row in identifierFieldRows.select("field").collect()]
identifierFieldRows.select(col("field").alias('Categorical Identifier')).show(20,False)

nominalFieldRows    = dfDesc.filter("type = 'Categorical Nominal'")
nominalFields = [row['field'] for row in nominalFieldRows.select("field").collect()]
nominalFieldRows.select(col("field").alias('Categorical Nominal')).show(20,False)

continousFieldRows  = dfDesc.filter("type = 'Numeric Continuous'")
continousFields = [row['field'] for row in continousFieldRows.select("field").collect()]
continousFieldRows.select(col("field").alias('Numeric Continuous')).show(20,False)

intervalFieldRows   = dfDesc.filter("type = 'Categorical Interval'")
intervalFields = [row['field'] for row in intervalFieldRows.select("field").collect()]
intervalFieldRows.select(col("field").alias('Categorical Interval')).show(20,False)

binaryFieldRows     = dfDesc.filter("type = 'Categorical Binary'")
binaryFields = [row['field'] for row in binaryFieldRows.select("field").collect()]
binaryFieldRows.select(col("field").alias('Categorical Binary')).show(20,False)


# COMMAND ----------

# DBTITLE 1,Set Date Fields to Timestamp Type
# Set empty dates to null
from pyspark.sql.functions import col
from pyspark.sql.types import TimestampType

for f in intervalFields:
  dfRaw = dfRaw.withColumn(f, col(f).cast(TimestampType() ) )


# COMMAND ----------

# DBTITLE 1,Display Data Schema
# Schema
dfRaw.printSchema()

# COMMAND ----------

# DBTITLE 1,Save as Database View
# Save as database view
dfRaw.createOrReplaceTempView("raw_data")


# COMMAND ----------

# DBTITLE 1,Categorical / Identifier Variables
from pyspark.sql.functions import when, count, col, countDistinct

for f in identifierFields:
  print(f)
  dfRaw.agg(
    countDistinct(f).alias("unique"), 
    count(when(col(f).isNull(), f)).alias("null")
  ).show()


# COMMAND ----------

# DBTITLE 1,Categorical / Nominal Variables
# Categorical / Nominal Values
for f in nominalFields:
  print(f)
  dfRaw.cube(f).count().sort("count", ascending=False).show()

# COMMAND ----------

# DBTITLE 1,Numerical / Continuous Variables
# Numerical / Continuous Variables
desc = dfRaw.describe()
for f in continousFields:
  print(f)
  desc.select("summary", f).show(5,False)


# COMMAND ----------

# DBTITLE 1,Numerical / Continuous Histograms
for f in continousFields:
  print(f)
  dfRaw.select(f).toPandas().hist()

# COMMAND ----------

# DBTITLE 1,Null Numerical / Continuous Variables
from pyspark.sql.functions import count, when, col

for c in continousFields:
  print(c)
  dfRaw.agg(
    count(when(col(c).isNull(), c)).alias("null")
  ).show()

# COMMAND ----------

dfRaw.select('assignment_due_date').summary().show()
#intervalFields

# COMMAND ----------

# DBTITLE 1,Categorical / Interval Variables
# Categorical / Interval Variables
sqlIn = "IN('2999-01-01 00:00:00', '1900-01-01 00:00:00')"

for f in intervalFields:
  print(f)
  sqlContext.sql("SELECT \
                 (SELECT COUNT(*) FROM raw_data WHERE " + f + " " + sqlIn + " ) AS null, \
                 (SELECT MIN(" + f + ") FROM raw_data WHERE " + f + " NOT " + sqlIn + ") AS min, \
                 (SELECT MAX(" + f + ") FROM raw_data WHERE " + f + " NOT " + sqlIn + ") AS max \
                 ").show(1, False)


# COMMAND ----------

# DBTITLE 1,Deleted Records Count
# Deleted records count
sqlContext.sql("SELECT count(distinct is_deleted) FROM raw_data WHERE is_deleted = true").show()


# COMMAND ----------

# MAGIC %md
# MAGIC # Data Cleaning

# COMMAND ----------

# DBTITLE 1,Create Clean Data Frame
dfClean = dfRaw

# COMMAND ----------

# MAGIC %md
# MAGIC ### Convert Default / Empty Dates to NULL

# COMMAND ----------

# DBTITLE 1,Count Default / Empty Dates
# Count Default / Empty Dates

for f in intervalFields:
  row = dfClean.filter( (col(f) == '2999-01-01 00:00:00') | (col(f) == '1900-01-01 00:00:00') ).select(count(f).alias("null")).collect()
  found = row[0][0]
  if (found > 0):
    print (f,"=", found)

# COMMAND ----------

# DBTITLE 1,Set Default / Empty Dates To NULL
# Set empty dates to null
from pyspark.sql.functions import when, col

for f in intervalFields:
  dfClean = dfClean.withColumn(f, when((col(f) == '2999-01-01 00:00:00') | (col(f) == '1900-01-01 00:00:00'), None ).otherwise( col(f) ) )


# COMMAND ----------

# DBTITLE 1,Confirm Default / Empty Dates Removed
# Count Default / Empty Dates
exists = 0
for f in intervalFields:
  row = dfClean.filter( (col(f) == '2999-01-01 00:00:00') | (col(f) == '1900-01-01 00:00:00') ).select(count(f).alias("null")).collect()
  found = row[0][0]
  if (found > 0):
    exists += 1
    print (f,"=", found)
    
if (exists == 0):
  print ("None Found")
else:
  print ("Found", exists)

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Conversion

# COMMAND ----------

# MAGIC %sql
