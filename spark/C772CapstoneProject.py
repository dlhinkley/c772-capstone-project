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

# DBTITLE 1,Change Date Fields from String to Timestamp Type
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
  dfRaw.groupBy(f).count().orderBy("count", ascending=False).show(50, False)
  

# COMMAND ----------

# MAGIC %md
# MAGIC #### Null Values
# MAGIC - response_correctness
# MAGIC   - Investigate further
# MAGIC   - Could be null because the question wasn't answered or a different method of scoring the question
# MAGIC - item_type_code_name
# MAGIC   - Investigate further
# MAGIC   - Could be related to unstarted or unanswered questions
# MAGIC #### large number of categorical values
# MAGIC - item_type_code_name
# MAGIC   - Need to transform by reclassifying to reduce number of categories
# MAGIC   

# COMMAND ----------

# DBTITLE 1,Numerical / Continuous Variables
# Numerical / Continuous Variables
desc = dfRaw.describe()
for f in continousFields:
  desc.select("summary", f).show(5,False)


# COMMAND ----------

# DBTITLE 1,Numerical / Continuous Histograms
for f in continousFields:
  print(f)
  dfRaw.select(f).toPandas().hist()

# COMMAND ----------

# MAGIC %md
# MAGIC Normal Distribution
# MAGIC - number_of_learners
# MAGIC 
# MAGIC Right Skewed
# MAGIC - final_score_unweighted
# MAGIC - number_of_distinct_instance_items
# MAGIC - points_possible_unweighted
# MAGIC 
# MAGIC Two Values (0/1)
# MAGIC - assignment_max_attempts
# MAGIC - assignment_attempt_number
# MAGIC - Appears binary but the variable name indicates it could have any values. The data only contains 1 and 0

# COMMAND ----------

# DBTITLE 1,Null Numerical / Continuous Variables
from pyspark.sql.functions import count, when, col

for c in continousFields:
  print(c)
  dfRaw.agg(
    count(when(col(c).isNull(), c)).alias("null")
  ).show()

# COMMAND ----------

# DBTITLE 1,Categorical / Interval Variables
from pyspark.sql.functions import countDistinct, count, when, col, min, max

for f in intervalFields:
  print (f)
  dfRaw.agg(
    countDistinct(f).alias("unique"), 
    count(when(col(f).isNull(), f)).alias("null"),
    min(f).alias("min"),
    max(f).alias("max")
 ).show(1, False)


# COMMAND ----------

# MAGIC %md
# MAGIC Some dates have default values '2999-01-01 00:00:00' as max and '1900-01-01 00:00:00' as min
# MAGIC These are substitutes for no value and will need to be replaced nulls

# COMMAND ----------

# DBTITLE 1,Categorical / Interval Variables (exclude defaults)
from pyspark.sql.functions import countDistinct, count, when, col, min, max

# Categorical / Interval Variables
defaults = ["2999-01-01 00:00:00","1900-01-01 00:00:00"]

for f in intervalFields:
  print (f)
  dfRaw.agg(
    countDistinct(f).alias("unique"), 
    count( when(col(f).isNull(), f)).alias("null"),
    min( when(col(f).isin(defaults) == False, col(f) )).alias("min"),
    max( when(col(f).isin(defaults) == False, col(f) )).alias("max")
 ).show(1, False)

# COMMAND ----------

# MAGIC %md
# MAGIC Dates fall in range of a school year of 8/2019 to 5/2020

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
# MAGIC All default dates converted

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reclassify categories in item_type_code_name

# COMMAND ----------

# DBTITLE 1,Existing Categories
# Before Categories
catResults = dfClean.filter(col("item_type_code_name").isNull() == False).groupBy("item_type_code_name").count().orderBy("count", ascending=False).toPandas()
catResults.plot.bar(x='item_type_code_name', y='count')

# COMMAND ----------

from pyspark.sql.functions import col, round

tot = dfClean.filter(col("item_type_code_name").isNull() == False).count()

freqTable = dfClean.groupBy("item_type_code_name") \
               .count() \
               .withColumnRenamed('count', 'cnt_per_group') \
               .withColumn('perc_of_count_total', ( col('cnt_per_group') / tot) * 100 ) \
               .orderBy("cnt_per_group", ascending=False)

freqTable.show(50, False)

# COMMAND ----------

# MAGIC %md
# MAGIC Reduce dimensionality by using thresholding.  
# MAGIC Use the cuttoff of 3%, categorize levels with 3% or less as other

# COMMAND ----------

otherRows    = freqTable.filter("perc_of_count_total < 3")
otherFields = [row['item_type_code_name'] for row in otherRows.select("item_type_code_name").collect()]


dfClean = dfClean.withColumn("item_type_code_name", when( col("item_type_code_name").isin(otherFields), "Other" ).otherwise(col("item_type_code_name")) )

# Display new values
dfClean.groupBy("item_type_code_name").count().orderBy("count", ascending=False).show(50, False)


# COMMAND ----------




# COMMAND ----------

# MAGIC %md
# MAGIC # Data Conversion

# COMMAND ----------

# MAGIC %sql
