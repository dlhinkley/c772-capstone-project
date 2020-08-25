# Databricks notebook source
# DBTITLE 1,Load Assessment Items Data
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

# DBTITLE 1,Save as Database View
# Save as database view
dfRaw.createOrReplaceTempView("raw_data")
dfRaw.cache()


# COMMAND ----------

# DBTITLE 1,Save Continous Describe Data
# Describe data
desc = dfRaw.describe()

# COMMAND ----------

# DBTITLE 1,Save Variable Categories
from pyspark.sql import functions as F
# Save field names
identifierFields = dfDesc.filter("type = 'Categorical Identifier'").select("field")
nominalFields    = dfDesc.filter("type = 'Categorical Nominal'").select("field")
continousFields  = dfDesc.filter("type = 'Numeric Continous'").select("field")
intervalFields   = dfDesc.filter("type = 'Categorical Interval'").select("field")
binaryFields     = dfDesc.filter("type = 'Categorical Binary'").select("field")

# COMMAND ----------

#continousFields.show(40,False)
desc.select("summary",
            "assignment_attempt_number",
            "assignment_max_attempts" ,
            "final_score_unweighted",
            "number_of_distinct_instance_items",
            "number_of_learners",
            "points_possible_unweighted" ).show(5,False)

# COMMAND ----------

#nominalFields.show(40,False)
desc.select("summary",
            "assigned_item_status",
            "ced_assignment_type_code" ,
            "item_type_code_name",
            "learner_attempt_status",
            "response_correctness",
            "scoring_type_code" ).show(5,False)

# COMMAND ----------

# DBTITLE 1,Number of Records
# Display number of records
sqlContext.sql("SELECT is_deleted FROM raw_data").show(1)

# COMMAND ----------

# DBTITLE 1,Display Data Schema
# Schema
dfRaw.printSchema()

# COMMAND ----------

# DBTITLE 1,Identifier Data Summary
# Identifier Data Summary
from pyspark.sql.functions import countDistinct
from functools import partial
from pyspark.sql import Row

# Convert columns to rows
def flatten_table(column_names, column_values):
    row = zip(column_names, column_values)
    _, key = next(row)  # Special casing retrieving the first column
    return [
        Row(Variable=column, Observations=value)
        for column, value in row
    ]

# Get the observation count with each type of record
counts = dfRaw.agg( 
  countDistinct("assessment_sk").alias("assessment_sk"), 
  countDistinct("assessment_instance_sk").alias("assessment_instance_sk"), 
  countDistinct("learner_assignment_attempt_sk").alias("learner_assignment_attempt_sk"), 
  countDistinct("assessment_instance_attempt_sk").alias("assessment_instance_attempt_sk"),  
  countDistinct("learner_assigned_item_attempt_sk").alias("learner_assigned_item_attempt_sk"), 
  countDistinct("assessment_item_response_sk").alias("assessment_item_response_sk"), 
  countDistinct("learner_sk").alias("learner_sk"), 
  countDistinct("section_sk").alias("section_sk"), 
  countDistinct("org_sk").alias("org_sk")
)
# Display the counts
counts.rdd.flatMap(partial(flatten_table, counts.columns)).toDF().sort('Variable').show(9, False)

# COMMAND ----------

# DBTITLE 1,Deleted Records Count
# Deleted records count
sqlContext.sql("SELECT count(distinct is_deleted) FROM raw_data WHERE is_deleted = true").show()


# COMMAND ----------

dfRaw.summary()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM raw_data where is_deleted;

# COMMAND ----------


