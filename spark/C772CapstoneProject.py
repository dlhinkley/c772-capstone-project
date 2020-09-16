# Databricks notebook source
# MAGIC %md
# MAGIC # Data Exploration

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data Exploration: Initialize Functions

# COMMAND ----------

# DBTITLE 1,Create Todo List
# Create Todo list
def init_todo():
  global todoList
  todoList = spark.createDataFrame(
      [
          ('Todo List', False),
      ],
      ['todo', 'finished']
  )

def add_todo(desc):
  global todoList
  newRow = spark.createDataFrame([(desc,False)])
  todoList = todoList.union(newRow)
  
def list_todo():
  global todoList
  display(todoList)

init_todo()


# COMMAND ----------

# DBTITLE 1,Create Function
from pyspark.sql.functions import col

def filter_default(dfIn, f1, f2):
  # Given a dataframe and two date field names, returns the dataframe removing records 
  # where the f1 or f2 columns equal a default date
  defaultDates = ["2999-01-01 00:00:00", "1900-01-01 00:00:00"]
  return dfIn.filter( ~col(f1).isin(defaultDates) & ~col(f2).isin(defaultDates) )


def date_stats(dfIn, f1, f2):
  # Given a dataframe and two date field names, returns a new dataframe with the difference between
  # the dates in minutes, hours and minutes
  dfOut = filter_default(dfIn, f1, f2)

  dfOut = dfOut.withColumn("minues", (col(f1).cast("long") - col(f2).cast("long"))/60.).select(f1, f2, "minues")

  dfOut = dfOut.withColumn("hours", (col(f1).cast("long") - col(f2).cast("long"))/3600.).select(f1, f2, "hours", "minues")

  return dfOut.withColumn("days", (col(f1).cast("long") - col(f2).cast("long"))/86400.).select("days", "hours", "minues")



# COMMAND ----------

# DBTITLE 1,Create Function
from pyspark.sql.functions import col, countDistinct, when

def perfect_cor(df, groupCol):
    """ Give a dataframe, and a column to group by, create a bar chart of all 
        perfectly correlated variables and return list of correlated variables
    """

    exCols = []

    # Count distinct values of rows where assignment_start_date is null
    dfCounts = df.groupBy(groupCol).agg(*(countDistinct( when(col(c).isNull(), "Empty").otherwise(col(c).cast("string") ) ).alias(c) for c in df.columns))

    # Filter fields to those with count of 1
    for row in dfCounts.collect():
      for c in dfCounts.columns:
        if (row[c] != 1):
          exCols.append(c)


    exCols  = list(set(exCols)) # Get unique list
    allCols = dfCounts.columns
    inCols  = [col for col in allCols if col not in exCols] # Return cols not in exCols

    return inCols;


# COMMAND ----------

# MAGIC %md
# MAGIC #### Data Exploration: Load Data

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

# MAGIC %md
# MAGIC #### Data Exploration: Summerize Data

# COMMAND ----------

# DBTITLE 1,Display Variable Descriptions
import pandas as pd

pd.set_option('display.max_colwidth', None)
dfPanda = dfDesc.toPandas()
dfPanda

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
# MAGIC ##### Null Values
# MAGIC - response_correctness
# MAGIC   - Investigate further
# MAGIC   - Could be null because the question wasn't answered or a different method of scoring the question
# MAGIC - item_type_code_name
# MAGIC   - Investigate further
# MAGIC   - Could be related to unstarted or unanswered questions
# MAGIC 
# MAGIC ##### Large number of categorical values
# MAGIC - item_type_code_name
# MAGIC   - Need to transform by reclassifying to reduce number of levels
# MAGIC   

# COMMAND ----------

# Create Todo list
add_todo('Investigate null values in response_correctness')
add_todo('Investigate null values in item_type_code_name')
add_todo('Reduce number of levels in item_type_code_name')


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
# MAGIC - possible outliers greaterthan 40
# MAGIC - investigate further
# MAGIC 
# MAGIC Right Skewed
# MAGIC - final_score_unweighted
# MAGIC - number_of_distinct_instance_items
# MAGIC - points_possible_unweighted
# MAGIC 
# MAGIC Binary Values (0/1)
# MAGIC - assignment_max_attempts
# MAGIC - assignment_attempt_number
# MAGIC - Appears binary but the variable name indicates it could have any values. The data only contains 1 and 0
# MAGIC - Investigate further

# COMMAND ----------

add_todo("Investigate number_of_learners > 40 outliers")
add_todo("Investigate binary variables assignment_attempt_number and assignment_max_attempts")

# COMMAND ----------

# DBTITLE 1,Null and Zero Numerical / Continuous Variables
from pyspark.sql.functions import count, when, col

for c in continousFields:
  print(c)
  dfRaw.agg(
    count(when(col(c).isNull(), c)).alias("null"),
    count(when(col(c) == 0, c)).alias("zero")
  ).show()

# COMMAND ----------

# MAGIC %md
# MAGIC - possibile correlation between assignment_attempt_number and assignment_max_attempts
# MAGIC   - both have 1566 zero values
# MAGIC   - Needs further investigation
# MAGIC - final_score_unweighted has 83,670 zero values
# MAGIC   - possibly because not yet scored
# MAGIC   - needs further investigation

# COMMAND ----------

add_todo('Investigate assignment_attempt_number and assignment_max_attempts both have 1566 values')
add_todo('Investigate final_score_unweighted has 83,670 zero values')

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
# MAGIC Default Date Values
# MAGIC - All variables have some dates have default values '2999-01-01 00:00:00' as max and '1900-01-01 00:00:00' as min
# MAGIC - These are substitutes for no value and will need to be replaced nulls
# MAGIC - Further investigation is needed as what the nulls mean

# COMMAND ----------

add_todo('Replace default dates with nulls')
add_todo('Investigate why some dates are null')

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

# DBTITLE 1,Binary Variables
# Categorical / Nominal Values
for f in binaryFields:
  dfRaw.groupBy(f).count().orderBy("count", ascending=False).show(50, False)
  

# COMMAND ----------

# MAGIC %md
# MAGIC Variables With Unary Values
# MAGIC - assignment_late_submission and is_deleted
# MAGIC - Variables will be removed

# COMMAND ----------

add_todo("Remove variables assignment_late_submission and is_deleted")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data Exploration: Explore Data

# COMMAND ----------

# MAGIC %md
# MAGIC - learner_attempt_status of "fully scored"
# MAGIC   - The tests with a final score
# MAGIC   - We will be analyzing only these scores
# MAGIC - item_type_code_name: 57,745 null values
# MAGIC - response_correctness: 61,047 null values

# COMMAND ----------

# DBTITLE 1,Cross-tabulation learner_attempt_status vs response_correctness
import pandas as pd

dfPd = dfRaw.toPandas()
# Return cross-tabulation table of learner_attempt_status vs response_correctness adding counts for null values
pd.crosstab(dfPd.learner_attempt_status.fillna('null'), dfPd.response_correctness.fillna('null'), margins=True, margins_name="Total")

# COMMAND ----------

# DBTITLE 1,Cross-tabulation learner_attempt_status vs assigned_item_status
# Return cross-tabulation table of learner_attempt_status vs assigned_item_status adding counts for null values
pd.crosstab(dfPd.learner_attempt_status.fillna('null'), dfPd.assigned_item_status.fillna('null'), margins=True, margins_name="Total")

# COMMAND ----------

# DBTITLE 1,Cross-tabulation learner_attempt_status vs scoring_type_code
# Return cross-tabulation table of learner_attempt_status vs scoring_type_code adding counts for null values
pd.crosstab(dfPd.learner_attempt_status.fillna('null'), dfPd.scoring_type_code.fillna('null'), margins=True, margins_name="Total")

# COMMAND ----------

# DBTITLE 1,Interval Correlations
from pyspark.sql.functions import unix_timestamp, col
import seaborn as sn
import matplotlib.pyplot as plt

dfPd =  filter_default(dfRaw).select(* (unix_timestamp(c).alias(c) for c in intervalFields) ).toPandas()

corrMatrix = dfPd.corr()
plt.figure(figsize=(10,12))
sn.heatmap(corrMatrix, annot=True)
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC Interval Correlation Results
# MAGIC - Perfectly correlated (1 or -1)
# MAGIC   - assignment_due_date and..
# MAGIC     - assignment_final_submission_date
# MAGIC     - assignment_start_date
# MAGIC     - Comments: Verify and possibly use only one
# MAGIC - Highly correlated (> .7)
# MAGIC   - max_student_start_datetime and min_student_stop_datetime
# MAGIC     - Explanation: quizes are short and ending always follows starting
# MAGIC   - scored_datetime and..
# MAGIC     - student_stop_datetime
# MAGIC     - was_fully_scored_datetime
# MAGIC     - was_submitted_datetime_actual
# MAGIC     - Explanation: actions closely follow stopping
# MAGIC   - student_start_datetime and..
# MAGIC     - student_stop_datetime
# MAGIC     - was_inprogress_datetime
# MAGIC     - Explanation: quizes are short and ending always follows starting
# MAGIC   - student_stop_datetime and..
# MAGIC     - scored_datetime
# MAGIC     - student_start_datetime
# MAGIC     - was_fully_scored_datetime
# MAGIC     - Explanation: quizes are short and ending always follows starting

# COMMAND ----------

# DBTITLE 1,Difference Between Dates
date_stats(dfRaw, "assignment_due_date", "assignment_final_submission_date").describe().show()


# COMMAND ----------

# MAGIC %md
# MAGIC # Data Cleaning

# COMMAND ----------

# DBTITLE 1,Create Clean Data Frame of Only "Fully Scored" 
dfClean = dfRaw.filter("learner_attempt_status = 'fully scored'")

# COMMAND ----------

# DBTITLE 1,Remove Unary Variables
# Remove from data frame
dfClean = dfClean.drop("is_deleted")
dfClean = dfClean.drop("assignment_late_submission")

# Remove from field list
binaryFields.remove("is_deleted") 
binaryFields.remove("assignment_late_submission") 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Convert Default / Empty Dates to NULL

# COMMAND ----------

# DBTITLE 1,Count Default / Empty Dates
from pyspark.sql.functions import count, col
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
# MAGIC ##### All default dates converted

# COMMAND ----------

# MAGIC %md
# MAGIC # Investigations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Is learner_attempt_status of "fully scored" perfectly correlated other variables

# COMMAND ----------

cols = perfect_cor(dfClean, "learner_attempt_status")
print("Correlated Vars", cols)

# COMMAND ----------

# MAGIC %md
# MAGIC - No

# COMMAND ----------

# MAGIC %md
# MAGIC ### Null values in response_correctness

# COMMAND ----------

# DBTITLE 1,Null values in response_correctness Perfect Correlations
rcDf = dfClean.filter(col("response_correctness").isNull() == True)
cols = perfect_cor(rcDf, "response_correctness")
print("Correlated Vars", cols)

# COMMAND ----------

# DBTITLE 1,View Null values in response_correctness Perfect Correlations
rcDf.select(*cols).show(1)

# COMMAND ----------

# MAGIC %md
# MAGIC Explanation Null values in response_correctness Perfect Correlations
# MAGIC - response_correctness is null when not scored (scoring_type_code = '[unassigned]')
# MAGIC - All correlated fields appear to be default values when not scorred

# COMMAND ----------

# MAGIC %md 
# MAGIC Are the following related?
# MAGIC - assignment_due_date, assignment_final_submission_date and assignment_start_date have 1566 empty dates
# MAGIC - assignment_attempt_number and assignment_max_attempts have 1566 records with the value 0
# MAGIC - there are 1566 records with the response_correctness "[unassigned]" 

# COMMAND ----------

# DBTITLE 1,Check relation
df = dfClean.filter(col("assignment_due_date").isNull() == True)
cols = perfect_cor(df, "assignment_due_date")

# Show the results on two lines
length = len(cols)
middle_index = length//2
first_half = cols[:middle_index]
second_half = cols[middle_index:]

df.select(*first_half).show(1)
df.select(*second_half).show(1)



# COMMAND ----------

# MAGIC %md
# MAGIC Yes they are all related
# MAGIC - only occurs in one organization
# MAGIC - could be record keeping differences
# MAGIC - EXCLUDE?

# COMMAND ----------

# MAGIC %md
# MAGIC ### Why Are Empty Dates Empty

# COMMAND ----------

# DBTITLE 1,Count Empty Dates
from pyspark.sql.functions import count, col
# Count Default / Empty Dates

for f in intervalFields:
  count = dfClean.filter(col(f).isNull() == True).count()
  print (f,"=", count)

# COMMAND ----------

# DBTITLE 1,scored_datetime Nulls
df = dfClean.filter(col("scored_datetime").isNull() == True)
cols = perfect_cor(df, "scored_datetime")
df.select(*cols).show(1)


# COMMAND ----------

# MAGIC %md
# MAGIC scored_datetime is null at times when fully scored

# COMMAND ----------

# DBTITLE 1,student_start_datetime & student_stop_datetime & was_fully_scored_datetime Nulls
import math

df = dfClean.filter(col("student_start_datetime").isNull() == True)
cols = perfect_cor(df, "student_start_datetime")

# Show the results on two lines

df.select(*cols[:5]).show(1)
df.select(*cols[5:9]).show(1)
df.select(*cols[9:]).show(1)

# COMMAND ----------

# MAGIC %md
# MAGIC - Null for one organization
# MAGIC - Could just be record keeping difference

# COMMAND ----------

# DBTITLE 1,was_in_progress_datetime Nulls
df = dfClean.filter(col("was_in_progress_datetime").isNull() == True)
cols = perfect_cor(df, "was_in_progress_datetime")
df.select(*cols).show(1)


# COMMAND ----------

# MAGIC %md
# MAGIC - Only when fully scored

# COMMAND ----------

# DBTITLE 1,was_submitted_datetime_actual Nulls
df = dfClean.filter(col("was_submitted_datetime_actual").isNull() == True)
cols = perfect_cor(df, "was_submitted_datetime_actual")
df.select(*cols).show(1)


# COMMAND ----------

# MAGIC %md
# MAGIC - Only when fully scored

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reclassify categories in item_type_code_name

# COMMAND ----------

# DBTITLE 1,Existing Categories
# Before Categories
dfClean.select("item_type_code_name").distinct().orderBy("item_type_code_name").show(50, False)


# COMMAND ----------

# MAGIC %md
# MAGIC Combine Suffix Levels
# MAGIC - The levels with the suffix Response (ex: FillinBlankResponse) is the same type of question as level without the suffix (ex: fillInTheBlank)

# COMMAND ----------

# Combine fillInTheBlank and FillinBlankResponse 
dfClean = dfClean.withColumn("item_type_code_name", when( col("item_type_code_name") == "FillinBlankResponse", "fillInTheBlank" ).otherwise(col("item_type_code_name")) )

# Combine multipleChoice and MultipleChoiceResponse 
dfClean = dfClean.withColumn("item_type_code_name", when( col("item_type_code_name") == "MultipleChoiceResponse", "multipleChoice" ).otherwise(col("item_type_code_name")) )


# COMMAND ----------

# DBTITLE 1,Display Frequency of Levels
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
# MAGIC We only want five levels, so convert everything below 6% to other

# COMMAND ----------

# DBTITLE 1,Convert Below 6% to Other
otherRows    = freqTable.filter("perc_of_count_total < 6")
otherLevels  = [row['item_type_code_name'] for row in otherRows.select("item_type_code_name").collect()]

dfClean = dfClean.withColumn("item_type_code_name", when( col("item_type_code_name").isin(otherLevels) | col("item_type_code_name").isNull() , "Other" ).otherwise(col("item_type_code_name")) )

# Display new values
dfClean.groupBy("item_type_code_name").count().orderBy("count", ascending=False).show(50, False)


# COMMAND ----------

# DBTITLE 1,Save As clean_data View
dfClean.createOrReplaceTempView("clean_data")

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Aggregation

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS answers_by_attempt;
# MAGIC CREATE TABLE answers_by_attempt AS (
# MAGIC     SELECT a.learner_assignment_attempt_id,
# MAGIC            count(a.assessment_item_response_id)   number_of_distinct_instance_items_answered
# MAGIC     FROM (
# MAGIC         SELECT DISTINCT learner_assignment_attempt_id, learner_assigned_item_attempt_id, assessment_item_response_id
# MAGIC         FROM clean_data
# MAGIC         WHERE learner_attempt_status = 'fully scored'
# MAGIC     ) a
# MAGIC     GROUP BY a.learner_assignment_attempt_id
# MAGIC )

# COMMAND ----------

spark.sql("SELECT * FROM answers_by_attempt").printSchema()


# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS scores;
# MAGIC CREATE TABLE scores AS (
# MAGIC    SELECT DISTINCT (cl.learner_assignment_attempt_id)  AS attempt_id,
# MAGIC                          cl.assessment_id,
# MAGIC                          cl.learner_id,
# MAGIC                          cl.section_id,
# MAGIC                          cl.org_id,
# MAGIC                          cl.final_score_unweighted,
# MAGIC                          cl.points_possible_unweighted,
# MAGIC                          cl.was_fully_scored_datetime,
# MAGIC                          cl.number_of_distinct_instance_items, -- number of questions
# MAGIC                          aba.number_of_distinct_instance_items_answered
# MAGIC          FROM clean_data cl
# MAGIC          LEFT JOIN answers_by_attempt aba ON cl.learner_assignment_attempt_id = aba.learner_assignment_attempt_id
# MAGIC          WHERE learner_attempt_status = 'fully scored'
# MAGIC );

# COMMAND ----------

spark.sql("SELECT * FROM scores").printSchema()


# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS learners;
# MAGIC CREATE TABLE learners AS (
# MAGIC          SELECT learner_id,
# MAGIC                 section_id,
# MAGIC                 org_id,
# MAGIC                 MIN( DATE(was_fully_scored_datetime) ) AS min_was_fully_scored_date,
# MAGIC                 MAX( DATE(was_fully_scored_datetime) ) AS max_was_fully_scored_date,
# MAGIC                 DATEDIFF( MAX(DATE(was_fully_scored_datetime)), MIN(DATE(was_fully_scored_datetime)) ) AS days
# MAGIC          FROM clean_data
# MAGIC          GROUP BY learner_id, section_id, org_id
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS sections;
# MAGIC CREATE TABLE sections AS (
# MAGIC          SELECT section_id,
# MAGIC                 MIN(DATE(was_fully_scored_datetime)) AS min_was_fully_scored_date,
# MAGIC                 MAX(DATE(was_fully_scored_datetime)) AS max_was_fully_scored_date,
# MAGIC                 DATEDIFF( MAX(DATE(was_fully_scored_datetime)), MIN(DATE(was_fully_scored_datetime)) ) AS days
# MAGIC          FROM clean_data
# MAGIC          GROUP BY section_id
# MAGIC      );

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS assessments;
# MAGIC CREATE TABLE assessments AS (
# MAGIC          SELECT assessment_id,
# MAGIC                 MIN(DATE(was_fully_scored_datetime)) AS min_was_fully_scored_date,
# MAGIC                 MAX(DATE(was_fully_scored_datetime)) AS max_was_fully_scored_date,
# MAGIC                 DATEDIFF( MAX(DATE(was_fully_scored_datetime)), MIN(DATE(was_fully_scored_datetime)) ) AS days
# MAGIC          FROM clean_data
# MAGIC          GROUP BY assessment_id
# MAGIC          )

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS orgs;
# MAGIC CREATE TABLE orgs AS (
# MAGIC          SELECT org_id,
# MAGIC                 MIN(DATE(was_fully_scored_datetime)) AS min_was_fully_scored_date,
# MAGIC                 MAX(DATE(was_fully_scored_datetime)) AS max_was_fully_scored_date,
# MAGIC                 DATEDIFF( MAX(DATE(was_fully_scored_datetime)), MIN(DATE(was_fully_scored_datetime)) ) AS days
# MAGIC          FROM clean_data
# MAGIC          GROUP BY org_id
# MAGIC      )

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS score_by_learner;
# MAGIC CREATE TABLE score_by_learner AS ( -- How learners performed on all assessments attempts
# MAGIC          SELECT l.learner_id, --1126
# MAGIC                 ROUND(AVG(s.final_score_unweighted)) AS learner_final_score_unweighted,
# MAGIC                 ROUND(AVG(s.points_possible_unweighted)) AS learner_points_possible_unweighted,
# MAGIC                 ROUND(AVG(s.number_of_distinct_instance_items_answered)) AS learner_number_of_distinct_instance_items_answered,
# MAGIC                 ROUND(AVG(s.number_of_distinct_instance_items)) AS learner_number_of_distinct_instance_items,
# MAGIC                 COUNT(*) AS learner_num_attempts,
# MAGIC                 ROUND(AVG(l.days)) AS learner_days -- The number of days the learner took assessments
# MAGIC 
# MAGIC          FROM learners l,
# MAGIC               scores s
# MAGIC          WHERE l.learner_id = s.learner_id
# MAGIC          GROUP BY l.learner_id
# MAGIC      )

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS score_by_assessment;
# MAGIC CREATE TABLE score_by_assessment  AS ( -- How all learners performed on attempts of an assessment
# MAGIC          SELECT a.assessment_id, -- 329
# MAGIC                 ROUND(AVG(s.final_score_unweighted)) AS assessment_final_score_unweighted,
# MAGIC                 ROUND(AVG(s.points_possible_unweighted)) AS assessment_points_possible_unweighted,
# MAGIC                 ROUND(AVG(s.number_of_distinct_instance_items_answered)) AS assessment_number_of_distinct_instance_items_answered,
# MAGIC                 ROUND(AVG(s.number_of_distinct_instance_items)) AS assessment_number_of_distinct_instance_items,
# MAGIC                 COUNT(*) AS assessment_num_attempts
# MAGIC          FROM assessments a,
# MAGIC               scores s
# MAGIC          WHERE a.assessment_id = s.assessment_id
# MAGIC          GROUP BY a.assessment_id
# MAGIC      )

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS score_by_section;
# MAGIC CREATE TABLE score_by_section  AS ( -- How all learners performed on attempts of an assessment by section
# MAGIC          SELECT a.section_id, --490
# MAGIC                 s.assessment_id,
# MAGIC                 ROUND(AVG(s.final_score_unweighted)) AS section_final_score_unweighted,
# MAGIC                 ROUND(AVG(s.points_possible_unweighted)) AS section_points_possible_unweighted,
# MAGIC                 ROUND(AVG(s.number_of_distinct_instance_items_answered)) AS section_number_of_distinct_instance_items_answered,
# MAGIC                 ROUND(AVG(s.number_of_distinct_instance_items)) AS section_number_of_distinct_instance_items,
# MAGIC                 COUNT(*) AS section_num_attempts,
# MAGIC                 ROUND(AVG(a.days)) AS section_days
# MAGIC          FROM sections a,
# MAGIC               scores s
# MAGIC          WHERE a.section_id = s.section_id
# MAGIC          GROUP BY a.section_id, s.assessment_id
# MAGIC      )

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS score_by_org;
# MAGIC CREATE TABLE score_by_org  AS (
# MAGIC          SELECT a.org_id, --329
# MAGIC                 s.assessment_id,
# MAGIC                 ROUND(AVG(s.final_score_unweighted)) AS organization_final_score_unweighted,
# MAGIC                 ROUND(AVG(s.points_possible_unweighted)) AS organization_points_possible_unweighted,
# MAGIC                 ROUND(AVG(s.number_of_distinct_instance_items_answered)) AS organization_number_of_distinct_instance_items_answered,
# MAGIC                 ROUND(AVG(s.number_of_distinct_instance_items)) AS organization_number_of_distinct_instance_items,
# MAGIC                 COUNT(*) AS organization_num_attempts,
# MAGIC                 ROUND(AVG(a.days)) AS organization_days
# MAGIC          FROM orgs a,
# MAGIC               scores s
# MAGIC          WHERE a.org_id = s.org_id
# MAGIC          GROUP BY a.org_id, s.assessment_id
# MAGIC      )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT attempt_id,
# MAGIC        assessment_id,
# MAGIC        learner_id,
# MAGIC        section_id,
# MAGIC        org_id,
# MAGIC        was_fully_scored_datetime,
# MAGIC        final_score_unweighted,
# MAGIC        points_possible_unweighted,
# MAGIC        number_of_distinct_instance_items,
# MAGIC        number_of_distinct_instance_items_answered
# MAGIC FROM scores s LIMIT 10;

# COMMAND ----------

intervalFields


# COMMAND ----------

spark.sql("SELECT * FROM clean_data LIMiT 20").printSchema()

# COMMAND ----------

# DBTITLE 1,How does assessment instance and assignment start date relate
# MAGIC %sql
# MAGIC select assessment_instance_id, 
# MAGIC  section_id,
# MAGIC  assessment_instance_attempt_id,
# MAGIC  assignment_start_date
# MAGIC FROM clean_data
# MAGIC WHERE assessment_instance_attempt_id IS NOT NULL
# MAGIC ORDER BY assessment_instance_id, assessment_instance_attempt_id, student_start_datetime LIMIT 100
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC The assignment start date is the same for every assessment_instance_id.  Looks like it's assigned with the instance is created

# COMMAND ----------

# DBTITLE 1,See if any null assignment_start_dates
# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM clean_data WHERE assignment_start_date IS NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC Hmm... there's that 1566 again.  Let's look at those records

# COMMAND ----------

dfDesc.select('field').show(40, False)

# COMMAND ----------


