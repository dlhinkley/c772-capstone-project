# Databricks notebook source
# MAGIC %md
# MAGIC # Data Exploration

# COMMAND ----------

# DBTITLE 1,Define Functions
def cramers_v(confusion_matrix):
    """ calculate Cramers V statistic for categorial-categorial association.
        uses correction from Bergsma and Wicher,
        Journal of the Korean Statistical Society 42 (2013): 323-328
    """
    chi2 = ss.chi2_contingency(confusion_matrix)[0]
    n = confusion_matrix.sum()
    phi2 = chi2 / n
    r, k = confusion_matrix.shape
    phi2corr = max(0, phi2 - ((k-1)*(r-1))/(n-1))
    rcorr = r - ((r-1)**2)/(n-1)
    kcorr = k - ((k-1)**2)/(n-1)
    return np.sqrt(phi2corr / min((kcorr-1), (rcorr-1)))

# COMMAND ----------

# DBTITLE 1,Create Function
from pyspark.sql.functions import col, countDistinct

def relate_bar_chart(df, groupCol):
    """ Give a dataframe, and a column to group by, create a bar chart of all 
        variables that don't change as the group by changes
    """

    exCols = []

    # Count distinct values of rows where assignment_start_date is null
    dfCounts = df.groupBy(groupCol).agg(*(countDistinct(col(c)).alias("n_" + c) for c in df.columns))

    # Filter fields to those with count of 1
    for row in dfCounts.collect():
      for c in dfCounts.columns:
        if (row[c] != 1):
          exCols.append(c)


    exCols  = list(set(exCols)) # Get unique list
    exCols.append("n_" + groupCol) # Exclude the column we group by
    allCols = dfCounts.columns
    inCols  = [col for col in allCols if col not in exCols] # Return cols not in exCols


    dfCounts.select(*inCols, groupCol).toPandas().plot.bar(x=groupCol, figsize=(7,7))


# COMMAND ----------

# DBTITLE 1,Load Assessment Items Data
# Load Assessment Items Data
from pyspark import SparkFiles

url = 'https://github.com/dlhinkley/c772-capstone-project/raw/master/data/assessment_items.csv'

spark.sparkContext.addFile(url)

file = "file://" + SparkFiles.get("assessment_items.csv")

dfRaw = spark.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(file)

# COMMAND ----------

# DBTITLE 1,Count Deleted Observations
from pyspark.sql.functions import countDistinct, count, when, col, min, max

dfRaw.agg(
  count( when(col("is_deleted") == True, "is_deleted")).alias("num_deleted")
).show(1, False)

# COMMAND ----------

# MAGIC %md
# MAGIC Zero observations deleted.  This variable can be deleted

# COMMAND ----------

# DBTITLE 1,Remove Unused is_deleted Variable
dfRaw = dfRaw.drop("is_deleted")

# COMMAND ----------

# DBTITLE 1,Load Assessment Variable Descriptions
from pyspark import SparkFiles

url = 'https://github.com/dlhinkley/c772-capstone-project/raw/master/data/descriptions.csv'

spark.sparkContext.addFile(url)

file = "file://" + SparkFiles.get("descriptions.csv")

dfDesc = spark.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(file)

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
# MAGIC - learner_attempt_status of "fully scored"
# MAGIC   - The tests with a final score
# MAGIC   - We will be analyzing only these scores
# MAGIC - item_type_code_name: 57,745 null values
# MAGIC - response_correctness: 61,047 null values

# COMMAND ----------

# MAGIC %md
# MAGIC #### Null Values
# MAGIC - response_correctness
# MAGIC   - Investigate further
# MAGIC   - Could be null because the question wasn't answered or a different method of scoring the question
# MAGIC - item_type_code_name
# MAGIC   - Investigate further
# MAGIC   - Could be related to unstarted or unanswered questions
# MAGIC 
# MAGIC #### Large number of categorical values
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

# MAGIC %md
# MAGIC # Data Cleaning

# COMMAND ----------

# DBTITLE 1,Create Clean Data Frame of Only "Fully Scored" 
dfClean = dfRaw.filter("learner_attempt_status = 'fully scored'")

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
# MAGIC ### Does learner_attempt_status of "fully scored" relate to other variables

# COMMAND ----------

relate_bar_chart(dfClean, "learner_attempt_status")

# COMMAND ----------

# MAGIC %md
# MAGIC - The only column that is always the same value when "fully scored" is assignment_late_submission
# MAGIC - What is that value?

# COMMAND ----------

dfClean.where(col("learner_attempt_status") == "fully scored").select('assignment_late_submission').distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC - The value is false
# MAGIC - When learner_attempt_status is "fully scored", the assigment is never submitted late

# COMMAND ----------

# MAGIC %md
# MAGIC ### Why Are Empty Dates Empty

# COMMAND ----------

# DBTITLE 1,Save Dataframe as View
# Save as database view
dfClean.createOrReplaceTempView("clean_data")

# COMMAND ----------

# DBTITLE 1,Count Empty Dates
from pyspark.sql.functions import count, col
# Count Default / Empty Dates

for f in intervalFields:
  count = dfClean.filter(col(f).isNull() == True).count()
  print (f,"=", count)

# COMMAND ----------

# MAGIC %md 
# MAGIC Are the following related?
# MAGIC - assignment_due_date, assignment_final_submission_date and assignment_start_date have 1566 empty dates
# MAGIC - assignment_attempt_number and assignment_max_attempts have 1566 records with the value 0
# MAGIC - there are 1566 records with the response_correctness "[unassigned]" 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Count the records
# MAGIC SELECT count(*)
# MAGIC FROM clean_data
# MAGIC WHERE assignment_due_date IS NULL 
# MAGIC   AND assignment_final_submission_date IS NULL
# MAGIC   AND assignment_start_date IS NULL
# MAGIC   AND assignment_attempt_number = 0 
# MAGIC   AND assignment_max_attempts = 0
# MAGIC   AND response_correctness = '[unassigned]';

# COMMAND ----------

# MAGIC %md
# MAGIC Yes they are releated
# MAGIC - The same records all have 1566 empty

# COMMAND ----------

# MAGIC %md
# MAGIC Does the same 1566 also include these
# MAGIC - student_start_datetime = 749
# MAGIC - student_stop_datetime = 749

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Count the records
# MAGIC SELECT count(*)
# MAGIC FROM clean_data
# MAGIC WHERE student_start_datetime IS NULL
# MAGIC   AND student_stop_datetime IS NULL
# MAGIC   AND assignment_due_date IS NULL 
# MAGIC   AND assignment_final_submission_date IS NULL
# MAGIC   AND assignment_start_date IS NULL
# MAGIC   AND assignment_attempt_number = 0 
# MAGIC   AND assignment_max_attempts = 0
# MAGIC   AND response_correctness = '[unassigned]';

# COMMAND ----------

# MAGIC %md
# MAGIC They do not

# COMMAND ----------

# MAGIC %md
# MAGIC Does the same 1566 also include these
# MAGIC - was_fully_scored_datetime = 750

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Count the records
# MAGIC SELECT count(*)
# MAGIC FROM clean_data
# MAGIC WHERE was_fully_scored_datetime IS NULL
# MAGIC   AND assignment_due_date IS NULL 
# MAGIC   AND assignment_final_submission_date IS NULL
# MAGIC   AND assignment_start_date IS NULL
# MAGIC   AND assignment_attempt_number = 0 
# MAGIC   AND assignment_max_attempts = 0
# MAGIC   AND response_correctness = '[unassigned]';

# COMMAND ----------

df = dfClean.where(col("assignment_start_date").isNull())
relate_bar_chart(df, "ced_assignment_type_code")

# COMMAND ----------

# MAGIC %md
# MAGIC They do not

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
# MAGIC We only want four levels, so convert everything below 6% to other

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
# MAGIC            count(a.assessment_item_response_id)   num_questions_answered
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
# MAGIC                          cl.final_score_unweighted AS num_final_score,
# MAGIC                          cl.points_possible_unweighted AS num_possible_score,
# MAGIC                          DATE(was_fully_scored_datetime) AS scored_date,
# MAGIC                          cl.number_of_distinct_instance_items AS num_questions, -- number of questions
# MAGIC                          aba.num_questions_answered
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
# MAGIC                 MIN( DATE(was_fully_scored_datetime) ) AS min_scored_date,
# MAGIC                 MAX( DATE(was_fully_scored_datetime) ) AS max_scored_date,
# MAGIC                 DATEDIFF( MAX(DATE(was_fully_scored_datetime)), MIN(DATE(was_fully_scored_datetime)) ) AS days
# MAGIC          FROM clean_data
# MAGIC          GROUP BY learner_id, section_id, org_id
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS sections;
# MAGIC CREATE TABLE sections AS (
# MAGIC          SELECT section_id,
# MAGIC                 MIN(DATE(was_fully_scored_datetime)) AS min_scored_date,
# MAGIC                 MAX(DATE(was_fully_scored_datetime)) AS max_scored_date,
# MAGIC                 DATEDIFF( MAX(DATE(was_fully_scored_datetime)), MIN(DATE(was_fully_scored_datetime)) ) AS days
# MAGIC          FROM clean_data
# MAGIC          GROUP BY section_id
# MAGIC      );

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS assessments;
# MAGIC CREATE TABLE assessments AS (
# MAGIC          SELECT assessment_id,
# MAGIC                 MIN(DATE(was_fully_scored_datetime)) AS min_scored_date,
# MAGIC                 MAX(DATE(was_fully_scored_datetime)) AS max_scored_date,
# MAGIC                 DATEDIFF( MAX(DATE(was_fully_scored_datetime)), MIN(DATE(was_fully_scored_datetime)) ) AS days
# MAGIC          FROM clean_data
# MAGIC          GROUP BY assessment_id
# MAGIC          )

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS orgs;
# MAGIC CREATE TABLE orgs AS (
# MAGIC          SELECT org_id,
# MAGIC                 MIN(DATE(was_fully_scored_datetime)) AS min_scored_date,
# MAGIC                 MAX(DATE(was_fully_scored_datetime)) AS max_scored_date,
# MAGIC                 DATEDIFF( MAX(DATE(was_fully_scored_datetime)), MIN(DATE(was_fully_scored_datetime)) ) AS days
# MAGIC          FROM clean_data
# MAGIC          GROUP BY org_id
# MAGIC      )

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS score_by_learner;
# MAGIC CREATE TABLE score_by_learner AS ( -- How learners performed on all assessments attempts
# MAGIC          SELECT l.learner_id, --1126
# MAGIC                 ROUND(AVG(s.num_final_score)) AS learner_num_final_score,
# MAGIC                 ROUND(AVG(s.num_possible_score)) AS learner_num_possible_score,
# MAGIC                 ROUND(AVG(s.num_questions_answered)) AS learner_num_questions_answered,
# MAGIC                 ROUND(AVG(s.num_questions)) AS learner_num_questions,
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
# MAGIC                 ROUND(AVG(s.num_final_score)) AS assessment_num_final_score,
# MAGIC                 ROUND(AVG(s.num_possible_score)) AS assessment_num_possible_score,
# MAGIC                 ROUND(AVG(s.num_questions_answered)) AS assessment_num_questions_answered,
# MAGIC                 ROUND(AVG(s.num_questions)) AS assessment_num_questions,
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
# MAGIC                 ROUND(AVG(s.num_final_score)) AS section_num_final_score,
# MAGIC                 ROUND(AVG(s.num_possible_score)) AS section_num_possible_score,
# MAGIC                 ROUND(AVG(s.num_questions_answered)) AS section_num_questions_answered,
# MAGIC                 ROUND(AVG(s.num_questions)) AS section_num_questions,
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
# MAGIC                 ROUND(AVG(s.num_final_score)) AS organization_num_final_score,
# MAGIC                 ROUND(AVG(s.num_possible_score)) AS organization_num_possible_score,
# MAGIC                 ROUND(AVG(s.num_questions_answered)) AS organization_num_questions_answered,
# MAGIC                 ROUND(AVG(s.num_questions)) AS organization_num_questions,
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
# MAGIC        scored_date,
# MAGIC        num_final_score,
# MAGIC        num_possible_score,
# MAGIC        num_questions,
# MAGIC        num_questions_answered
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

# DBTITLE 1,View records with null assignment_start_date
# MAGIC %sql
# MAGIC SELECT * FROM clean_data WHERE assignment_start_date IS NULL;

# COMMAND ----------

# DBTITLE 1,How does start and stop dates relate
# MAGIC %sql
# MAGIC select assessment_instance_attempt_id,
# MAGIC  max_student_stop_datetime,
# MAGIC  student_start_datetime,
# MAGIC  min_student_start_datetime
# MAGIC FROM clean_data
# MAGIC WHERE assessment_instance_attempt_id IS NOT NULL
# MAGIC ORDER BY assessment_instance_attempt_id, student_start_datetime LIMIT 100
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC - assignment_start_date related to when it's assigned to a class?

# COMMAND ----------

# DBTITLE 1,How does in progress and start dates relate
# MAGIC %sql
# MAGIC select assessment_instance_attempt_id,
# MAGIC  assignment_start_date,
# MAGIC  student_start_datetime,
# MAGIC  min_student_start_datetime,
# MAGIC  was_in_progress_datetime,
# MAGIC  FROM clean_data
# MAGIC WHERE assessment_instance_attempt_id IS NOT NULL
# MAGIC ORDER BY assessment_instance_attempt_id, student_start_datetime LIMIT 100
# MAGIC ;

# COMMAND ----------


