# Temp for local development
import ssl
from itertools import chain

import matplotlib.pyplot as plt
# os.environ['PYSPARK_PYTHON'] = '/Library/Frameworks/Python.framework/Versions/3.6/bin/python3.6'
import numpy as np
import pandas as pd
import seaborn as sn
# https://github.com/shakedzy/dython
from dython.nominal import associations
from matplotlib.ticker import ScalarFormatter
from pyspark.sql import functions as F
from pyspark.sql import types as T

from pyspark import SparkContext
from pyspark.sql import SparkSession
from lib.woe import WOE_IV

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

import os
import json

dataDir = '/Users/duane.hinkley/PycharmProjects/c772-capstone-project/jupyter/.data/'

if not os.path.exists(dataDir):
  os.makedirs(dataDir)

ssl._create_default_https_context = ssl._create_unverified_context

types_global = None


def init_raw_df():
  rawDf = import_by_url('https://github.com/dlhinkley/c772-capstone-project/raw/master/data/assessment_items.csv')

  # Save to reuse
  save_df(rawDf, 'rawDf')

  return rawDf


def filter_raw_df(rawDf):
  # Only keep "fully scored" items
  # Filter to learner_attempt_status = 'fully scored'
  filterDf = rawDf.filter(
    (F.col('assessment_item_response_id').isNull() == False)
    & (F.col('learner_attempt_status') == 'fully scored')

  )

  # Change Date Fields from String to Timestamp Type
  types = get_var_types()
  for f in types['intervalVars']:
    filterDf = filterDf.withColumn(f, F.col(f).cast(T.TimestampType()))

  # Set default date values to null (years 2999 and 1900)
  # Set empty dates to null
  for f in types['intervalVars']:
    # Change to empty if date is more than 30 months in past or future
    filterDf = filterDf.withColumn(f, F.when(F.abs(F.months_between(F.col(f), F.current_timestamp())) > 30,
                                             None).otherwise(F.col(f)))

  return filterDf


def init_desc_df():
  descDf = import_by_url('https://github.com/dlhinkley/c772-capstone-project/raw/master/data/descriptions.csv')
  save_df(descDf, 'descDf')
  return descDf


def save_df(df, name):
  df.repartition(1).write.mode('overwrite').parquet(dataDir + name + ".parquet")


def save_dict(data, name):
  with open(dataDir + name + ".json", "w") as f:
    json.dump(data, f)


def load_dict(name):
  with open(dataDir + name + ".json") as f:
    out = json.load(f)

  return out


def load_df(name):
  return spark.read.parquet(dataDir + name + ".parquet")


from tinydb import TinyDB, Query


# Create Todo list
def init_todo():
  global td, dataDir
  td = TinyDB(dataDir + 'todo.json')


def add_todo(desc):
  global td
  q = Query()
  if not td.contains(q.todo == desc):
    td.insert({'todo': desc, 'finished': False})

  print("Todo: " + desc)


def list_todo(finished=None):
  global td

  for item in td:
    if (finished is not None):
      if (item['finished'] == finished):
        print(item)
    else:
      print(item)


def finish_todo(desc):
  global td
  q = Query()
  td.update({'finished': True}, q.todo == desc)
  print("Finished: " + desc)


def delete_todo(desc):
  global td
  q = Query()
  td.remove(q.todo == desc)


init_todo()

import os
from pyspark import SparkFiles


def import_by_url(url):
  # Given a url to a csv file, import and return a dataframe
  #
  sc.addFile(url)
  filename = os.path.basename(url)
  file = "file://" + SparkFiles.get(filename)
  return spark.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(file)


def filter_default(dfIn, f1, f2):
  # Given a dataframe and two date field names, returns the dataframe removing records
  # where the f1 or f2 columns equal a default date
  defaultDates = ["2999-01-01 00:00:00", "1900-01-01 00:00:00"]
  return dfIn.filter(~F.col(f1).isin(defaultDates) & ~F.col(f2).isin(defaultDates))


def date_stats(dfIn, f1, f2):
  # Given a dataframe and two date field names, returns a new dataframe with the difference between
  # the dates in minutes, hours and minutes
  dfOut = filter_default(dfIn, f1, f2)

  dfOut = dfOut.withColumn("minues", (F.col(f1).cast("long") - F.col(f2).cast("long")) / 60.).select(f1, f2, "minues")

  dfOut = dfOut.withColumn("hours", (F.col(f1).cast("long") - F.col(f2).cast("long")) / 3600.).select(f1, f2, "hours",
                                                                                                      "minues")

  return dfOut.withColumn("days", (F.col(f1).cast("long") - F.col(f2).cast("long")) / 86400.).select("days", "hours",
                                                                                                     "minues")


def annotate_plot(ax):
  # Add total labels to plot
  for p in ax.patches:
    ax.annotate(
      round(p.get_height(), 2),
      (p.get_x() + p.get_width() / 2., p.get_height()),
      ha='center',
      va='center',
      color='white',
      fontweight='bold',
      xytext=(0, -10),
      textcoords='offset points')


def date_boxplot(df, title, ax=False):
  # Given a dataframe of datestimes, create a boxplot of
  # date distribution
  types = get_var_types()
  # Convert to timestamps
  pdDf = df.select(*(F.unix_timestamp(c).alias(c) for c in types['intervalVars'] if c in df.columns)).toPandas()

  if ax:
    pdDf.boxplot(rot=270, figsize=[10, 10], ax=ax)
  else:
    ax = pdDf.boxplot(rot=270, figsize=[10, 10])

  # Min and Max date plus and minus one month
  max = pd.to_datetime(pdDf.max().max(), unit='s') + pd.DateOffset(months=1)
  min = pd.to_datetime(pdDf.min().min(), unit='s') - pd.DateOffset(months=1)

  # Date labels by month
  yLabels = pd.date_range(start=min.date(), end=max.date(), freq='MS')
  # Convert ticks to unix timestamp (int)
  ytick = [t.value // 10 ** 9 for t in yLabels]
  # Year and month readable labels
  newLabels = [ts.strftime('%Y-%m') for ts in yLabels]
  ax.set_yticks(ytick)
  ax.set_yticklabels(labels=newLabels)
  # Add category to labels
  labels = [types[label.get_text()] for label in ax.get_xticklabels()]
  ax.set_xticklabels(labels=labels)
  ax.set_title(title)


def type_cat(var):
  # return types_global[var]
  return var


# Create a udf for pyspark
type_cat_udf = F.udf(type_cat)


def distinct_val(df, skipKnown=True):
  # Given a dataframe, return the distinct values
  knowCols = ['assignment_late_submission',
              'learner_attempt_status',
              'is_deleted']
  if (skipKnown):
    cols = blacklist(df.columns, knowCols)
  else:
    cols = df.columns

  # Get count of values in each column
  svDf = df.agg(
    *(F.countDistinct(F.when(F.col(c).isNull(), 'null').otherwise(F.col(c).cast('string'))).alias(c) for c in cols))

  # Save dataframe to list
  sv = svDf.collect()[0]
  # Get columns with a count of one
  svCols = [c for c in cols if sv[c] == 1]
  svCols.sort()
  # Return one row of panda dataframe with count of 1
  pdDf = df.select(svCols).limit(1).toPandas()
  # Change the variable names to variables with labels
  return col_to_label(pdDf).transpose()


def id_to_name(df, idVar, newVar, newIdList):
  # Given a dataframe, id variable, new variable name and list of new ids
  # add a new variable to the dataframe mapping the id to the array

  # Save org ids to a list
  oldIdList = [row[idVar] for row in df.select(idVar).distinct().orderBy(idVar).collect()]

  # Create map
  newIdMap = dict()
  # Add letters to map
  for i, val in enumerate(oldIdList):
    newIdMap[val] = newIdList[i]

  # Create mapping expression
  mapping_expr = F.create_map([F.lit(x) for x in chain(*newIdMap.items())])

  # Add org column with letter related to id
  return df.withColumn(newVar, mapping_expr[df[idVar]])


# Return elements in whitelist
def whitelist(l, whitelist):
  if whitelist:
    return [x for x in l if x in whitelist]
  else:
    return l


# Return elements except those in blacklist
def blacklist(l, blacklist):
  if blacklist:
    return [x for x in l if x not in blacklist]
  else:
    return l


# Return a dictionary including arrays of variable names for each category
# If dfColumns provied, return only values in dfColumns
def get_var_cats(dfColumns=False):
  cat = dict()
  dfPd = load_df('descDf').toPandas()

  cat['orgVars'] = whitelist(dfPd.loc[dfPd['category'] == 'Organization'].field.tolist(), dfColumns)
  cat['sectionVars'] = whitelist(dfPd.loc[dfPd['category'] == 'Section'].field.tolist(), dfColumns)
  cat['learnerVars'] = whitelist(dfPd.loc[dfPd['category'] == 'Learner'].field.tolist(), dfColumns)
  cat['assessmentVars'] = whitelist(dfPd.loc[dfPd['category'] == 'Assessment'].field.tolist(), dfColumns)
  cat['assignmentVars'] = whitelist(dfPd.loc[dfPd['category'] == 'Assignment'].field.tolist(), dfColumns)
  cat['itemVars'] = whitelist(dfPd.loc[dfPd['category'] == 'Item'].field.tolist(), dfColumns)
  cat['assignmentAttemptVars'] = whitelist(dfPd.loc[dfPd['category'] == 'Assignment Attempt'].field.tolist(), dfColumns)
  cat['itemAttemptVars'] = whitelist(dfPd.loc[dfPd['category'] == 'Item Attempt'].field.tolist(), dfColumns)

  return cat


# Return a dictionary including arrays of variable names for each type
# If dfColumns provied, return only values in dfColumns
def get_var_types(dfColumns=False):
  if (types_global is None):

    type = dict()
    descDf = load_df('descDf')

    type['identifierVars'] = whitelist(variable_types_label(descDf, 'Categorical Identifier').variable.tolist(),
                                       dfColumns)
    type['nominalVars'] = whitelist(variable_types_label(descDf, 'Categorical Nominal').variable.tolist(), dfColumns)
    type['continuousVars'] = whitelist(variable_types_label(descDf, 'Numeric Continuous').variable.tolist(), dfColumns)
    type['intervalVars'] = whitelist(variable_types_label(descDf, 'Categorical Interval').variable.tolist(), dfColumns)
    type['binaryVars'] = whitelist(variable_types_label(descDf, 'Categorical Binary').variable.tolist(), dfColumns)
    type['durationVars'] = [
      'item_attempt_duration_mins',
      'student_duration_mins',
      'timeliness_duration_mins'
    ]

    type['identifierVarsLabels'] = whitelist(
      variable_types_label(descDf, 'Categorical Identifier').variable_label.tolist(), dfColumns)
    type['nominalVarsLabels'] = whitelist(variable_types_label(descDf, 'Categorical Nominal').variable_label.tolist(),
                                          dfColumns)
    type['continuousVarsLabels'] = whitelist(variable_types_label(descDf, 'Numeric Continuous').variable_label.tolist(),
                                             dfColumns)
    type['intervalVarsLabels'] = whitelist(variable_types_label(descDf, 'Categorical Interval').variable_label.tolist(),
                                           dfColumns)
    type['binaryVarsLabels'] = whitelist(variable_types_label(descDf, 'Categorical Binary').variable_label.tolist(),
                                         dfColumns)

    # Sort
    for key in type:
      type[key].sort()

    # Add variable to label map
    for cat in ['identifierVars', 'nominalVars', 'continuousVars', 'intervalVars', 'binaryVars']:
      for p in range(len(type[cat])):
        type[type[cat][p]] = type[cat + 'Labels'][p]

    return type
  else:
    return types_global


# Given a dataframe of datetime fields, return a matrix of the mean difference
#
def date_diff_map(df, title, scale='D', ax=None):
  intVars = df.columns
  intSize = len(intVars)
  am = pd.DataFrame(np.zeros(shape=(intSize, intSize)), columns=intVars, index=intVars)

  for v1 in intVars:
    for v2 in intVars:
      if v1 != v2:
        mean = ((df[v1] - df[v2]) / np.timedelta64(1, scale)).mean()
        am.at[v1, v2] = mean

  ax = sn.heatmap(am, annot=True, fmt=".0f", ax=ax)
  ax.set_title(title)


def get_random_sample(df):
  return df.sample(False, .10, 8764664)


def mean_hours_assignment_interval(df, ax=None):
  assignIntVars = [
    'student_start_datetime',
    'was_in_progress_datetime',
    'scored_datetime',
    'was_submitted_datetime_actual',
    'student_stop_datetime',
    'was_fully_scored_datetime',
  ]
  date_diff_map(df.select(assignIntVars).toPandas(), "Mean Hours Between Assignment Interval Vars", 'h', ax)


def dual_mean_hours_assignment(df1, df2, title1='', title2='', main=''):
  fig, (ax1, ax2) = plt.subplots(ncols=2, sharey=True, sharex=True)

  mean_hours_assignment_interval(df1, ax1)
  ax1.set_title(title1)

  mean_hours_assignment_interval(df2, ax2)
  ax2.set_title(title2)

  plt.suptitle(main)
  plt.show()


def remove_null_student_dates(df):
  return df.filter(
    (F.col('student_start_datetime').isNull() == False)
    | (F.col('student_stop_datetime').isNull() == False)
  )


def impute_3422_null_dates(filterDf):
  # Get sample to extract means
  pdDf = get_random_sample(filterDf).select(
    'student_stop_datetime',
    'scored_datetime').toPandas()

  # Calculate mean difference in seconds
  mScoredDate = ((pdDf['scored_datetime'] - pdDf['student_stop_datetime']) / np.timedelta64(1, 's')).mean()

  return filterDf.withColumn(
    "scored_datetime_imputed",
    F.col("scored_datetime").isNull()
  ).withColumn(
    "scored_datetime",
    F.when(
      F.col("scored_datetime").isNull(),
      (F.unix_timestamp("student_stop_datetime") - mScoredDate).cast('timestamp')
    ).otherwise(F.col("scored_datetime"))
  )


def impute_9965_null_dates(df):
  # Get sample to extract means
  pdDf = get_random_sample(df).select('student_start_datetime', 'student_stop_datetime', 'was_in_progress_datetime',
                                      'scored_datetime').toPandas()

  # Calculate mean difference in seconds
  meanDiff = ((pdDf['was_in_progress_datetime'] - pdDf['student_start_datetime']) / np.timedelta64(1, 's')).mean()

  return df.withColumn(
    "scored_datetime",
    F.when(
      F.col("was_in_progress_datetime").isNull(),
      (F.unix_timestamp("student_start_datetime") - meanDiff).cast('timestamp')
    ).otherwise(F.col("was_in_progress_datetime"))
  )


# Given a dataframe and variable name, return the value names and counts for that variable
def count_values(df, f):
  return df.groupBy(f).count().orderBy('count', ascending=False)


def response_correctness_bar_plot(df, varName, ax=None):
  pdDf = df.groupBy(varName).count().orderBy('count', ascending=False).toPandas()
  ax = pdDf.plot(kind='bar', ax=ax)
  labels = pdDf[varName].fillna('null').tolist()
  ax.set_xticklabels(labels=labels)
  ax.set_title('Values ' + varName)
  annotate_plot(ax)


# Create a binary catagorical variable for final_score_unweighted
def add_zero_final_score_var(df):
  return df.withColumn(
    "zero_score",
    F.when(F.col('final_score_unweighted') == 0, 'Yes').otherwise("No")
  )


def add_zero_raw_score_var(df):
  return df.withColumn(
    "zero_score",
    F.when(F.col('raw_score').isNull(), 'Null')
      .otherwise(F.when(F.col('raw_score') == 0, 'Yes').otherwise("No"))
  )


# Create a bar chart counting countvar for each groupvar
def num_group_bar_chart(df, groupByVar, countVar, countAlias, title, ax=None):
  sByO = df.groupBy(groupByVar).agg(F.countDistinct(countVar).alias(countAlias)).orderBy(groupByVar)
  pdDf = sByO.toPandas()

  # Add mean
  mean = sByO.agg(F.round(F.avg(F.col(countAlias))).alias('mean')).collect()[0][0]
  # Append row with mean
  pdDf = pdDf.append({groupByVar: 'mean', countAlias: mean}, ignore_index=True)

  axa = pdDf.plot(groupByVar, countAlias, kind='bar', ax=ax, title=title)
  annotate_plot(axa)
  if (ax is None):
    plt.show()


def num_sections_by_org_bar_chart(df, ax=None, title='Num Sections by Organization'):
  num_group_bar_chart(df, 'org_id', 'section_id', 'sections', title, ax)


def num_learners_by_org_bar_chart(df):
  num_group_bar_chart(df, 'org_id', 'learner_id', 'learners', 'Num Learners by Organization')


def mean_group_bar_chart(df, group1, group2, countVar, countAlias, title='', ax=None):
  lByS = df.groupBy(group1, group2).agg(F.countDistinct(countVar).alias(countAlias))
  # Av
  lBySMean = lByS.groupBy(group1).agg(F.avg(countAlias).alias(countAlias)).orderBy(group1)
  pdDf = lBySMean.toPandas()

  # Add mean
  mean = lBySMean.agg(F.round(F.avg(F.col(countAlias))).alias('mean')).collect()[0][0]
  # Append row with mean
  pdDf = pdDf.append({group1: 'mean', countAlias: mean}, ignore_index=True)

  axo = pdDf.plot.bar(group1, countAlias, ax=ax, title=title)
  annotate_plot(axo)

  if (ax is None):
    plt.show()


def mean_sec_learners_by_org_bar_chart(df):
  mean_group_bar_chart(df, 'org_id', 'section_id', 'learner_id', 'learners', 'Mean Section Learners by Organization')


def mean_sec_assess_by_org_bar_chart(df, ax=None, title='Mean Section Assessments by Organization'):
  mean_group_bar_chart(df, 'org_id', 'section_id', 'assessment_id', 'assessments', ax=ax, title=title)


def mean_assess_by_org_bar_chart(df, ax=None, title='Mean Learners Assessments by Organization'):
  mean_group_bar_chart(df, 'org_id', 'learner_id', 'assessment_id', 'assessments', ax=ax, title=title, )


# Mean Scores by Organization
def mean_scores_by_orgs_bar_chart(df):
  sByO = df.groupBy('org_id').agg(F.avg('final_score_unweighted').alias('scores')).orderBy('org_id')

  pdDf = sByO.toPandas()

  # Add mean
  meanAssess = sByO.agg(F.round(F.avg(F.col('scores'))).alias('mean')).collect()[0][0]
  # Append row with mean
  pdDf = pdDf.append({'org_id': 'mean', 'scores': meanAssess}, ignore_index=True)

  ax = pdDf.plot.bar('org_id', 'scores', title='Mean Scores by Organization')
  annotate_plot(ax)
  plt.show()


def crosstab_percent(table):
  return table.apply(lambda r: round(r / r.sum() * 100), axis=1)


# Given two dataframes of datetime fields, with optional titles, return two side by side boxplots
#
def dual_date_boxplot(df1, df2, title1='', title2='', main=''):
  fig, (ax1, ax2) = plt.subplots(ncols=2, sharey=True, sharex=True)

  date_boxplot(df1, title1, ax1)
  date_boxplot(df2, title2, ax2)

  plt.suptitle(main)
  plt.show()


# Given two pandas dataframes with one variable each, return two histograms
def dual_hist(pdDf1, pdDf2, title1='', title2='', main=''):
  fig, (ax1, ax2) = plt.subplots(ncols=2, sharey=True)

  pdDf1.hist(ax=ax1)
  ax1.set_title(title1)

  pdDf2.hist(ax=ax2)
  ax2.set_title(title2)

  plt.suptitle(main)
  plt.show()


# Given two dataframes, create two heatmaps of all variables in dataframe
def dual_assoc_heatmap(df1, df2, title1='', title2='', main='', figsize=(10, 5)):
  fig, (ax1, ax2) = plt.subplots(ncols=2, sharey=True, sharex=True, figsize=figsize)

  associations(df1.toPandas(), nan_replace_value='null', ax=ax1, plot=False)
  ax1.set_title(title1)

  associations(df2.toPandas(), nan_replace_value='null', ax=ax2, plot=False)
  ax2.set_title(title2)

  plt.suptitle(main)
  plt.show()


# Impute the 4446 null dates with the mean difference of was_submitted_datetime_actual and student_stop_datetime
def impute_4446_null_dates(df):
  # Get sample to extract means
  pdDf = df.select('scored_datetime', 'was_submitted_datetime_actual').toPandas()

  # Calculate mean difference in seconds
  meanDiff = ((pdDf['scored_datetime'] - pdDf['was_submitted_datetime_actual']) / np.timedelta64(1, 's')).mean()

  return df.withColumn(  # Imputed flag
    "was_submitted_datetime_actual_imputed",
    F.col('was_submitted_datetime_actual').isNull()
  ).withColumn(
    "was_submitted_datetime_actual",
    F.when(
      (F.col('scored_datetime_imputed') == False)  # don't impute with imputed value
      & (F.col('was_submitted_datetime_actual').isNull())
      & (F.col('final_score_unweighted') > 0),
      (F.unix_timestamp("scored_datetime") - meanDiff).cast('timestamp')
    ).otherwise(F.col("was_submitted_datetime_actual"))
  )


def impute_number_of_learners(cleanDf):
  # Calculate number of learners on Filtered
  dfCount = cleanDf.groupBy('assessment_instance_id', 'number_of_learners').agg(
    F.countDistinct('learner_id').alias('number_of_learners_calc')
  ).select('assessment_instance_id', 'number_of_learners_calc')

  # Update with calculated value
  cleanDf = cleanDf.join(dfCount, on=['assessment_instance_id'], how='left')

  # Drop incorrect number of learners
  cleanDf = cleanDf.drop('number_of_learners')

  # Rename calculated to original
  return cleanDf.withColumnRenamed("number_of_learners_calc", "number_of_learners")


# Reduce the number of levels in item_type_code_name
def reduce_type_code_levels(cleanDf):
  # Combine fillInTheBlank and FillinBlankResponse
  cleanDf = cleanDf.withColumn("item_type_code_name", F.when(F.col("item_type_code_name") == "FillinBlankResponse",
                                                             "fillInTheBlank").otherwise(F.col("item_type_code_name")))

  # Combine multipleChoice and MultipleChoiceResponse
  cleanDf = cleanDf.withColumn("item_type_code_name", F.when(F.col("item_type_code_name") == "MultipleChoiceResponse",
                                                             "multipleChoice").otherwise(F.col("item_type_code_name")))

  # Total count
  tot = cleanDf.filter(F.col("item_type_code_name").isNull() == False).count()

  freqTable = cleanDf.groupBy("item_type_code_name") \
    .count() \
    .withColumnRenamed('count', 'cnt_per_group') \
    .withColumn('perc_of_count_total', (F.col('cnt_per_group') / tot) * 100) \
    .orderBy("cnt_per_group", ascending=False)

  # freqTable.show(50, False)

  # We only want five levels, so convert everything below 6% to other

  otherRows = freqTable.filter("perc_of_count_total < 6")
  otherLevels = [row['item_type_code_name'] for row in otherRows.select("item_type_code_name").collect()]

  return cleanDf.withColumn("item_type_code_name", F.when(
    F.col("item_type_code_name").isin(otherLevels) | F.col("item_type_code_name").isNull(), "Other").otherwise(
    F.col("item_type_code_name")))


# Return dataframe of min, max, mean grouped by column
def group_by_describe(df, groupBy, statsCol):
  return df.groupBy(groupBy).agg(
    F.round(F.count(statsCol)).alias('count'),
    F.round(F.min(statsCol)).alias('min'),
    F.round(F.avg(statsCol)).alias('mean'),
    F.round(F.max(statsCol)).alias('max')
  )


# Given a dataframe and list of cols, displays the min, max, null, and unique value counts
def date_min_max_null_unique(df, cols):
  for f in cols:
    print(f)
    df.agg(
      F.countDistinct(f).alias('unique'),
      F.count(F.when(F.col(f).isNull(), f)).alias('null'),
      F.min(f).alias('min'),
      F.max(f).alias('max')
    ).show(1, False)


# Given a dataframe of dates and columns, return a pandas datafram of statistics
def date_statisticts(df, cols):
  cols.sort()
  distinct = df.agg(
    *(F.countDistinct(F.col(c)).alias(c) for c in cols)
  ).collect()[0]
  null = df.agg(
    *(F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in cols)
  ).collect()[0]
  min = df.agg(
    *(F.min(F.col(c).cast(T.DateType())).alias(c) for c in cols)
  ).collect()[0]
  max = df.agg(
    *(F.max(F.col(c).cast(T.DateType())).alias(c) for c in cols)
  ).collect()[0]
  plotdata = pd.DataFrame({
    "distinct": distinct,
    "null": null,
    "min": min,
    "max": max,
  },
    index=cols)

  return plotdata


def null_zero_counts(df, cols):
  cols.sort()
  null = df.agg(
    *(F.count(F.when(F.col(c).isNull(), c)).alias('null') for c in cols)
  ).collect()[0]
  zero = df.agg(
    *(F.count(F.when(F.col(c) == 0, c)).alias("zero") for c in cols)
  ).collect()[0]

  return pd.DataFrame({
    "null": null,
    "zero": zero,
  }, index=cols)


def unique_nulls(df, cols):
  cols.sort()
  null = df.agg(
    *(F.count(F.when(F.col(c).isNull(), c)).alias('null') for c in cols)
  ).collect()[0]
  unique = df.agg(
    *(F.countDistinct(c).alias('unique') for c in cols)
  ).collect()[0]

  return pd.DataFrame({
    "null": null,
    "unique": unique,
  }, index=cols)


# Given a full name of multiple words, return the initials in lower case
def initials(fullname):
  xs = (fullname)
  name_list = xs.split()

  initials = ""

  for name in name_list:  # go through each name
    initials += name[0].lower()  # append the initial

  return initials


# Given a full name of multiple words,
# return the initials in lower case wrapped in parenthesis
def wrap_initials(fullname):
  return '(' + initials(fullname) + ')'


# Create a udf for pyspark
initials_udf = F.udf(wrap_initials)


# Given a dataframe and variable type, return the names of variables
def variable_types(df, type):
  return df.filter(F.col('type') == type).select(
    F.concat_ws(' ', F.col('category'), initials_udf(F.col('category'))).alias('category'),
    F.col('field').alias('variable')
  ).orderBy('category', 'variable').toPandas()


# Given a dataframe and variable type, return the names of
# variables with cat label as pandas dataframe
def variable_types_label(df, type=None):
  return df.filter(F.col('type') == type).select(
    F.concat_ws(' ', F.col('field'), initials_udf(F.col('category'))).alias('variable_label'),
    F.col('field').alias('variable')
  ).orderBy('category', 'variable').toPandas()


# Given a pandas dataframe remap the column name to columns with labels
def col_to_label(pdDf):
  typeDict = get_var_types()
  pdDf.columns = pdDf.columns.to_series().map(typeDict)
  return pdDf


# Removes start dates after stop dates and null dates
def clean_item_attempt_dates(df):
  return df.filter(
    F.col('item_attempt_start_datetime_utc').cast('long') <= F.col('item_attempt_end_datetime_utc').cast('long')
  )


# Adds the duration between start and stop of the attempt and each item
def add_attempt_duration(df):
  return df.withColumn(
    'item_attempt_duration_mins',
    (F.col('item_attempt_end_datetime_utc').cast('long') - F.col('item_attempt_start_datetime_utc').cast('long')) / 60
  ).withColumn(
    'student_duration_mins',
    (F.col('student_stop_datetime').cast('long') - F.col('student_start_datetime').cast('long')) / 60
  ).withColumn(
    'timeliness_duration_mins',
    (F.col('assignment_due_date').cast('long') - F.col('student_start_datetime').cast('long')) / 60
  )


def remove_attempt_stop_dates_before_start_dates(df):
  inList = df.filter(
    F.col('item_attempt_end_datetime_utc') < F.col('item_attempt_start_datetime_utc')
  ).select('assessment_item_response_id').toPandas().assessment_item_response_id.tolist()

  return df.filter(F.col('assessment_item_response_id').isin(inList) == False)


def rotate_matrix_labels(pdDf, axs):
  n = len(pdDf.columns)
  for x in range(n):
    for y in range(n):
      # to get the axis of subplots
      ax = axs[x, y]
      # to make x axis name vertical
      ax.xaxis.label.set_rotation(90)
      # to make y axis name horizontal
      ax.yaxis.label.set_rotation(0)
      # to make sure y axis names are outside the plot area
      ax.yaxis.labelpad = 50


def logrithmic_histogram(pdDf, tickStep=5000):
  import matplotlib.ticker as plticker

  ax = pdDf.plot(kind='hist', logy=True, bins=100, bottom=0.1, rot=270)

  # Add more steps to x labels
  start, end = ax.get_xlim()
  ax.xaxis.set_ticks(np.arange(start, end, tickStep))

  loc = plticker.MultipleLocator(tickStep)  # this locator puts ticks at regular intervals
  ax.xaxis.set_major_locator(loc)

  # Remove scientific notation
  for axis in [ax.xaxis, ax.yaxis]:
    axis.set_major_formatter(ScalarFormatter())


def get_iqr_filter(df, col):
  # Only positive (negative are a different problem)
  pdDf = df.filter(F.col(col) >= 0).select(col).toPandas()
  Q1 = pdDf[col].quantile(0.25)
  Q3 = pdDf[col].quantile(0.75)
  median = pdDf[col].quantile(0.50)

  IQR = Q3 - Q1

  lowFilter = (Q1 - 1.5 * IQR)
  highFilter = (Q3 + 1.5 * IQR)

  return lowFilter, highFilter, median


# Given a dataframe and column, impute outliers over IQR with mean
def iqr_impute(df, col):
  (lowFilter, highFilter, median) = get_iqr_filter(df, col)
  return df.withColumn(
    col,
    F.when(
      F.col(col) > highFilter,
      median
    ).otherwise(F.col(col))
  )


def impute_item_attempt_duration(df):
  return iqr_impute(df, 'item_attempt_duration_mins')


def impute_student_duration(df):
  return iqr_impute(df, 'student_duration_mins')


def impute_timeliness_duration(df):
  return iqr_impute(df, 'timeliness_duration_mins')


def remove_unassigned_response_correctness(df):
  return df.filter(
    (F.col('response_correctness').isNull() == True)
    | (F.col('response_correctness') != '[unassigned]')
  )


def remove_null_response_correctness(df):
  return df.filter(F.col('response_correctness').isNull() == False)


# Add the target variable using raw score
def create_target_var_from_raw_score(df):
  return df.withColumn(
    'target',
    F.when(F.col('raw_score') > 0, 1).otherwise(0)
  )

# Given a spark dataframe, name of categorical variable and name of target variable, add a column with the
# weight of evidence value for the categorical variable
#
def add_weight_of_evidence(df, catVar, targetVar, smoothing = 0):
  woe = WOE_IV(df, [catVar], targetVar, 1, smoothing)
  woe.fit()
  return woe.transform(df)

# Adds smoothed weight of evidence to provided column
# c = smoothing parameter
# target = target variable name
# cat = categorical variable name
#
# From SAS Certification Part 2, Lesson 3: Preparing the Input Variables
def add_swoe(df, target, cat, c):
    # Proportion of events in sample
    p1 = df.agg( F.avg( F.col(target) ).alias('mean') ).collect()[0]['mean']

    # Get event and non event counts
    cntDf =  df.groupBy(cat).agg(
        F.sum( F.when( F.col(target) == 1, 1 ).otherwise(0)).alias('1'),
        F.sum( F.when( F.col(target) == 0, 1 ).otherwise(0)).alias('0')
    )
    swoeVar = cat + '_swoe'
    # Add swoe
    # Formula:
    #           # events + cp1
    # ln( ----------------------- )
    #     # nonevents + c(1 - p1)
    cntDf = cntDf.withColumn(
        swoeVar,
       F.log(  (F.col('1') +  p1 * c)  / (F.col('0') +  (1 - p1) * c) )
    )
    # Append column to original table name
    return df.join(cntDf.select('cat', swoeVar), 'cat')
