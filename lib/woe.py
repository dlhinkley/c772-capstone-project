import math

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Ref: https://github.com/albertusk95/weight-of-evidence-spark
# Modified to return smoothed weight of evidence if smoothing parameter provided

class WOE_IV(object):
    # If smoothing > 0, use smoothed weight of evidence
    def __init__(self, df: DataFrame, cols_to_woe: [str], label_column: str, good_label: str, smoothing: int):
        self.df = df
        self.cols_to_woe = cols_to_woe
        self.label_column = label_column
        self.good_label = good_label
        self.fit_data = {}
        self.smoothing = smoothing
        self.rhol = 0

    def fit(self):
        for col_to_woe in self.cols_to_woe:
            total_good = self.compute_total_amount_of_good()
            total_bad = self.compute_total_amount_of_bad()

            woe_df = self.df.select(col_to_woe)
            categories = woe_df.distinct().collect()
            for category_row in categories:
                category = category_row[col_to_woe]
                good_amount = self.compute_good_amount(col_to_woe, category)
                bad_amount = self.compute_bad_amount(col_to_woe, category)

                good_amount = good_amount if good_amount != 0 else 0.5
                bad_amount = bad_amount if bad_amount != 0 else 0.5

                good_dist = good_amount / total_good
                bad_dist = bad_amount / total_bad

                self.build_fit_data(col_to_woe, category, good_dist, bad_dist)

    def transform(self, df: DataFrame):
        def _encode_woe(col_to_woe_):
            return F.coalesce(
                *[F.when(F.col(col_to_woe_) == category, F.lit(woe_iv['woe']))
                  for category, woe_iv in self.fit_data[col_to_woe_].items()]
            )

        for col_to_woe, woe_info in self.fit_data.items():
            df = df.withColumn(col_to_woe + '_woe', _encode_woe(col_to_woe))
        return df

    def compute_mean_of_target(self):
        return self.df.agg( F.avg( F.col(self.label_column) ).alias('mean') ).collect()[0]['mean']

    def compute_total_amount_of_good(self):
        return self.df.select(self.label_column).filter(F.col(self.label_column) == self.good_label).count()

    def compute_total_amount_of_bad(self):
        return self.df.select(self.label_column).filter(F.col(self.label_column) != self.good_label).count()

    def compute_good_amount(self, col_to_woe: str, category: str):
        return self.df.select(col_to_woe, self.label_column)\
                      .filter(
                            (F.col(col_to_woe) == category) & (F.col(self.label_column) == self.good_label)
                      ).count()

    def compute_bad_amount(self, col_to_woe: str, category: str):
        return self.df.select(col_to_woe, self.label_column)\
                      .filter(
                            (F.col(col_to_woe) == category) & (F.col(self.label_column) != self.good_label)
                      ).count()

    def build_fit_data(self, col_to_woe, category, good_dist, bad_dist):

        if (self.smoothing > 0):
            c = self.smoothing
            self.rhol = self.compute_mean_of_target()
            woe_info = {
                category: {
                    'woe': math.log( (good_dist + self.rhol * c) / bad_dist + (1 - self.rhol ) *  c),
                    'iv': (good_dist - bad_dist) * math.log(good_dist / bad_dist)
                }
            }
        else:
            woe_info = {
                category: {
                    'woe': math.log(good_dist / bad_dist),
                    'iv': (good_dist - bad_dist) * math.log(good_dist / bad_dist)
                }
            }

        if col_to_woe not in self.fit_data:
            self.fit_data[col_to_woe] = woe_info
        else:
            self.fit_data[col_to_woe].update(woe_info)

    def compute_iv(self):
        iv_dict = {}

        for woe_col, categories in self.fit_data.items():
            iv_dict[woe_col] = 0
            for category, woe_iv in categories.items():
                iv_dict[woe_col] += woe_iv['iv']
        return iv_dict
