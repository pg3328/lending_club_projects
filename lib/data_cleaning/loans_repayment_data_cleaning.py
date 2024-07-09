from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
import math

spark = SparkSession.builder \
    .appName('project') \
    .config('spark.ui.port', '0') \
    .config("spark.sql.warehouse.dir", f"/user/itv012667/warehouse") \
    .config('spark.shuffle.useOldFetchProtocol', 'true') \
    .enableHiveSupport() \
    .master('yarn') \
    .getOrCreate()

"""
    Retreives loans repayment data from disk and return the dataframe
"""

def get_loans_repayment_data():
    schema = 'loan_id string, total_principal_received float, total_interest_received float, total_late_fee_received float, total_payment_received float, last_payment_amount float, last_payment_date string, next_payment_date string'
    return spark.read.option("mode","permissive").csv("project/raw_divided_data/loans_repayments_csv",schema = schema)

"""
    add ingestion timestamp
"""
def add_ingestion_timestamp():
    loans_repayment_data = get_loans_repayment_data()
    return loans_repayment_data.withColumn("ingest_date",current_timestamp())

"""
    dropping nulls if particular columns are null 
"""
def drop_null_rows():
    loans_repayment_data = get_loans_repayment_data()
    null_column_lookup = ["total_principal_received", "total_interest_received", "total_late_fee_received", "total_payment_received", "last_payment_amount"]
    return loans_repayment_data.dropna(subset = null_column_lookup)

"""
    A few datapoints don't have details about total payment received, Hence replace the total payment received. 
"""

def add_column_total_payment_received():
    loans_repay_filtered_df = drop_null_rows()
    return loans_repay_filtered_df.withColumn(
   "total_payment_received",
    when(
        (col("total_principal_received") != 0.0) &
        (col("total_payment_received") == 0.0),
        col("total_principal_received") + col("total_interest_received") + col("total_late_fee_received")
    ).otherwise(col("total_payment_received")))

    
"""
    Last payment date is recorded as 0.0 replacing it with Nulls
"""

def clean_last_payment_date():
    loans_cleaned_data = add_column_total_payment_received()
    return  loans_cleaned_data.withColumn(
  "last_payment_date",
   when(
       (col("last_payment_date") == 0.0),
       None
       ).otherwise(col("last_payment_date"))
    )

"""
    Replacing next payment date is recorded as 0.0 replacing it with Nulls
"""

def clean_next_payment_date():
    loans_cleaned_data = clean_last_payment_date()
    return  loans_cleaned_data.withColumn(
  "next_payment_date",
   when(
       (col("next_payment_date") == 0.0),
       None
       ).otherwise(col("next_payment_date"))
    )

def write_to_disk():
    loans_cleaned_data = clean_next_payment_date()
    loans_cleaned_data.write.mode("overwrite").option("path","project/cleaned_data/loans_repayment_cleaned_data").save()

if __name__ == "__main__":
    write_to_disk()