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
    Reads the loans data and returns a spark dataframe with it
"""
    
def get_loans_data():
    schema ='loan_id string, member_id string, loan_amount float, funded_amount float, loan_term_months string, interest_rate float, monthly_installment float, issue_date string, loan_status string, loan_purpose string, loan_title string'
    return spark.read.csv("project/raw_divided_data/loans_data_csv",schema=schema)

"""
    Drop the rows when all the column values are nulls
"""
def drop_nulls():
    loans_data = get_loans_data()
    return loans_data.dropna(how='all')

"""
    Ingestion Timestamp column addition
"""
def add_ingestion_timestamp():
    df = drop_nulls()
    return df.withColumn("ingest_date",current_timestamp())

"""
    Filtering the rows with nulls in said columns
"""
def drop_specific_nulls():
    loans_data = add_ingestion_timestamp()
    columns_to_check = ["loan_amount", "funded_amount", "loan_term_months", "interest_rate", "monthly_installment", "issue_date", "loan_status", "loan_purpose"]
    return loans_data.dropna(subset=columns_to_check)

"""
    Using regex converting loan term in %d months to %d 
"""
def update_months():
    loans_data = drop_specific_nulls()
    return loans_data.withColumn("loan_term_months", (regexp_replace(col("loan_term_months"), " months", "") .cast("int") / 12) \
    .cast("int")) \
    .withColumnRenamed("loan_term_months","loan_term_years")

"""
    ensure that loan_purpose is in enum of set values otherwise replace it with other
"""
def handle_loan_purpose():
    loan_purpose_lookup = ["debt_consolidation", "credit_card", "home_improvement", "other", "major_purchase", "medical", "small_business", "car", "vacation", "moving", "house", "wedding", "renewable_energy", "educational"]
    loans_data = update_months()
    return loans_data.withColumn("loan_purpose",when(col("loan_purpose").isin(loan_purpose_lookup),col("loan_purpose")).otherwise("other"))

"""
    write the cleaned data back to disk
"""
def clean_data_write_to_disk():
    cleaned_data = handle_loan_purpose()
    cleaned_data.write.mode("overwrite").option("path","project/cleaned_data/loans_cleaned_data").save()



if __name__ == "__main__":
    clean_data_write_to_disk()