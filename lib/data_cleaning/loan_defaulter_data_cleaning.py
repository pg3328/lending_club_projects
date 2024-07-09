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
    gets the loan defaulter data from disk and returns a dataframe
"""

def get_loan_defaulter_data():
    schema = loan_defaulters_schema = "member_id string, delinq_2yrs float, delinq_amnt float, pub_rec float, pub_rec_bankruptcies float,inq_last_6mths float, total_rec_late_fee float, mths_since_last_delinq float, mths_since_last_record float"
    return spark.read.csv("project/raw_divided_data/loans_defaulters_csv",schema=schema)

"""
    Cast a few columns into the respective data types and remove nulls in the columns and add 0. 
"""
def cast_dtypes():
    loans_raw_df = get_loan_defaulter_data()
    return loans_raw_df.withColumn("delinq_2yrs", col("delinq_2yrs").cast("integer")).fillna(0, subset = ["delinq_2yrs"])

"""
    Takes the cleaned data and find the data apt to consider for delinq and write it to disk
"""
def write_delinq_data(cleaned_data):
    delinq_data = cleaned_data.filter((col("delinq_2yrs")> 0) | (col("mths_since_last_delinq")>0)).withColumn("mths_since_last_delinq",cleaned_data.mths_since_last_delinq.cast(IntegerType())).select("member_id","delinq_2yrs","delinq_amnt","mths_since_last_delinq")
    delinq_data.write.mode("overwrite").option("path","project/cleaned_data/loans_defaulters_deling_csv").save()
    
"""
    List of members that have defaulted previously. 
"""

def write_default_member_id(cleaned_data):
     default_members = cleaned_data.filter((col("pub_rec")>0.0) | (col("pub_rec_bankruptcies")>0.0) | (col("inq_last_6mths")>0.0)).select("member_id")
     default_members.write.mode("overwrite").option("path","project/cleaned_data/loans_defaulters_records_enq_csv").save()

"""
    collects the publics records from clean data, converts the data types and write it to disk. 
"""
def write_detailed_public_records(cleaned_data):
    detailed_public_records = cleaned_data.withColumn("pub_rec", col("pub_rec").cast("integer")).fillna(0, subset = ["pub_rec"]).withColumn("pub_rec_bankruptcies", col("pub_rec_bankruptcies").cast("integer")).fillna(0, subset = ["pub_rec_bankruptcies"]).withColumn("inq_last_6mths", col("inq_last_6mths").cast("integer")).fillna(0, subset = ["inq_last_6mths"]).select("member_id", "pub_rec", "pub_rec_bankruptcies", "inq_last_6mths")
    detailed_public_records.write.mode("overwrite").option("path","project/cleaned_data/loans_def_detail_records_enq_df_csv").save()

"""
    Orchestrator for all the functions
"""
def write_to_disk():
    cleaned_data = cast_dtypes()
    write_delinq_data(cleaned_data)
    write_default_member_id(cleaned_data)
    write_detailed_public_records(cleaned_data)
    

if __name__ == "__main__":
    write_to_disk()