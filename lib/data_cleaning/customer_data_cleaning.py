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
    Gets the csv file data into memory and returns dataframe
"""
def get_customer_data():
    customer_schema = "member_id string, emp_title string, emp_length string, home_ownership string, annual_inc float, addr_state string,zip_code string, country string, grade string, sub_grade string, verification_status string, tot_hi_cred_lim float, application_type string, annual_inc_joint float, verification_status_joint string"
    return spark.read.csv("project/raw_divided_data/customers_data_csv",header=True,schema=customer_schema)

"""
    Renames a few columns and returns the dataframe. 
"""

def rename_columns():
    customer_df = get_customer_data()
    return customer_df.withColumnRenamed("annual_inc", "annual_income") \
    .withColumnRenamed("addr_state", "address_state") \
    .withColumnRenamed("zip_code", "address_zipcode") \
    .withColumnRenamed("country", "address_country") \
    .withColumnRenamed("tot_hi_credit_lim", "total_high_credit_limit") \
    .withColumnRenamed("annual_inc_joint", "join_annual_income")

"""
    adds ingestion_time for future audit/comparisons and returns the dataframe. 
"""
def add_ingestion_time():
    renamed_df = rename_columns()
    return renamed_df.withColumn("ingestion_timestamp",current_timestamp())

"""
    removes the duplicate_rows
"""
def handle_duplication():
    time_added_df = add_ingestion_time()
    return time_added_df.drop_duplicates()

"""
    removes the rows where annual income value is null
"""
def remove_annual_income_nulls():
    corrected_data = handle_duplication()
    return corrected_data.dropna(subset=["annual_income"])

"""
    emp_length is in formats that describe length as >10 years etc.. replacing it with integers
"""
def manage_emp_length():
    cleaned_data = remove_annual_income_nulls()
    return cleaned_data.withColumn("emp_length",regexp_replace(col("emp_length"),"(\D)","")) 
    
"""
    Updating the nulls with avg value
"""
def update_emp_length_nulls():
    emp_length_cleaned_data = manage_emp_length()
    emp_length_cleaned_casted_data = emp_length_cleaned_data.withColumn("emp_length",emp_length_cleaned_data.emp_length.cast('int'))
    avg_value = math.floor(emp_length_cleaned_casted_data.agg(mean(col('emp_length'))).collect()[0][0])
    return emp_length_cleaned_casted_data.fillna(avg_value,subset=["emp_length"])

"""
    Updating adress_state with more than 2 characters with NA. 
"""
def update_address_state():
    updated_data = update_emp_length_nulls()
    return updated_data.withColumn("address_state",when(length(col("address_state"))>2, 'NA').otherwise(col("address_state")))

"""
    Writing cleaned data back to disk
"""

def write_cleaned_data():
    customer_cleaned_data = update_address_state()
    customer_cleaned_data.write.mode("overwrite").parquet("project/cleaned_data/customer_cleaned_data")
    

if __name__ == "__main__":
    write_cleaned_data()
    
    