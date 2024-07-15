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
    This file helps in generating tables for the cleaned data. This will help in sharing the access with multiple teams 
    to view the data and will answer the queries as required.
    @author : Pradeep Kumar Gontla
"""
spark.sql("create database if not exists lending_club_project_pg3328")

"""
    create external tables for each of the cleaned dataframe.
"""

spark.sql("""create external table if not exists lending_club_project_pg3328.customers(
member_id string, emp_title string, emp_length int, 
home_ownership string, annual_income float, address_state string, address_zipcode string, address_country string, grade string, 
sub_grade string, verification_status string, total_high_credit_limit float, application_type string, join_annual_income float, 
verification_status_joint string, ingest_date timestamp)
stored as parquet location  'project/cleaned_data/customer_cleaned_data'
""")

spark.sql("""
create external table if not exists lending_club_project_pg3328.loans(
loan_id string, member_id string, loan_amount float, funded_amount float,
loan_term_years integer, interest_rate float, monthly_installment float, issue_date string,
loan_status string, loan_purpose string, loan_title string, ingest_date timestamp)
stored as parquet
location 'project/cleaned_data/loans_cleaned_data'
""")

spark.sql("drop table if exists lending_club_project_pg3328.loans_repayments")
spark.sql("""CREATE EXTERNAL TABLE if not exists lending_club_project_pg3328.loans_repayments(loan_id string, total_principal_received float,
total_interest_received float,total_late_fee_received float,total_payment_received float,last_payment_amount float,
last_payment_date string,next_payment_date string,ingest_date timestamp)
stored as parquet LOCATION 'project/cleaned_data/loans_repayment_cleaned_data'
""")

spark.sql("""CREATE EXTERNAL TABLE if not exists lending_club_project_pg3328.loans_defaulters_delinq(
member_id string, delinq_2yrs integer, delinq_amnt float, mths_since_last_delinq integer)
stored as parquet LOCATION 'project/cleaned_data/delinq_data_csv'""")

spark.sql("""CREATE EXTERNAL TABLE if not exists lending_club_project_pg3328.loans_defaulters_detail_rec_enq(
member_id string, pub_rec integer, pub_rec_bankruptcies integer, inq_last_6mths integer)
stored as parquet LOCATION 'project/cleaned_data/loans_def_detail_records_enq_df_csv'""")