from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
import math

"""
    It is observed that there are multiple customers in the tables, cleaning this by ignoring the records and 
    creating the tables after respective cleaning
    @author : Pradeep Kumar Gontla
"""

spark = SparkSession.builder \
    .appName('project') \
    .config('spark.ui.port', '0') \
    .config("spark.sql.warehouse.dir", f"/user/itv012667/warehouse") \
    .config('spark.shuffle.useOldFetchProtocol', 'true') \
    .enableHiveSupport() \
    .master('yarn') \
    .getOrCreate()
    
    
"""
    Create a table with the customers having repeated entries and extract their memberids
"""
bad_customer_data = spark.sql("select member_id from(select member_id, count(*) as total from lending_club_project_pg3328.customers group by member_id having total > 1)")
bad_customer_data.write.mode("overwrite").saveAsTable("lending_club_project_pg3328.bad_customer_data")

"""
    Excluding the members whom we believe have multiple records, so as to get the confirmation from data team. 
"""
def clean_customers_data():
    customers_df = spark.sql(""" select * from lending_club_project_pg3328.customers where member_id not in (select member_id from lending_club_project_pg3328.bad_customer_data)""")
    customers_df.write.mode("overwrite").option("path","project/gold_layer_data/customers").save()
    spark.sql("""
    create EXTERNAL TABLE if not exists lending_club_project_pg3328.customers_new (member_id string, emp_title string, emp_length int, home_ownership string, 
    annual_income float, address_state string, address_zipcode string, address_country string, grade string, 
    sub_grade string, verification_status string, total_high_credit_limit float, application_type string, 
    join_annual_income float, verification_status_joint string, ingest_date timestamp)
    stored as parquet
    LOCATION '/public/trendytech/lendingclubproject/cleaned_new/customer_parquet'""")
    

"""
    Excluding the members whom we believe have multiple records, so as to get the confirmation from data team. 
"""
def clean_delinq_data():
    loans_defaulters_delinq_df = spark.sql("""select * from lending_club_project_pg3328.loans_defaulters_delinq  where member_id NOT IN (select member_id from lending_club_project_pg3328.bad_customer_data)""")
    loans_defaulters_delinq_df.write.mode("overwrite").option("path","project/gold_layer_data/loan_defaulters_delinq_parquet").save()
    spark.sql("""
    create EXTERNAL TABLE if not exists lending_club_project_pg3328.loans_defaulters_delinq_new(member_id string,delinq_2yrs integer, delinq_amnt float, mths_since_last_delinq integer)stored as parquet LOCATION '/public/trendytech/lendingclubproject/cleaned_new/loan_defaulters_delinq_parquet'""")
    

"""
    Excluding the members whom we believe have multiple records, so as to get the confirmation from data team. 
"""
def clean_loans_defaulters_public():
    loans_defaulters_detail_rec_enq_df = spark.sql("""select * from lending_club_project_pg3328.loans_defaulters_detail_rec_enq
    where member_id NOT IN (select member_id from lending_club_project_pg3328.bad_customer_data)""")
    loans_defaulters_detail_rec_enq_df.write.mode("overwrite").option("path","project/gold_layer_data/loans_defaulters_detail_rec_enq_parquet").save()
    spark.sql("""create EXTERNAL TABLE lending_club_project_pg3328.loans_defaulters_detail_rec_enq_new(member_id string, pub_rec integer, pub_rec_bankruptcies integer, inq_last_6mths integer) stored as parquet LOCATION '/public/trendytech/lendingclubproject/cleaned_new/loan_defaulters_detail_rec_enq_parquet'""")

if __name__ == "__main__":
    clean_customers_data()
    clean_delinq_data()
    clean_loans_defaulters_public()

    
    
    

