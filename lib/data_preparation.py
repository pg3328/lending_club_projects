from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
"""
This file describes the precleaning state of the data . Steps for data preparation 

1) added customer id based on a few other columns using sha2 hashing. 
2) Divided the dataset into 
    a) customer_data - with customer information.
    b) loans_data - with loan imformation.
    c) loans_repayments_data - how a particular loan repayment has been.
    d) loans_defaulter_data - Public information  about a client on his/her loan defaulting status. 

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
To get the memberid , the assumption is that the customer will have the same details for below columns. Hence, we are concatinating the Emp_title, emp_length, home_ownership, annual_inc, zip_code, addr_state, grade, sub_grad, verification status and treating the hash generated as memberid
"""

def add_member_id():
    raw_df = spark.read.csv("/user/itv012667/accepted_2007_to_2018Q4.csv",header=True,inferSchema=True)
    raw_df = raw_df.withColumn("name_sha2",sha2(concat_ws("||",*["emp_title","emp_length","home_ownership","annual_inc","zip_code","addr_state","grade","sub_grade","verification_status"]),256))
    raw_df.createOrReplaceTempView("complete_data")

"""
generate customer data and create a spark managed table as well as write it on the disk.

"""
def generate_customer_data():
    spark.sql("""select name_sha2 as member_id,emp_title,emp_length,home_ownership,annual_inc,addr_state,zip_code,'USA' as country,grade,sub_grade,
    verification_status,tot_hi_cred_lim,application_type,annual_inc_joint,verification_status_joint from complete_data
    """).repartition(1).write \
    .option("header","true")\
    .format("csv") \
    .mode("overwrite") \
    .option("path", "project/raw_divided_data/customers_data_csv") \
    .save()
    
"""
generates loan data and create a spark managed table as well as write it on the disk.

"""
def generate_loans_data():
    spark.sql("""select id as loan_id, name_sha2 as member_id,loan_amnt,funded_amnt,term,int_rate,installment,issue_d,loan_status,purpose,
    title from complete_data""").repartition(1).write \
    .option("header",True)\
    .format("csv") \
    .mode("overwrite") \
    .option("path", "project/raw_divided_data/loans_data_csv") \
    .save()

"""
generates loan repayments data and create a spark managed table as well as write it back to the disk. 

"""
def generate_loans_repayments_data():
    spark.sql("""select id as loan_id,total_rec_prncp,total_rec_int,total_rec_late_fee,total_pymnt,last_pymnt_amnt,last_pymnt_d,next_pymnt_d from complete_data""").repartition(1).write \
    .option("header",True)\
    .format("csv") \
    .mode("overwrite") \
    .option("path", "project/raw_divided_data/loans_repayments_csv") \
    .save()
    
"""
generates loan defaults data and create a spark managed table as well as write it back to the disk. 

"""

def generate_loans_defaulter_data():
    spark.sql("""select name_sha2 as member_id,delinq_2yrs,delinq_amnt,pub_rec,pub_rec_bankruptcies,inq_last_6mths,total_rec_late_fee,mths_since_last_delinq,mths_since_last_record from complete_data""").repartition(1).write \
    .option("header",True)\
    .format("csv") \
    .mode("overwrite") \
    .option("path", "project/raw_divided_data/loans_defaulters_csv") \
    .save()
    

if __name__ == "__main__":
    add_member_id()
    generate_customer_data()
    generate_loans_data()
    generate_loans_defaulter_data()
    generate_loans_repayments_data()