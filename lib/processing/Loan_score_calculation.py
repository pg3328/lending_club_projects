from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
import math

"""
   To Calculate Loan score, the assumption is that the loan score is dependent on
    
    1) Payment History
    2) Previous Loan Defaults
    3) Financial Health. 
    
    This module calculates the loan score and update the table. 
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
    variables used to describe the points allocated as per the data. 
"""
def declare_the_variables():
    spark.conf.set("spark.sql.unacceptable_rated_pts", 0)
    spark.conf.set("spark.sql.very_bad_rated_pts", 100)
    spark.conf.set("spark.sql.bad_rated_pts", 250)
    spark.conf.set("spark.sql.good_rated_pts", 500)
    spark.conf.set("spark.sql.very_good_rated_pts", 650)
    spark.conf.set("spark.sql.excellent_rated_pts", 800)
    spark.conf.set("spark.sql.unacceptable_grade_pts", 750)
    spark.conf.set("spark.sql.very_bad_grade_pts", 1000)
    spark.conf.set("spark.sql.bad_grade_pts", 1500)
    spark.conf.set("spark.sql.good_grade_pts", 2000)
    spark.conf.set("spark.sql.very_good_grade_pts", 2500)
    
    
"""
    Calculates the Payment History
    
"""
def calculate_payment_history():
   spark.sql("select c.member_id, \
   case \
   when p.last_payment_amount < (c.monthly_installment * 0.5) then ${spark.sql.very_bad_rated_pts} \
   when p.last_payment_amount >= (c.monthly_installment * 0.5) and p.last_payment_amount < c.monthly_installment then ${spark.sql.very_bad_rated_pts} \
   when (p.last_payment_amount = (c.monthly_installment)) then ${spark.sql.good_rated_pts} \
   when p.last_payment_amount > (c.monthly_installment) and p.last_payment_amount <= (c.monthly_installment * 1.50) then ${spark.sql.very_good_rated_pts} \
   when p.last_payment_amount > (c.monthly_installment * 1.50) then ${spark.sql.excellent_rated_pts} \
   else ${spark.sql.unacceptable_rated_pts} \
   end as last_payment_pts, \
   case \
   when p.total_payment_received >= (c.funded_amount * 0.50) then ${spark.sql.very_good_rated_pts} \
   when p.total_payment_received < (c.funded_amount * 0.50) and p.total_payment_received > 0 then ${spark.sql.good_rated_pts} \
   when p.total_payment_received = 0 or (p.total_payment_received) is null then ${spark.sql.unacceptable_rated_pts} \
   end as total_payment_pts \
    from itv005857_lending_club.loans_repayments p \
    inner join itv005857_lending_club.loans c on c.loan_id = p.loan_id")