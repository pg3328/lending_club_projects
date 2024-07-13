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
    The view below gives single consolidated view of the entire data and consists data from all the data sets. 
    @author : Pradeep Kumar Gontla. 
"""
    

spark.sql("""
create or replace view if not exists lending_club_project_pg3328.customers_loan_v as select
l.loan_id,
c.member_id,
c.emp_title,
c.emp_length,
c.home_ownership,
c.annual_income,
c.address_state,
c.address_zipcode,
c.address_country,
c.grade,
c.sub_grade,
c.verification_status,
c.total_high_credit_limit,
c.application_type,
c.join_annual_income,
c.verification_status_joint,
l.loan_amount,
l.funded_amount,
l.loan_term_years,
l.interest_rate,
l.monthly_installment,
l.issue_date,
l.loan_status,
l.loan_purpose,
r.total_principal_received,
r.total_interest_received,
r.total_late_fee_received,
r.last_payment_date,
r.next_payment_date,
d.delinq_2yrs,
d.delinq_amnt,
d.mths_since_last_delinq,
e.pub_rec,
e.pub_rec_bankruptcies,
e.inq_last_6mths

FROM lending_club_project_pg3328.customers c
LEFT JOIN lending_club_project_pg3328.loans l on c.member_id = l.member_id
LEFT JOIN lending_club_project_pg3328.loans_repayments r ON l.loan_id = r.loan_id
LEFT JOIN lending_club_project_pg3328.loans_defaulters_delinq d ON c.member_id = d.member_id
LEFT JOIN lending_club_project_pg3328.loans_defaulters_detail_rec_enq e ON c.member_id = e.member_id
""")