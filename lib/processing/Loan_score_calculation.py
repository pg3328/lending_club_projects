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