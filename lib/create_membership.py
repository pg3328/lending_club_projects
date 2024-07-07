from pyspark.sql import *

spark = SparkSession.builder \
    .appName('project') \
    .config('spark.ui.port', '0') \
    .config("spark.sql.warehouse.dir", f"/user/itv012667/warehouse") \
    .enableHiveSupport() \
    .master('yarn') \
    .getOrCreate()
print("hello")
print(spark)