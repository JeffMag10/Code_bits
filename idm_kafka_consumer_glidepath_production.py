##Creating the time logs

def time_log_start():
    import datetime
    now = datetime.datetime.now()
    print("Start date and time :")
    print(now.strftime("%Y-%m-%d %H:%M:%S"))
    
    
def time_log_finish():
    import datetime
    now = datetime.datetime.now()
    print("Finish date and time :")
    print(now.strftime("%Y-%m-%d %H:%M:%S"))

print("#import dependencies")
time_log_start()
from pyspark.sql.functions import udf
import io, json
from common_functions import time_log_start, time_log_finish
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

print('Start of Spark session')
time_log_start()
spark = SparkSession \
         .builder \
         .appName('FTM-IDM Integration') \
         .enableHiveSupport() \
         .config("hive.exec.dynamic.partition", "true") \
         .config("hive.exec.dynamic.partition.mode", "nonstrict") \
         .getOrCreate()
sc = spark.sparkContext
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
time_log_finish()

time_log_start()
print('Create functions')
# utf-8 decode
def utf8_decode(yoyo):
    decoded = yoyo.decode('utf-8')
    return decoded
spark.udf.register("uncode", utf8_decode, StringType())

# deserialize single-column json
def inject_id(row):
    js = json.loads(row['value'])
# js['group_id'] = row['group_id']
    return json.dumps(js)
time_log_finish()

time_log_start()
# print("consume data from 'dev' into dataframe")
# df = spark \
# .read 
# .format("kafka") \
# .option("kafka.bootstrap.servers", "10.86.165.44:9092,10.204.108.255:9092,10.204.108.255:9092") \
# .option("subscribe", "IDM.FTM.Integration") \
# .load()
# df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# print("consume data from 'stage_dz' into dataframe")
# df = spark \
# .read \
# .format("kafka") \
# .option("kafka.bootstrap.servers", "apsrs9002:9092,apsrs9004:9092,apsrs9006:9092") \
# .option("subscribe", "Test.BPM") \
# .load()
# df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

print("consume data from 'prod_dz' into dataframe")
df = spark \
   .read \
   .format("kafka") \
   .option("kafka.bootstrap.servers", "10.207.148.104:9092,10.201.68.38:9092,10.207.150.105:9092") \
   .option("subscribe", "IDM.Glidepath") \
   .load()
time_log_finish()
print('Count the records - filtering on key')
time_log_start()
df_0 = df.filter(df.key=='glidepath.prod.20210415_1')
print('key count =',df_0.count())
time_log_finish()

print('Count the records of last records entered')
time_log_start()
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
print(df.count())
print("filter only the latest data")
time_log_finish()