import os
import lxml
import re
import bs4
import numpy as np
import glob
import pandas as pd
import pyspark
import pyarrow
import fastparquet
import re
from bs4 import BeautifulSoup as bs
from IPython.core.display import display, HTML
import pprint
from pyspark.sql.functions import lit 
from pyspark.sql.functions import input_file_name
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import *

from common_functions import time_log_start, time_log_finish

time_log_start()
from pyspark.sql import SparkSession
spark = SparkSession \
        .builder \
        .appName('Python Example - PySpark Read XML') \
        .enableHiveSupport() \
        .config("hive.exec.dynamic.partition", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .getOrCreate()
sc = spark.sparkContext

time_log_finish()
###Function to create a list of lists

def listoflists(lst):
    return [[el] for el in lst]
###############################
##  Getting a list of xml files
###############################


fp_1 = '/mapr/datalake/optum/optuminsight/opsirsch1/dsar/CCD'
os.chdir(fp_1)
list_xml_all = glob.glob("/mapr/datalake/optum/optuminsight/opsirsch1/dsar/CCD/*.xml")
print('Number of files =',len(list_xml_all))
#print(list_xml_all[0:3])
#print(os.getcwd())

lol = listoflists(list_xml_all)
#print(lol[0:3])

list_xml_short = list_xml_all[0:3]
lst1 = ([s.replace('/mapr','') for s in list_xml_all])
print(lst1[0:2])

df_lst1 = pd.DataFrame(lst1,columns={'filename'})

df= spark.read.format("xml")\
              .option("rowTag", "section")\
              .load(','.join(lst1))\
              .withColumn("filename",input_file_name())

df = df.withColumn('title', regexp_replace('title', '\d{4}\-\d{2}\-\d{2}', ''))
df = df.withColumn('title', rtrim(df.title))
df = df.withColumn('title', regexp_replace('title', ' ', '_'))

df = df.withColumn('filename',regexp_replace('filename','maprfs:///datalake/optum/optuminsight/opsirsch1/dsar/CCD/',''))
df = df.withColumn('text', regexp_replace('text', '<[^>]*>', ''))
df = df.withColumn('text', regexp_replace('text', '\n', ''))

# Saving the file
os.chdir('/mapr/datalake/optum/optuminsight/opsirsch1/dsar/Developer2/jmagouri/projects/CCD/Data/')
df.createOrReplaceTempView("XML_DATA")
df2 = spark.sql("Select title, text, filename from XML_DATA")

print('df2 =',df2.show(n=3))
df3=(df2
     .groupby(df['filename'])
     .pivot("title")
    .agg(first("text")))


df3.write.mode("overwrite").parquet('file041521_1.parquet')
print('df3 =',df3.show(n=3))

df_pd = df3.toPandas()
print(df_pd.columns)


