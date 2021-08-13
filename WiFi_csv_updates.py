from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import pyspark.sql.functions as F
import os

sc = SparkContext('local')
spark = SparkSession(sc)

df_ap = spark.read.csv('ap_data/18676_dump.csv', header = True, inferSchema = True, sep = ",", nullValue = '') 
df_ap = df_ap.withColumnRenamed('erm__serial_number', 'SERIAL_NO')
columns_to_drop = df_ap.columns[2:]
columns_to_drop.append('_c0')

allLines = []
path = 'Updated_WiFi_CIR/'
fileList = os.listdir(path)
for f in fileList:
    df_wifi = spark.read.csv('{}{}'.format(path, f), header=True, inferSchema=True, sep=",", nanValue='', nullValue='')
    df_wifi = df_wifi.na.replace('No', None)
    df = df_wifi.join(df_ap, 'SERIAL_NO', 'left').fillna('').drop(*columns_to_drop)
    df.toPandas().to_csv('{}{}'.format('Updated_WiFi_2/', f)) 


