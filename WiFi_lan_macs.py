from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)
import pyspark.sql.functions as F
import os

df_lan_macs = spark.read.csv('lan_mac_data/updated_lan_macs.csv', header = True, inferSchema = True, sep = ",", nullValue = '') 
allLines = []
columns_to_drop = []
path = 'WiFi_CIR/'
fileList = os.listdir(path)
for f in fileList:
    df_wifi = spark.read.csv('Wifi_CIR/{}'.format(f), header=True, inferSchema=True, sep=",", nanValue='', nullValue='')
    df_wifi = df_wifi.na.replace('No', None)

    df = df_wifi.join(df_lan_macs, 'SERIAL_NO', 'left').fillna('').drop(*columns_to_drop)
    df.toPandas().to_csv("Updated " + f)


