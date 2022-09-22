import datetime
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark import SQLContext
from pyspark.sql.types import StructType, DoubleType, DateType, StringType, FloatType, TimestampType, StructField, IntegerType, BooleanType
import argparse

timestart = datetime.datetime.now()

'''
parser = argparse.ArgumentParser()
parser.add_argument('--query', required=True, type=str)
parametros = parser.parse_args()
vQueryTmp = parametros.query
'''
spark = SparkSession\
    .builder\
    .appName("PROCESO")\
    .master("yarn")\
    .enableHiveSupport()\
    .getOrCreate()
    
sc = spark.sparkContext
sc.setLogLevel("ERROR")


TDHOST = "proxfulldg1.otecel.com.ec"
TDPORT  = "7594"
TDDB = "tomstby.otecel.com.ec"

#vJar="ojdbc6-11.2.0.3.jar"
vUrl="jdbc:oracle:thin:@proxfulldg1.otecel.com.ec:7594/tomstby.otecel.com.ec"

#jdbc:oracle:thin:@fulldb-scan.otecel.com.ec:7594/sasdb.otecel.com.ec

vClass="oracle.jdbc.driver.OracleDriver"
vUsuario="rdb_reportes"
vClave="TelfEcu2017"

query1 = """ 
r_om_portin_co
"""


df0 = spark.read.format("jdbc")\
    .option("url",vUrl)\
    .option("driver",vClass)\
    .option("user",vUsuario)\
    .option("password",vClave)\
    .option("dbtable",query)\
    .load()
    
df0.printSchema()

df1 = df0.filter(df0.FVC > '01/06/2022')

df1.repartition(20).write.format('hive').format("orc").mode("overwrite").saveAsTable('db_desarrollo2021.r_om_portin_co')

df0.show(5)

spark.stop()
timeend = datetime.datetime.now()
duracion = timeend - timestart
print("Duracion {}".format(duracion))