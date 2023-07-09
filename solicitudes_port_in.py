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
    .config("hive.exec.dynamic.partition", "true")\
    .config("hive.exec.dynamic.partition.mode", "nonstrict")\
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

qry = """
select 
a.telefono
,a.numero_abonado
,a.fecha_alta
from db_cs_altas.otc_t_altas_bi a	 
where a.p_fecha_proceso = 20230117
and a.marca='TELEFONICA'
"""

df_a = spark.sql(qry)
df_a.count()





query = """ 
(select   case when cr.cust_acct_number is null then cb.cust_acct_number else cr.cust_acct_number end as CustomerAccountNumber,
case when cr.doc_number is null then cb.doc_number else cr.doc_number end doc_number,
case when (select y.value from nc_list_values y where y.list_value_id = cb.doc_type) is null then
(select y.value from nc_list_values y where y.list_value_id = cr.doc_type)  else
(select y.value from nc_list_values y where y.list_value_id = cb.doc_type) end as doc_type,
ctg.name as CustomerCategory,
to_char( a.object_id) as PortinCommonOrderID,
(select lv.value from nc_list_values lv where lv.list_value_id = a.request_status) as RequestStatus,
(select lv.localized_value from vw_list_values lv where lv.list_value_id = a.ascp_response) as estado,
to_char(a.fvc,'DD-MM-YYYY') as fvc,
donor.name as operadora,
to_char(a.donor_account_type) as donor_account_type1,
(select lv.value from nc_list_values lv where lv.list_value_id = a.donor_account_type) as LN_Origen,
u.name as AssignedCSR,
a.created_when as created_when,
to_char(o.object_id) as SalesOrderID,
(select lv.value from nc_list_values lv where lv.list_value_id = o.sales_ord_status) as SalesOrderStatus,
o.processed_when as SalesOrderProcessedDate,
ri.name as telefono,
substr(sim.name,1,19) as AssociatedSIMICCID,
oi.tariff_plan_name as PlanDestino,
a.ascp_rejection_comment  as motivo_rechazo
from R_OM_PORTIN_CO a
left join PROXTOMSREP_RDB.R_OM_PORTIN_CO_MPN_SR mpn on mpn.object_id = a.object_id
left join R_RI_MOBILE_PHONE_NUMBER ri on  mpn.value = ri.object_id
left join R_USR_USERS u on a.assigned_csr = u.object_id
left join R_PMGT_STORE t on t.object_id = u.current_location
left join R_CIM_RES_CUST_ACCT cr on a.customer_account = cr.object_id
left join R_CIM_BSNS_CUST_ACCT cb on a.customer_account = cb.object_id
left join R_PIM_CUST_CATEGORY ctg on ctg.object_id = nvl(cb.cust_category, cr.cust_category)
left join R_RI_NUMBER_OWNER donor on donor.object_id = a.donor_operator
left join R_AM_SIM sim on sim.object_id = ri.assoc_sim_iccid
left join R_EH_ERROR_RECORD err on err.failed_order = a.object_id
join R_BOE_SALES_ORD o on o.object_id = a.sales_order    
left join R_USR_USERS u1 on u1.object_id = o.submitted_by
left join R_BOE_ORD_ITEM oi on oi.parent_id = a.sales_order and oi.phone_number = ri.object_id)
"""
## where a.created_when >= to_date('010322','ddmmyy') and  a.created_when < to_date('311222','ddmmyy')

df0 = spark.read.format("jdbc")\
    .option("url",vUrl)\
    .option("driver",vClass)\
    .option("user",vUsuario)\
    .option("password",vClave)\
    .option("dbtable",query)\
    .load()
    
df0.printSchema()

#df1 = df0.filter(df0.FVC > '01/06/2022')

df0.repartition(20).write.format('hive').format("parquet").mode("overwrite").saveAsTable('db_desarrollo2021.sol_port_in_3')

#df0.show(5)

spark.stop()
timeend = datetime.datetime.now()
duracion = timeend - timestart
print("Duracion {}".format(duracion))

#/usr/hdp/current/spark2-client/bin/pyspark --master yarn --executor-memory 16G --num-executors 4 --executor-cores 4 --driver-memory 16G --jars /home/nae108834/D_SOLICITUDES_PORT_IN/lib/ojdbc8.jar

/usr/hdp/current/spark2-client/bin/pyspark --master yarn --executor-memory 16G --num-executors 4 --executor-cores 4 --driver-memory 16G \
--conf spark.ui.enabled=false \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.port.maxRetries=100 