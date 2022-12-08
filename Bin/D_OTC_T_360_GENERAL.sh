#!/bin/bash
##########################################################################
#   Script de carga de Generica para entidades de URM con reejecución    #
# Creado 13-Jun-2018 (LC) Version 1.0                                    #
# Las tildes han sido omitidas intencionalmente en el script             #
#------------------------------------------------------------------------#
# REFACTORING: CRISTIAN ORTIZ											 #
# MODIFICADO : 26/JUN/2022												 #
# COMENTARIO: Se hizo un REFACTORING de este proceso para el BIGD-677    #
# "EXTRACTOR DE MOVIMIENTOS" 											 #
##########################################################################
#------------------------------------------------------
# VARIABLES CONFIGURABLES POR PROCESO (MODIFICAR)
#------------------------------------------------------

# ENT. ANTERIOR ----> ENTIDAD=OTC_T_360_GENERAL
## ENTIDAD=D_OTC_T_360_GENERAL
ENTIDAD=D_CLI360CLI00100
# AMBIENTE (1=produccion, 0=desarrollo)
((AMBIENTE=0))
FECHAEJE=$1 # yyyyMMdd
# Variable de control de que paso ejecutar
PASO=$2
### la linea de abajo se ha comentado para traer el valor correspondiente por parametro desde MySQL:
#ESQUEMA_TEMP=db_temporales
#ESQUEMA_TEMP=db_desarrollo2021

#PARAMETROS GENERICOS
VAL_CADENA_JDBC=`mysql -N  <<<"select valor from params_des where ENTIDAD = 'D_PARAM_BEELINE' AND parametro = 'VAL_CADENA_JDBC';"`
VAL_COLA_EJECUCION=`mysql -N  <<<"select valor from params_des where ENTIDAD = 'D_PARAM_BEELINE' AND parametro = 'VAL_COLA_EJECUCION';"`
VAL_USER=`mysql -N  <<<"select valor from params_des where ENTIDAD = 'D_OTC_T_360_GENERAL' AND parametro = 'VAL_USER';"`

#*****************************************************************************************************#
#                                            ¡¡ ATENCION !!                                           #
#                                                                                                     #
# Configurar las siguientes  consultas de acuerdo al orden de la tabla params de la base de datos URM #
# en el servidor 10.112.152.183                                                                       #
#*****************************************************************************************************#

isnum() { awk -v a="$1" 'BEGIN {print (a == a + 0)}'; }

function isParamListNum() #parametro es el grupo de valores separados por ;
{
	local value
	local isnumPar
	for value in `echo "$1" | sed -e 's/;/\n/g'`
	do
		isnumPar=`isnum "$value"`
		if [  "$isnumPar" ==  "0" ]; then
			((rc=999))
			echo " `date +%a" "%d"/"%m"/"%Y" "%X` [ERROR] $rc Parametro $value $2 no son numericos"
			exit $rc
		fi
	done	     

}  

#Verificar que la configuración de la entidad exista
if [ "$AMBIENTE" = "1" ]; then
	ExisteEntidad=`mysql -N  <<<"select count(*) from params where entidad = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"');"` 
else
	ExisteEntidad=`mysql -N  <<<"select count(*) from params_des where entidad = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"');"` 
fi
 
if ! [ "$ExisteEntidad" -gt 0 ]; then #-gt mayor a -lt menor a
   echo " $TIME [ERROR] $rc No existen parametros para la entidad $ENTIDAD"
	((rc=1))
	exit $rc
fi

# Verificacion de fecha de ejecucion
if [ -z "$FECHAEJE" ]; then #valida que este en blanco el parametro
	((rc=2))
	echo " $TIME [ERROR] $rc Falta el parametro de fecha de ejecucion del programa"
	exit $rc
fi

	
if [ "$AMBIENTE" = "1" ]; then
	# Cargar Datos desde la base
	VAL_RUTA=`mysql -N  <<<"select valor from params where entidad = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') AND parametro = 'VAL_RUTA';"` 
	PESOS_PARAMETROS=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'PESOS_PARAMETROS' );"`
	PESOS_NSE=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'PESOS_NSE' );"`
	TOPE_RECARGAS=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'TOPE_RECARGAS' );"`
	TOPE_TARIFA_BASICA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'TOPE_TARIFA_BASICA' );"`
	ESQUEMA_TABLA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'ESQUEMA_TABLA' );"`
	ESQUEMA_TABLA_1=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'ESQUEMA_TABLA_1' );"`
	ESQUEMA_TABLA_2=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'ESQUEMA_TABLA_2' );"`
	ESQUEMA_TABLA_3=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'ESQUEMA_TABLA_3' );"`
	VAL_SQL_1=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'VAL_SQL_1' );"`
	VAL_SQL_2=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'VAL_SQL_2' );"`
	ESQUEMA_TEMP=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND PARAMETRO = 'ESQUEMA_TEMP';"`
	ESQUEMA_CS_ALTAS=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND PARAMETRO = 'ESQUEMA_CS_ALTAS';"`
	ESQUEMA_REPORTES=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND PARAMETRO = 'ESQUEMA_REPORTES';"` 
else 
	# Cargar Datos desde la base
	VAL_RUTA=`mysql -N  <<<"select valor from params_des where entidad = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') AND parametro = 'VAL_RUTA';"` 
	PESOS_PARAMETROS=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'PESOS_PARAMETROS' );"`
	PESOS_NSE=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'PESOS_NSE' );"`
	TOPE_RECARGAS=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'TOPE_RECARGAS' );"`
	TOPE_TARIFA_BASICA=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'TOPE_TARIFA_BASICA' );"`
	ESQUEMA_TABLA=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'ESQUEMA_TABLA' );"`
	ESQUEMA_TABLA_1=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'ESQUEMA_TABLA_1' );"`
	ESQUEMA_TABLA_2=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'ESQUEMA_TABLA_2' );"`  
	ESQUEMA_TABLA_3=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'ESQUEMA_TABLA_3' );"`  
	VAL_SQL_1=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'VAL_SQL_1' );"`
	VAL_SQL_2=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'VAL_SQL_2' );"`
	ESQUEMA_TEMP=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND PARAMETRO = 'ESQUEMA_TEMP';"` 
	ESQUEMA_CS_ALTAS=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND PARAMETRO = 'ESQUEMA_CS_ALTAS';"` 
	ESQUEMA_REPORTES=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND PARAMETRO = 'ESQUEMA_REPORTES';"` 
fi	
	
 #Verificar si tuvo datos de la base
TIME=`date +%a" "%d"/"%m"/"%Y" "%X`
if [ -z "$VAL_RUTA" ]; then
((rc=3))
echo " $TIME [ERROR] $rc No se han obtenido los valores necesarios desde la base de datos"
exit $rc
fi

if [ -z "$PESOS_PARAMETROS" ]; then
	((rc=999))
	echo " `date +%a" "%d"/"%m"/"%Y" "%X` [ERROR] $rc Falta el parametro de los pesos para calculo de nse global"
	exit $rc
else 
	if [ `echo "$PESOS_PARAMETROS" | sed -e 's/;/\n/g' |wc -l` -ne 7 ]; then
		((rc=999))
		TIME=`date +%a" "%d"/"%m"/"%Y" "%X`
		echo " `date +%a" "%d"/"%m"/"%Y" "%X` [ERROR] $rc Numero de pesos para calculo global incorrecto"
		exit $rc
	fi
	isParamListNum $PESOS_PARAMETROS "PESOS_PARAMETROS"
fi


if [ -z "$TOPE_RECARGAS" ]; then
	((rc=999))
	echo " `date +%a" "%d"/"%m"/"%Y" "%X` [ERROR] $rc Falta el parametro de dia tope recargas del programa"
	exit $rc
fi	

if [ -z "$TOPE_TARIFA_BASICA" ]; then
	((rc=999))
	echo " `date +%a" "%d"/"%m"/"%Y" "%X` [ERROR] $rc Falta el parametro de dia tope tarifa basica del programa"
	exit $rc
fi		

nse_peso_global=(`echo "$PESOS_PARAMETROS" | sed -e 's/;/\n/g'`)

nse_peso_global_nse=(`echo "$PESOS_NSE" | sed -e 's/;/\n/g'`)	

# Verificacion de re-ejecucion
if [ -z "$PASO" ]; then
	PASO=0
	echo " $TIME [INFO] $rc Este es un proceso normal"
else
	echo " $TIME [INFO] $rc Este es un proceso de re-ejecucion"

fi
#------------------------------------------------------
# VARIABLES DE OPERACION Y AUTOGENERADAS
#------------------------------------------------------
   
EJECUCION=$ENTIDAD"_"$FECHAEJE
#DIA: Obtiene la fecha del sistema
DIA=`date '+%Y%m%d'` 
#HORA: Obtiene hora del sistema
HORA=`date '+%H%M%S'` 
# rc es una variable que devuelve el codigo de error de ejecucion
((rc=0)) 
#EJECUCION_LOG Entidad_Fecha_hora nombre del archivo log
EJECUCION_LOG=$EJECUCION"_"$DIA$HORA		
#LOGS es la ruta de carpeta de logs por entidad
LOGS=$VAL_RUTA/Log
VAL_RUTA_ARCHIVO=$VAL_RUTA/input

version=1.2.1000.2.6.4.0-91
HADOOP_CLASSPATH=$(hcat -classpath) export HADOOP_CLASSPATH

HIVE_HOME=/usr/hdp/current/hive-client
HCAT_HOME=/usr/hdp/current/hive-webhcat
SQOOP_HOME=/usr/hdp/current/sqoop-client

export LIB_JARS=$HCAT_HOME/share/hcatalog/hive-hcatalog-core-${version}.jar,${HIVE_HOME}/lib/hive-metastore-${version}.jar,$HIVE_HOME/lib/libthrift-0.9.3.jar,$HIVE_HOME/lib/hive-exec-${version}.jar,$HIVE_HOME/lib/libfb303-0.9.3.jar,$HIVE_HOME/lib/jdo-api-3.0.1.jar,$SQOOP_HOME/lib/slf4j-api-1.7.7.jar,$HIVE_HOME/lib/hive-cli-${version}.jar

#------------------------------------------------------
# VERIFICACION INICIAL 
#------------------------------------------------------

#Verificar si existe la ruta de sistema 
if ! [ -e "$VAL_RUTA" ]; then
	((rc=10))
	echo "$TIME [ERROR] $rc la ruta provista en el script no existe en el sistema o no tiene permisos sobre la misma. Cree la ruta con los permisos adecuados y vuelva a ejecutar el programa"
	exit $rc
else 
	if ! [ -e "$LOGS" ]; then
		mkdir -p $VAL_RUTA/log
			if ! [ $? -eq 0 ]; then
				((rc=11))
				echo " $TIME [ERROR] $rc no se pudo crear la ruta de logs"
				exit $rc
			fi
	fi
fi

#VALIDACION DE PARAMETROS INICIALES
if [ -z "$ENTIDAD" ] || [ -z "$VAL_USER" ] || [ -z "$VAL_CADENA_JDBC" ] || [ -z "$VAL_COLA_EJECUCION" ] || [ -z "$ESQUEMA_TABLA" ] || [ -z "$ESQUEMA_TABLA_1" ] || [ -z "$ESQUEMA_TABLA_2" ] || [ -z "$ESQUEMA_TABLA_3" ] || [ -z "$VAL_SQL_1" ] || [ -z "$VAL_SQL_2" ] || [ -z "$ESQUEMA_TEMP" ] || [ -z "$ESQUEMA_CS_ALTAS" ] || [ -z "$ESQUEMA_REPORTES" ] ; then 
	echo " ERROR - uno de los parametros esta vacio o nulo"
	exit 1
fi

#------------------------------------------------------
# DEFINICION DE FECHAS
#------------------------------------------------------

eval year=`echo $FECHAEJE | cut -c1-4`
eval month=`echo $FECHAEJE | cut -c5-6`
day="01"
fechaMes=$year$month
fechaIniMes=$year$month$day                            #Formato YYYYMMDD
fecha_eje1=`date '+%Y-%m-%d' -d "$FECHAEJE"`
let fecha_hoy=$fecha_eje1
fecha_eje2=`date '+%Y%m%d' -d "$FECHAEJE"`
let fecha_proc1=$fecha_eje2
fecha_eje4=`date '+%d-%m-%Y' -d "$FECHAEJE"`
let fecha_g=$fecha_eje4
fecha_inico_mes_1_1=`date '+%Y-%m-%d' -d "$fechaIniMes"`
let fechainiciomes=$fecha_inico_mes_1_1
fecha_inico_mes_1_2=`date '+%Y%m%d' -d "$fechaIniMes"`
let fechainiciomes=$fecha_inico_mes_1_2
fecha_eje3=`date '+%Y%m%d' -d "$FECHAEJE-1 day"`
let fecha_proc_menos1=$fecha_eje3
fechamas1=`date '+%Y%m%d' -d "$FECHAEJE+1 day"`
let fecha_mas_uno=$fechamas1
#let fechaInimenos1mes=$fechaInimenos1mes_1*1
#fechamas1_1=`date '+%Y%m%d' -d "$FECHAEJE+1 day"`						  
#let fechaInimenos1mes=$fechaInimenos1mes_1*1
fechamas1_1=`date '+%Y%m%d' -d "$FECHAEJE+1 day"`
let fechamas11=$fechamas1_1*1
fechamenos1mes_1=`date '+%Y%m%d' -d "$FECHAEJE-1 month"`
let fechamenos1mes=$fechamenos1mes_1*1
fechamenos2mes_1=`date '+%Y%m%d' -d "$fechamenos1mes-1 month"`
let fechamenos2mes=$fechamenos2mes_1*1
fechamenos6mes_1=`date '+%Y%m%d' -d "$fechamenos1mes-6 month"`
let fechamenos6mes=$fechamenos6mes_1*1  
fechaInimenos1mes_1=`date '+%Y%m%d' -d "$fechaIniMes-1 month"`
let fechaInimenos1mes=$fechaInimenos1mes_1*1
fechaInimenos2mes_1=`date '+%Y%m%d' -d "$fechaIniMes-2 month"`
let fechaInimenos2mes=$fechaInimenos2mes_1*1
fechaInimenos3mes_1=`date '+%Y%m%d' -d "$fechaIniMes-3 month"`
let fechaInimenos3mes=$fechaInimenos3mes_1*1
fechamenos5_1=`date '+%Y%m%d' -d "$FECHAEJE-10 day"`
let fechamenos5=$fechamenos5_1*1
fecha_no_reciclable=`date '+%Y-%m-%d' -d "$FECHAEJE"`
let fecha_n_r=$fecha_no_reciclable
fecha_mes_desp=`date -d "$FECHAEJE-1 month" "+%Y%m"`
fecha_port_ini=`date -d "$FECHAEJE-2 month" "+%Y-%m-%d"`
fecha_port_fin=`date -d "$FECHAEJE" "+%Y-%m-%d"`
#------------------------------------------------------
# CREACION DE LOGS 
#------------------------------------------------------
    #Verificar si hay parámetro de re-ejecución
if [ "$PASO" = "0" ]; then

	echo $DIA-$HORA" Creacion de directorio para almacenamiento de logs" 
	
	# Comprobacion ruta de trabajo
	if  [ -e "$LOGS" ]; then
		echo $DIA-$HORA" Directorio "$LOGS " ya existe"			
	else
		#Cree el directorio LOGS para la ubicacion ingresada		
		mkdir -p $LOGS
		#Validacion de greacion completa
		if  ! [ -e "$LOGS" ]; then
		(( rc = 21)) 
		echo $DIA-$HORA" Error $rc : La ruta $LOGS no pudo ser creada" 
		exit $rc
		fi
	fi

	# CREACION DEL ARCHIVO DE LOG 
	echo "# Entidad: "$ENTIDAD" Fecha: "$FECHAEJE $DIA"-"$HORA > $LOGS/$EJECUCION_LOG.log
	if [ $? -eq 0 ];	then
		echo "# Fecha de inicio: "$DIA" "$HORA >> $LOGS/$EJECUCION_LOG.log
		echo "---------------------------------------------------------------------" >> $LOGS/$EJECUCION_LOG.log
	else
		(( rc = 22))
		echo $DIA-$HORA" Error $rc : Fallo al crear el archivo de log $LOGS/$EJECUCION_LOG.log"
		exit $rc
	fi
	
	# CREACION DE ARCHIVO DE ERROR 
	
	echo "# Entidad: "$ENTIDAD" Fecha: "$FECHAEJE $DIA"-"$HORA > $LOGS/$EJECUCION_LOG.log
	if [ $? -eq 0 ];	then
		echo "# Fecha de inicio: "$DIA" "$HORA >> $LOGS/$EJECUCION_LOG.log
		echo "---------------------------------------------------------------------" >> $LOGS/$EJECUCION_LOG.log
	else
		(( rc = 23)) 
		echo $DIA-$HORA" Error $rc : Fallo al crear el archivo de error $LOGS/$EJECUCION_LOG.log"
		exit $rc
	fi
PASO=2
fi
	
#------------------------------------------------------
# EJECUCION DE CONSULTA EN HIVE (INSERTAR TEMP)
#------------------------------------------------------
echo "==== Inicia ejecucion del proceso refactoring D_OTC_T_360_GENERAL ===="`date '+%Y%m%d%H%M%S'` >> $LOGS/$EJECUCION_LOG.log
#Verificar si hay parámetro de re-ejecución
if [ "$PASO" = "2" ]; then
	echo "HIVE: $rc Ejecucion de la consulta en HIVE"
 # Inicio del marcado de tiempo para la tarea actual
	INICIO=$(date +%s)
	echo "==== Ejecucion D_OTC_T_360_GENERAL_1.sql ====" >> $LOGS/$EJECUCION_LOG.log
	beeline -u $VAL_CADENA_JDBC -n $VAL_USER --hiveconf tez.queue.name=$VAL_COLA_EJECUCION \
	--hiveconf hive.auto.convert.sortmerge.join=true --hiveconf hive.optimize.bucketmapjoin=true --hiveconf hive.optimize.bucketmapjoin.sortedmerge=true \
	--hivevar ESQUEMA_TEMP=${ESQUEMA_TEMP} --hivevar fechamas1=${fechamas1} --hivevar FECHAEJE=${FECHAEJE} --hivevar fechamenos1mes=${fechamenos1mes} --hivevar fechamenos2mes=${fechamenos2mes} \
	--hivevar ESQUEMA_TABLA_1=${ESQUEMA_TABLA_1} --hivevar ESQUEMA_TABLA_3=${ESQUEMA_TABLA_3} --hivevar ESQUEMA_TABLA_2=${ESQUEMA_TABLA_2} \
	-f ${VAL_RUTA}/sql/$VAL_SQL_1 2>> $LOGS/$EJECUCION_LOG.log 
	
   # Verificacion de creacion tabla external
	if [ $? -eq 0 ]; then
		echo "HIVE: $rc Fin de creacion e insert en tabla temporales sin dependencia --- D_OTC_T_360_GENERAL_1.sql" $PASO
		else
		(( rc = 40)) 
		echo "HIVE: $rc Fallo al ejecutar el script D_OTC_T_360_GENERAL_1.sql desde HIVE - Tabla" $PASO
		exit $rc
	fi	
   
	FIN=$(date +%s)
	DIF=$(echo "$FIN - $INICIO" | bc)
	TOTAL=$(printf '%d:%d:%d\n' $(($DIF/3600)) $(($DIF%3600/60)) $(($DIF%60)))
	PASO=3
fi	

#------------------------------------------------------
# EJECUCION DE CONSULTA EN HIVE (INSERTAR QUERY)
#------------------------------------------------------
#Verificar si hay parámetro de re-ejecución
if [ "$PASO" = "3" ]; then
	INICIO=$(date +%s)
	echo "HIVE: $rc INICIO EJECUCION del INSERT en HIVE" $PASO
	echo "==== Ejecucion D_OTC_T_360_GENERAL_2.sql ====" >> $LOGS/$EJECUCION_LOG.log
	beeline -u $VAL_CADENA_JDBC -n $VAL_USER --hiveconf tez.queue.name=$VAL_COLA_EJECUCION \
	--hiveconf hive.auto.convert.sortmerge.join=true --hiveconf hive.optimize.bucketmapjoin=true --hiveconf hive.optimize.bucketmapjoin.sortedmerge=true \
	--hivevar ESQUEMA_TEMP=${ESQUEMA_TEMP} --hivevar fechamas1=${fechamas1} --hivevar FECHAEJE=${FECHAEJE} --hivevar fechamenos1mes=${fechamenos1mes} --hivevar fechamenos2mes=${fechamenos2mes} \
	--hivevar ESQUEMA_TABLA_1=${ESQUEMA_TABLA_1} --hivevar ESQUEMA_TABLA_3=${ESQUEMA_TABLA_3} --hivevar ESQUEMA_TABLA_2=${ESQUEMA_TABLA_2} \
	--hivevar fecha_mes_desp=${fecha_mes_desp} --hivevar fecha_port_ini=${fecha_port_ini} \
	--hivevar fecha_eje1=${fecha_eje1} --hivevar ESQUEMA_CS_ALTAS=${ESQUEMA_CS_ALTAS} --hivevar ESQUEMA_REPORTES=${ESQUEMA_REPORTES} \
	--hivevar fecha_port_fin=${fecha_port_fin} \
	-f ${VAL_RUTA}/sql/$VAL_SQL_2 2>> $LOGS/$EJECUCION_LOG.log
		# Verificacion de creacion de archivo
		if [ $? -eq 0 ]; then
			echo "HIVE: $rc Finalizacion con EXITO del insert en hive - D_OTC_T_360_GENERAL_2.sql" $PASO
			else
			(( rc = 61)) 
			echo "HIVE: $rc Fallo al ejecutar el insert desde HIVE - D_OTC_T_360_GENERAL_2.sql" $PASO
			exit $rc
		fi
	  FIN=$(date +%s)
	DIF=$(echo "$FIN - $INICIO" | bc)
	TOTAL=$(printf '%d:%d:%d\n' $(($DIF/3600)) $(($DIF%3600/60)) $(($DIF%60)))
PASO=4
fi
	
exit $rc 
