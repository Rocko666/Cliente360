#!/bin/bash
##########################################################################
#   Script de carga de Generica para entidades de URM con reejecucion    #
# Creado 13-Jun-2018 (LC) Version 1.0                                    #
# Las tildes han sido omitidas intencionalmente en el script             #
#------------------------------------------------------------------------#
#------------------------------------------------------------------------#
# REFACTORING: CRISTIAN ORTIZ											 #
# MODIFICADO : 26/JUN/2022												 #
# COMENTARIO: Se hizo un REFACTORING de este proceso para el BIGD-677    #
# "EXTRACTOR DE MOVIMIENTOS" 											 #
##########################################################################
#------------------------------------------------------
# VARIABLES CONFIGURABLES POR PROCESO (MODIFICAR)
#------------------------------------------------------
ENTIDAD=D_OTC_T_360_MOVIMIENTOS_PARQUE
# AMBIENTE (1=produccion, 0=desarrollo)
((AMBIENTE=0))
FECHAEJE=$1 # yyyyMMdd
# Variable de control de que paso ejecutar
PASO=$2
## Se comenta la linea de abajo para leerla por parametro desde params_des en MySQL
#TABLA_PIVOTANTE=otc_t_360_parque_1_tmp; TABLA RESULTANTE DE PIVOT_PARQUE
		
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
	
#Verificar que la configuracion de la entidad exista
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

##se ha cambiado el valor de la linea inferior como precaucion en etapas de REFACTORING, cambiar a "$AMBIENTE" = "1" cuando se realize despliegue a produccion.
if [ "$AMBIENTE" = "1" ]; then
	# Cargar Datos desde la base
	VAL_RUTA=`mysql -N  <<<"select valor from params where entidad = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') AND parametro = 'VAL_RUTA';"` 
	NAME_SHELL=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'SHELL');"`
	ESQUEMA_TEMP=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'ESQUEMA_TEMP' );"`
	ESQUEMA_TABLA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'ESQUEMA_TABLA' );"`
	TABLA_PIVOTANTE=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') AND parametro = 'TABLA_PIVOTANTE';"`
	ESQUEMA_CS_ALTAS=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') AND parametro = 'ESQUEMA_CS_ALTAS';"`
else 
	# Cargar Datos desde la base
	VAL_RUTA=`mysql -N  <<<"select valor from params_des where entidad = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') AND parametro = 'VAL_RUTA';"` 
	#Limpiar (1=si, 0=no)
	NAME_SHELL=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'SHELL');"`
	ESQUEMA_TEMP=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'ESQUEMA_TEMP' );"`
	ESQUEMA_TABLA=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'ESQUEMA_TABLA' );"`
	TABLA_PIVOTANTE=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') AND parametro = 'TABLA_PIVOTANTE';"`
	ESQUEMA_CS_ALTAS=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') AND parametro = 'ESQUEMA_CS_ALTAS';"`
fi	

#Verificar si tuvo datos de la base
TIME=`date +%a" "%d"/"%m"/"%Y" "%X`
if [ -z "$VAL_RUTA" ]||[ -z "$NAME_SHELL" ]||[ -z "$ESQUEMA_TEMP" ]||[ -z "$ESQUEMA_TABLA" ]||[ -z "$TABLA_PIVOTANTE" ]; then
((rc=3))
echo " $TIME [ERROR] $rc No se han obtenido los parametros necesarios desde la base de datos"
exit $rc
fi

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
LOGS=$VAL_RUTA/log
	
#PARAMETROS GENERICOS BEELINE
VAL_CADENA_JDBC=`mysql -N  <<<"select valor from params_des where ENTIDAD = 'D_PARAM_BEELINE' AND parametro = 'VAL_CADENA_JDBC';"`
VAL_COLA_EJECUCION=`mysql -N  <<<"select valor from params_des where ENTIDAD = 'D_PARAM_BEELINE' AND parametro = 'VAL_COLA_EJECUCION';"`
VAL_USER=`mysql -N  <<<"select valor from params_des where ENTIDAD = 'D_OTC_T_360_MOVIMIENTOS_PARQUE' AND parametro = 'VAL_USER';"`

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
		mkdir -p $VAL_RUTA/$ENTIDAD/log
			if ! [ $? -eq 0 ]; then
				((rc=11))
				echo " $TIME [ERROR] $rc no se pudo crear la ruta de logs"
				exit $rc
			fi
	fi
fi
#------------------------------------------------------
# DEFINICION DE FECHAS
#------------------------------------------------------
fecha_proceso=`date -d "$FECHAEJE" "+%Y-%m-%d"`
f_check=`date -d "$FECHAEJE" "+%d"`
 # para tipo de dato DATE
fecha_movimientos=`date '+%Y-%m-%d' -d "$fecha_proceso+1 day"`
 # para las tablas particionadas
fecha_movimientos_cp=`date '+%Y%m%d' -d "$fecha_proceso+1 day"`
fecha_mes_ant_cp=`date -d "$FECHAEJE" "+%Y%m01"`
fecha_mes_desp=`date -d "$FECHAEJE-1 month" "+%Y%m"`
fecha_mes_ant=`date -d "$FECHAEJE" "+%Y-%m-01"`

if [ $f_check == "01" ]; then
	f_inicio=`date -d "$FECHAEJE -1 days" "+%Y-%m-01"`
	f_inicio_abr=`date -d "$FECHAEJE -1 days" "+%Y%m02"`
	f_fin_abr=`date -d "$FECHAEJE -1 days" "+%Y%m%d"`
	f_efectiva=`date -d "$FECHAEJE" "+%Y%m%d"`
else
	f_inicio=`date -d "$FECHAEJE" "+%Y-%m-01"`
	f_inicio_abr=`date -d "$FECHAEJE" "+%Y%m02"`
	f_fin_abr=`date -d "$FECHAEJE" "+%Y%m%d"`
	f_efectiva=`date -d "$FECHAEJE+1 day" "+%Y%m%d"`
echo $f_inicio
fi

echo $f_inicio" Fecha Inicio" >> $LOGS/$EJECUCION_LOG.log
echo $fecha_proceso" Fecha Ejecucion" >> $LOGS/$EJECUCION_LOG.log
#------------------------------------------------------
# CREACION DE LOGS 
#------------------------------------------------------
#Verificar si hay parametro de re-ejecucion
if [ "$PASO" = "0" ]; then

	echo $DIA-$HORA" Creacion de directorio para almacenamiento de logs" 
	
	#Comprobacion de la ruta de trabajo
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
	echo "# Entidad: "$ENTIDAD" Fecha: "$FECHAEJE $DIA"-"$HORA >> $LOGS/$EJECUCION_LOG.log
	if [ $? -eq 0 ];	then
		echo "# Fecha de inicio: "$DIA" "$HORA >> $LOGS/$EJECUCION_LOG.log
		echo "---------------------------------------------------------------------" >> $LOGS/$EJECUCION_LOG.log
	else
		(( rc = 22))
		echo $DIA-$HORA" Error $rc : Fallo al crear el archivo de log $LOGS/$EJECUCION_LOG.log"
		exit $rc
	fi
	
	# CREACION DE ARCHIVO DE ERROR 
	
	echo "# Entidad: "$ENTIDAD" Fecha: "$FECHAEJE $DIA"-"$HORA >> $LOGS/$EJECUCION_LOG.log
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
#Verificar si hay parametro de re-ejecucion
if [ "$PASO" = "2" ]; then
	# Inicio del marcado de tiempo para la tarea actual
	INICIO=$(date +%s)
	
	#Consulta a ejecutar
	echo "==== Empieza la ejecucion D_OTC_T_360_MOVIMIENTOS_PARQUE.sql ====" >> $LOGS/$EJECUCION_LOG.log
	beeline -u $VAL_CADENA_JDBC -n $VAL_USER --hiveconf tez.queue.name=$VAL_COLA_EJECUCION \
	--hiveconf hive.auto.convert.sortmerge.join=true --hiveconf hive.optimize.bucketmapjoin=true --hiveconf hive.optimize.bucketmapjoin.sortedmerge=true \
	--hivevar ESQUEMA_TABLA=${ESQUEMA_TABLA} --hivevar f_inicio=${f_inicio} --hivevar fecha_proceso=${fecha_proceso} \
	--hivevar fecha_movimientos_cp=${fecha_movimientos_cp} --hivevar ESQUEMA_TEMP=${ESQUEMA_TEMP} --hivevar fecha_movimientos=${fecha_movimientos} \
	--hivevar TABLA_PIVOTANTE=${TABLA_PIVOTANTE} --hivevar VAL_COLA_EJECUCION=${VAL_COLA_EJECUCION} --hivevar ESQUEMA_TEMP=${ESQUEMA_TEMP} \
	--hivevar fecha_mes_ant=${fecha_mes_ant} --hivevar fecha_mes_desp=${fecha_mes_desp} \
	--hivevar fecha_mes_ant_cp=${fecha_mes_ant_cp} --hivevar ESQUEMA_CS_ALTAS=${ESQUEMA_CS_ALTAS} \
	-f ${VAL_RUTA}/sql/D_OTC_T_360_MOVIMIENTOS_PARQUE.sql &>> $LOGS/$EJECUCION_LOG.log

				# Verificacion de creacion tabla external
	if [ $? -eq 0 ]; then
		echo "HIVE: $rc  Fin de creacion e insert en tabla temporales sin dependencia " >> $LOGS/$EJECUCION_LOG.log
		else
		(( rc = 40)) 
		echo "HIVE: $rc  Fallo al ejecutar script desde HIVE - Tabla" >> $LOGS/$EJECUCION_LOG.lo
		exit $rc
	fi	
	FIN=$(date +%s)
	DIF=$(echo "$FIN - $INICIO" | bc)
	TOTAL=$(printf '%d:%d:%d\n' $(($DIF/3600)) $(($DIF%3600/60)) $(($DIF%60)))
	echo "HIVE tablas temporales temp" $TOTAL "0" "0"		
 PASO=3
fi
echo "==== TERMINA el proceso refactoring D_OTC_T_360_MOVIMIENTOS_PARQUE de forma EXITOSA====" >> $LOGS/$EJECUCION_LOG.log	
exit $rc