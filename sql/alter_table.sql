----------------------************************************----------------------------------
-------------------------	principales en desarrollo
----------------------************************************----------------------------------
-- 360_general_rf:
SELECT
	*
FROM
	db_desarrollo2021.otc_t_360_general_rf
WHERE
	fecha_proceso = 20220415
LIMIT 600;
--and documento_cliente_ant  is not null 

SELECT
	*
FROM
	db_desarrollo2021.otc_t_360_parque_1_tmp_t_mov
WHERE
	documento_cliente_ant IS NOT NULL
LIMIT 20;

show partitions db_reportes.otc_t_360_general;

DESC db_desarrollo2021.otc_t_360_parque_mop_1_tmp;

DESC db_cs_altas.otc_t_nc_movi_parque_v1;

SELECT
	*
FROM
	db_cs_altas.otc_t_nc_movi_parque_v1
LIMIT 20;

SELECT
	*
FROM
	db_desarrollo2021.otc_t_360_parque_mop_1_tmp
LIMIT 200;

DESC db_temporales.otc_t_360_parque_1_tmp;

DESC db_desarrollo2021.otc_t_alta_baja_hist;
--------- ddl   otc_t_alta_baja_hist
show CREATE TABLE db_desarrollo2021.otc_t_alta_baja_hist;

DROP TABLE IF EXISTS db_desarrollo2021.otc_t_alta_baja_hist;

CREATE TABLE db_desarrollo2021.otc_t_alta_baja_hist(
tipo varchar(30)
, telefono char(9)
, fecha date
, canal string
, sub_canal varchar(50)
, nuevo_sub_canal varchar(50)
, portabilidad varchar(10)
, operadora_origen varchar(20)
, operadora_destino varchar(20)
, motivo varchar(50)
, distribuidor string
, oficina varchar(50)
, sub_movimiento varchar(50)
, imei char(14)
, equipo string
, icc char(19)
, ciudad varchar(110)
, provincia varchar(60)
, cod_categoria varchar(20)
, domain_login_ow varchar(110)
, nombre_usuario_ow varchar(110)
, domain_login_sub varchar(110)
, nombre_usuario_sub varchar(110)
, forma_pago varchar(50)
, cod_da string
, nom_usuario string
, campania string
, codigo_distribuidor varchar(60)
, codigo_plaza varchar(60)
, nom_plaza varchar(110)
, region string
, provincia_ivr string
, ejecutivo_asignado_ptr string
, area_ptr string
, codigo_vendedor_da_ptr string
, jefatura_ptr string
, provincia_ms string
, codigo_usuario char(9)
, calf_riesgo char(1)
, cap_endeu char(1)
, valor_cred int
, vol_invol string
, account_num bigint
, distribuidor_crm varchar(110)
, canal_transacc varchar(110)
--, resegmentar string 
--, alta_mes string
--, provincia_altamira string
--, ciudad_altamira string
--, ciudad_zoni string
--, fuente string
--, tipo_cliente string
)
clustered BY (telefono)
INTO
	4 buckets
tblproperties ('transactional' = 'true');
--------- ddl   otc_t_transfer_hist
show CREATE TABLE db_desarrollo2021.otc_t_transfer_hist;

DROP TABLE IF EXISTS db_desarrollo2021.otc_t_transfer_hist;

CREATE TABLE db_desarrollo2021.otc_t_transfer_hist(
tipo varchar(30)
, telefono char(9)
, fecha date
, canal varchar(50)
, sub_canal varchar(50)
, nuevo_sub_canal varchar(50)
, distribuidor varchar(60)
, oficina varchar(50)
, sub_movimiento varchar(50)
, imei char(14)
, equipo string
, icc char(19)
, domain_login_ow varchar(110)
, nombre_usuario_ow varchar(110)
, domain_login_sub varchar(110)
, nombre_usuario_sub varchar(110)
, forma_pago varchar(50)
, canal_transacc varchar(50)
, campania string
, codigo_distribuidor varchar(60)
, codigo_plaza varchar(60)
, nom_plaza varchar(110)
, region string
, ruc_distribuidor string
, ejecutivo_asignado_ptr string
, area_ptr string
, codigo_vendedor_da_ptr string
, jefatura_ptr string
, codigo_usuario char(9)
, calf_riesgo char(1)
, cap_endeu char(1)
, valor_cred int
, account_num_anterior varchar(30)
, ciudad_usuario varchar(40)
, provincia_usuario varchar(40)
, ciudad varchar(100)
, COD_PLAN_ANTERIOR varchar(20)
, DES_PLAN_ANTERIOR varchar(50)
, distribuidor_crm varchar(128)
--, mismo_cliente char(2)
--, dias_en_parque_prepago int
--, tipo_cliente varchar(50)
)
clustered BY (telefono)
INTO
	4 buckets
tblproperties ('transactional' = 'true');

SELECT
	*
FROM
	db_desarrollo2021.otc_t_transfer_hist
WHERE
	tipo = 'pos_pre'
LIMIT 200;
---------
--------- ddl   otc_t_cambio_plan_hist

DESC db_cs_altas.otc_t_cambio_plan_bi;

show CREATE TABLE db_reportes.OTC_T_CAMBIO_PLAN_HIST;

show CREATE TABLE db_desarrollo2021.OTC_T_CAMBIO_PLAN_HIST;

DROP TABLE IF EXISTS db_desarrollo2021.otc_t_cambio_plan_hist;

CREATE TABLE db_desarrollo2021.otc_t_cambio_plan_hist(
tipo varchar(20)
, telefono varchar(9)
, fecha date
, canal varchar(50)
, sub_canal varchar(50)
, nuevo_sub_canal varchar(50)
, distribuidor varchar(50)
, oficina varchar(50)
, cod_plan_anterior varchar(10)
, des_plan_anterior varchar(50)
, tb_descuento double
, tb_override double
, delta double
, sub_movimiento varchar(50)
, domain_login_ow varchar(100)
, nombre_usuario_ow varchar(100)
, domain_login_sub varchar(100)
, nombre_usuario_sub varchar(100)
, forma_pago varchar(100)
, campania varchar(80)
, codigo_distribuidor varchar(60)
, codigo_plaza varchar(60)
, nom_plaza varchar(110)
, region string
, ruc_distribuidor string
, tarifa_basica_anterior double
, fecha_inicio_plan_anterior date
, tarifa_final_plan_act double
, tarifa_final_plan_ant double
, provincia varchar(50)
)
clustered BY (telefono)
INTO
	4 buckets
	stored AS orc
tblproperties ('transactional' = 'true');
-----************RF
-------- fin rf

select 
--count(1)
* 
from db_desarrollo2021.otc_t_cambio_plan_hist;

SELECT
	--count(1)
tipo
, telefono 
,fecha 
,canal 
, SUB_CANAL
, NUEVO_SUB_CANAL
, DISTRIBUIDOR
,oficina 
,COD_PLAN_ANTERIOR
,DES_PLAN_ANTERIOR
, TB_DESCUENTO
, TB_OVERRIDE
, DELTA
, SUB_MOVIMIENTO
, DOMAIN_LOGIN_OW
, NOMBRE_USUARIO_OW
, DOMAIN_LOGIN_SUB
, NOMBRE_USUARIO_SUB
, FORMA_PAGO
, CAMPANIA
, CODIGO_DISTRIBUIDOR
, CODIGO_PLAZA
, NOM_PLAZA
,REGION 
, RUC_DISTRIBUIDOR
, TARIFA_FINAL_PLAN_ANT
, PROVINCIA
--, TARIFA_FINAL_PLAN_ACT
FROM
	db_desarrollo2021.otc_t_cambio_plan_hist
	--where delta is not null
LIMIT 200;
delete from db_desarrollo2021.otc_t_cambio_plan_hist;

---------
------  --- ddl otc_t_no_reciclable_hist
DESC db_cs_altas.no_reciclable;
--esta tabla no es particionada

DESC db_desarrollo2021.otc_t_no_reciclable_hist;

DROP TABLE db_desarrollo2021.otc_t_no_reciclable_hist ;

CREATE TABLE db_desarrollo2021.otc_t_no_reciclable_hist(
tipo varchar(30)
, sub_movimiento varchar(50)
, telefono char(9)
, fecha date
, documento_cliente_act char(13)
, fecha_proceso date
, canal_comercial string
, campania string
, codigo_distribuidor string
, nom_distribuidor varchar(110)
, codigo_plaza varchar(60)
, nom_plaza varchar(110)
, region string
, ruc_distribuidor string
, linea_negocio_baja string
, documento_cliente_ant char(13)
, dias int
, fecha_baja date
)
clustered BY (telefono)
INTO
	4 buckets
stored AS orc
tblproperties ('transactional' = 'true');


DROP TABLE if exists db_desarrollo2021.otc_t_no_reciclable_hist PURGE;
CREATE TABLE db_desarrollo2021.otc_t_no_reciclable_hist(
tipo varchar(30)
, sub_movimiento varchar(50)
, telefono char(9)
)
clustered BY (telefono)
INTO
	4 buckets
stored AS orc
tblproperties ('transactional' = 'true');

SELECT
	*
--count(1)
FROM
	db_desarrollo2021.otc_t_no_reciclable_hist
LIMIT 200;
delete from db_desarrollo2021.otc_t_no_reciclable_hist  ;
where telefono ='NO_RECICLABLE';

truncate table db_desarrollo2021.otc_t_no_reciclable_hist;
MSCK REPAIR TABLE db_desarrollo2021.otc_t_no_reciclable_hist;
ALTER TABLE db_desarrollo2021.otc_t_no_reciclable_hist RENAME TO db_desarrollo2021.otc_t_no_reciclable_hist_777;

-------- --- ddl otc_t_alta_baja_reproceso_hist
DESC db_desarrollo2021.otc_t_alta_baja_reproceso_hist;

DROP TABLE IF EXISTS db_desarrollo2021.otc_t_alta_baja_reproceso_hist;

CREATE TABLE db_desarrollo2021.otc_t_alta_baja_reproceso_hist(
tipo varchar(30)
, sub_movimiento varchar(50)
, telefono char(9)
, fecha date
, domain_login_ow varchar(110)
, icc char(19)
, distribuidor varchar(110)
, portabilidad string
, cod_da string
, canal_comercial string
, campania string
, codigo_distribuidor varchar(60)
, nom_distribuidor string
, codigo_plaza varchar(60)
, nom_plaza varchar(110)
, region string
, ruc_distribuidor string
, fecha_baja date
)
clustered BY (telefono)
INTO
	4 buckets
tblproperties ('transactional' = 'true');
-----

select * from db_desarrollo2021.otc_t_alta_baja_reproceso_hist;


--- DDL
DROP TABLE IF EXISTS db_desarrollo2021.otc_t_cambio_plan_hist;

CREATE TABLE db_desarrollo2021.otc_t_cambio_plan_hist(
  tipo varchar(30)
, telefono varchar(9)
, fecha date
, canal varchar(50)
, sub_canal varchar(50)
, nuevo_sub_canal varchar(50)
, distribuidor varchar(50)
, oficina varchar(50)
, cod_plan_anterior varchar(10)
, des_plan_anterior varchar(50)
, tb_descuento double
, tb_override double
, delta double
, domain_login_ow varchar(110)
, nombre_usuario_ow varchar(110)
, domain_login_sub varchar(110)
, nombre_usuario_sub varchar(110)
, campania string
, codigo_distribuidor varchar(60)
, codigo_plaza varchar(60)
, nom_plaza varchar(110)
, region string
, nombre_plan_anterior varchar(50)
, tarifa_basica_anterior float
, fecha_inicio_plan_anterior date
, tarifa_final_plan_act float
, tarifa_final_plan_ant float)
clustered BY (telefono) 
INTO
	4 buckets;
--- ddl db_desarrollo2021.otc_t_360_general_rf
show CREATE TABLE db_reportes.otc_t_360_general;
show CREATE TABLE db_desarrollo2021.otc_t_360_general_rf;
DROP TABLE IF EXISTS db_desarrollo2021.d_otc_t_360_general;

CREATE TABLE db_desarrollo2021.d_otc_t_360_general(
  num_telefonico string
, codigo_plan string
, usa_app string
, usuario_app string
, usa_movistar_play string
, usuario_movistar_play string
, fecha_alta timestamp
, nse double
, sexo string
, edad double
, mes string
, anio string
, segmento string
, linea_negocio string
, linea_negocio_homologado string
, forma_pago_factura varchar(40)
, forma_pago_alta string
, estado_abonado string
, sub_segmento string
, numero_abonado string
, account_num string
, identificacion_cliente string
, customer_ref string
, tac string
, tiene_bono string
, valor_bono double
, codigo_bono string
, probabilidad_churn double
, counted_days int
, categoria_plan string
, tarifa double
, nombre_plan string
, marca string
, grupo_prepago string
, fidelizacion_megas string
, fidelizacion_dumy string
, bancarizado string
, bono_combero string
, ticket_recarga string
, tiene_score_tiaxa string
, score_1_tiaxa double
, score_2_tiaxa double
, tipo_doc_cliente string
, nombre_cliente string
, ciclo_facturacion string
, email string
, telefono_contacto string
, fecha_ultima_renovacion date
, address_2 string
, address_3 string
, address_4 string
, fecha_fin_contrato_definitivo date
, vigencia_contrato int
, version_plan int
, fecha_ultima_renovacion_jn date
, fecha_ultimo_cambio_plan date
, tipo_movimiento_mes string
, fecha_movimiento_mes date
, es_parque string
, banco string
, parque_recargador string
, segmento_parque string
, susp_cobranza string
, susp_911 string
, susp_cobranza_puntual string
, susp_fraude string
, susp_robo string
, susp_voluntaria string
, vencimiento_cartera string
, saldo_cartera float
, fecha_alta_historia date
, canal_alta varchar(50)
, sub_canal_alta varchar(50)
, distribuidor_alta varchar(50)
, oficina_alta varchar(50)
, portabilidad varchar(10)
, operadora_origen varchar(20)
, operadora_destino varchar(20)
, motivo varchar(50)
, fecha_pre_pos date
, canal_pre_pos varchar(50)
, sub_canal_pre_pos varchar(50)
, distribuidor_pre_pos varchar(50)
, oficina_pre_pos varchar(50)
, fecha_pos_pre date
, canal_pos_pre varchar(50)
, sub_canal_pos_pre varchar(50)
, distribuidor_pos_pre varchar(50)
, oficina_pos_pre varchar(50)
, fecha_cambio_plan date
, canal_cambio_plan varchar(50)
, sub_canal_cambio_plan varchar(50)
, distribuidor_cambio_plan varchar(50)
, oficina_cambio_plan varchar(50)
, cod_plan_anterior varchar(10)
, des_plan_anterior varchar(50)
, tb_descuento double
, tb_override double
, delta double
, canal_movimiento_mes varchar(50)
, sub_canal_movimiento_mes varchar(50)
, distribuidor_movimiento_mes varchar(50)
, oficina_movimiento_mes varchar(50)
, portabilidad_movimiento_mes string
, operadora_origen_movimiento_mes string
, operadora_destino_movimiento_mes string
, motivo_movimiento_mes string
, cod_plan_anterior_movimiento_mes string
, des_plan_anterior_movimiento_mes string
, tb_descuento_movimiento_mes double
, tb_override_movimiento_mes double
, delta_movimiento_mes double
, fecha_alta_cuenta date
, fecha_inicio_pago_actual date
, fecha_fin_pago_actual date
, fecha_inicio_pago_anterior date
, fecha_fin_pago_anterior date
, forma_pago_anterior varchar(40)
, origen_alta_segmento varchar(20)
, fecha_alta_segmento date
, dias_voz int
, dias_datos int
, dias_sms int
, dias_contenido int
, dias_total int
, limite_credito double
, adendum double
, fecha_registro_app date
, perfil string
, usuario_web string
, fecha_registro_web timestamp
, fecha_nacimiento date comment 'fecha de nacimiento del cliente en formato yyyy-mm-dd'
------INSERTADO EN REFACTORING
, id_tipo_movimiento int
, tipo_movimiento varchar(50)
, id_subcanal int
, id_producto string
, sub_movimiento varchar(50)
, tecnologia char(2)
, dias_transcurridos_baja int
, dias_en_parque int
, dias_en_parque_prepago int
, tipo_descuento_conadis varchar(110)
, tipo_descuento varchar(110)
, ciudad varchar(50)
, provincia_activacion varchar(50)
, cod_categoria varchar(50)
, cod_da varchar(50)
, nom_usuario varchar(50)
, provincia_ivr varchar(50)
, provincia_ms varchar(50)
, fecha_movimiento_baja date
, vol_invol varchar(50)
, account_num_anterior varchar(50)
, imei char(14)
, equipo varchar(110)
, icc char(19)
, domain_login_ow varchar(110)
, nombre_usuario_ow varchar(110)
, domain_login_sub varchar(110)
, nombre_usuario_sub varchar(110)
, forma_pago varchar(110)
, id_canal int
, canal_comercial varchar(110)
, campania varchar(110)
, codigo_distribuidor varchar(110)
, nom_distribuidor varchar(110)
, codigo_plaza varchar(110)
, nom_plaza varchar(110)
, region varchar(110)
, ruc_distribuidor varchar(110)
, ejecutivo_asignado_ptr varchar(110)
, area_ptr varchar(110)
, codigo_vendedor_da_ptr varchar(110)
, jefatura_ptr varchar(110)
, codigo_usuario char(9)
, descripcion_desp varchar(50)
, calf_riesgo char(1)
, cap_endeu char(1)
, valor_cred int
, ciudad_usuario varchar(50)
, provincia_usuario varchar(50)
, linea_de_negocio_anterior varchar(50)
, cliente_anterior char(15)
, dias_reciclaje int
, fecha_baja_reciclada date
, tarifa_basica_anterior float
, fecha_inicio_plan_anterior date
, tarifa_final_plan_act float
, tarifa_final_plan_ant float
, mismo_cliente char(2)
, delta_tarifa_final float
, tipo_de_cuenta_en_operador_donante varchar(50)
, fecha_alta_prepago date
, TARIFA_BASICA_BAJA float
, canal_transacc varchar(110)
, distribuidor_crm varchar(110)
--FINALIZA REFACTORING
)
partitioned BY (fecha_proceso bigint)
stored AS orc
tblproperties ('orc.compress' = 'SNAPPY');

--------------AUXILIAR

----------FIN AUXILIAR





DROP TABLE db_desarrollo2021.solicitudes_de_portabilidad_in;

CREATE TABLE db_desarrollo2021.solicitudes_de_portabilidad_in(
 customeraccountnumber bigint
, doc_number bigint
, doc_type string
, customercategory string
, portincommonorderid string
, requeststatus string
, estado string
, fvc string
, operadora string
, donor_account_type1 string
, ln_origen string
, assignedcsr string
, created_when timestamp
, salesorderid string
, salesorderstatus string
, salesorderprocesseddate timestamp
, telefono bigint
--ssociatedsimiccid string 
, plandestino string
, motivo_rechazo string 
 )
 stored AS orc 
 tblproperties ('orc.compress' = 'snappy');

show CREATE TABLE db_desarrollo2021.solicitudes_de_portabilidad_in;

--- DDL otc_t_catalogo_consolidado_id
CREATE TABLE db_desarrollo2021.otc_t_catalogo_consolidado_id(
tipo_movimiento string
, id_tipo_movimiento string 
, nombre_id string 
, extractor string 
, crite string 
)
stored AS parquet;

show create table db_desarrollo2021.OTC_T_ALTA_BAJA_REPROCESO_HIST;
drop table db_desarrollo2021.OTC_T_ALTA_BAJA_REPROCESO_HIST;
drop table db_desarrollo2021.abr_altas_bajas_reproceso;
--create table db_desarrollo2021.abr_altas_bajas_reproceso(
create table db_desarrollo2021.OTC_T_ALTA_BAJA_REPROCESO_HIST(
telefono varchar (30)
,cliente	varchar(210)
,fecha_alta	date
,fecha_baja	date
,portabilidad	string
,linea_negocio varchar(60)
,tipo varchar(30)
,sub_movimiento varchar(50)
,account_num	bigint
,documento_cliente	varchar(60)
,nombre_plan	varchar(40)
,icc	varchar(60)
,domain_login_ow	varchar(110)
,nombre_usuario_ow	varchar(110)
,domain_login_sub	varchar(110)
,nombre_usuario_sub	varchar(110)
,canal_transacc	varchar(110)
,distribuidor_crm	varchar(110)
,oficina	varchar(110)
,forma_pago	varchar(50)
,cod_da	string
,nom_usuario	string
,canal	string
,campania	string
,codigo_distribuidor	varchar(60)
,distribuidor	string
,codigo_plaza	varchar(60)
,nom_plaza	varchar(110)
,region	string
,sub_canal	string
,operadora_origen string
, imei varchar(60)
, equipo string
, ejecutivo_asignado_ptr	string
, area_ptr	string
, codigo_vendedor_da_ptr	string
, jefatura_ptr	string
, codigo_usuario	string
, calf_riesgo	string
, cap_endeu	string
, valor_cred	int
)
clustered BY (telefono) 
INTO 4 buckets
TBLPROPERTIES ('transactional'='true');
	show create table db_desarrollo2021.abr_altas_bajas_reproceso;