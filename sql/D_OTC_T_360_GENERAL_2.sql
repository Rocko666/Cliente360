------------------------------------------------------
--EJECUCION DE CONSULTA EN HIVE (INSERTAR QUERY)
------------------------------------------------------
SET hive.cli.print.header = FALSE;

SET hive.vectorized.execution.enabled = FALSE;

SET hive.vectorized.execution.reduce.enabled = FALSE;


---N24
--tabla temporal para obtener el ultimo descuento por min
DROP TABLE IF EXISTS ${ESQUEMA_TEMP}.tmp_otc_t_desc_planes;

CREATE TABLE ${ESQUEMA_TEMP}.tmp_otc_t_desc_planes AS 
SELECT
	phone_number
	, tariff_plan_id
	, created_when_orden
	, descripcion_descuento
	, tipo_proceso_esp
	, p_fecha_proceso
	, discount_value
	, (CASE WHEN upper(descripcion_descuento) LIKE '%CONADIS%' THEN 'CONADIS'
			WHEN upper(descripcion_descuento) LIKE '%ADULTO%MAYOR%' THEN 'CONADIS' end ) as desc_conadis
FROM
	(
	SELECT
		phone_number
		, tariff_plan_id
		, created_when_orden
		, descripcion_descuento
		, tipo_proceso_esp
		, p_fecha_proceso
		, discount_value
		, ROW_NUMBER() OVER (PARTITION BY phone_number
	ORDER BY
		to_date(created_when_orden) DESC) rn
	FROM
		db_cs_altas.otc_t_descuentos_planes
	WHERE
		p_fecha_proceso = ${fechamas1}
	AND to_date(created_when_orden) between '{fecha_inico_mes_1_1}' and '{fecha_eje1}'
	and (tipo_proceso_esp  <>  'Suspender' or  tipo_proceso_esp is null)
	) t1
WHERE
	rn = 1;


--N25
--tabla temporal para obtener el ultimo overwrite por min
DROP TABLE IF EXISTS ${ESQUEMA_TEMP}.tmp_otc_t_ov_planes;

CREATE TABLE ${ESQUEMA_TEMP}.TMP_OTC_T_ov_PLANES AS 
SELECT
	created_when_orden
	, phone_number
	, p_fecha_proceso
	, tariff_plan_id
	, mrc_base_price
	, mrc_ov_price
FROM
	(
	SELECT
		created_when_orden
		, phone_number
		, p_fecha_proceso
		, tariff_plan_id
		, mrc_base_price
		, mrc_ov_price
		, ROW_NUMBER() OVER (PARTITION BY phone_number
	ORDER BY
		to_date(created_when_orden) DESC) rn
	FROM
		db_cs_altas.otc_t_overwrite_planes
	WHERE
		p_FECHA_PROCESO = ${fechamas1}
	AND to_date(created_when_orden) between '{fecha_inico_mes_1_1}' and '{fecha_eje1}'
	) t1
WHERE
	rn = 1;


--N26
-- TABLA TEMPORAL perimetros altas y trasnfer in
DROP TABLE ${ESQUEMA_TEMP}.tmp_PRMT_ALTA_TI;
CREATE TABLE ${ESQUEMA_TEMP}.tmp_prmt_alta_ti AS
SELECT 
identificador
,razon_social
,fecha_ingreso
,ejecutivo_asignado
,correo_ejecutivo_asignado
,area
,codigo_vendedor_da
,jefatura
,red_jefe
,tipopersona
,tiposegmentacion
,tiporuc
,region
,fecha_carga
,pt_fecha
FROM db_desarrollo2021.OTC_T_PRMTR_ALTAS_TI 
WHERE pt_fecha={vIFechaEje};


--N27
--TABLA TEMPORAL perimetros bajas y transfer out 
DROP TABLE ${ESQUEMA_TEMP}.tmp_PRMT_BAJA_TO;
CREATE TABLE ${ESQUEMA_TEMP}.tmp_prmt_baja_to AS
SELECT 
identificador
,razon_social
,fecha_ingreso
,ejecutivo_asignado
,correo_ejecutivo_asignado
,area
,codigo_vendedor_da
,jefatura
,region
,fecha_carga
,pt_fecha
FROM db_desarrollo2021.OTC_T_PRMTR_BAJAS_TO
WHERE pt_fecha={vIFechaEje};


--N028
-- creacion de tabla temporal de descuentos no pymes
DROP TABLE ${ESQUEMA_TEMP}.tmp_desc_no_pymes;
CREATE TABLE ${ESQUEMA_TEMP}.tmp_desc_no_pymes AS
SELECT telefono
	,plan_codigo
	,descuento
	,fecha_proceso
	,fecha_carga
	, detalle
from db_desarrollo2021.OTC_T_DESC_NO_PYMES
WHERE fecha_proceso = '{fecha_eje1}';


--N29
-- CREACION DE TABLA TEMPORAL PARA DESPACHOS
DROP TABLE ${ESQUEMA_TEMP}.tmp_desp_nc_final;
CREATE TABLE ${ESQUEMA_TEMP}.tmp_desp_nc_final AS
SELECT
	icc
	, no_min
	, (case when descripcion like'%HALFSIM CHIP PREPAGO $5 AUTOSERVICIO NUMERADO 15D%' 
			then 'HALFSIM CHIP PREPAGO $5 AUTOSERVICIO NUMERADO 15DIAS'
			when descripcion like'%HALFSIM CHIP PREPAGO $5 NUMERADO 15 D%' 
			then 'HALFSIM CHIP PREPAGO $5 NUMERADO 15 DIAS'
			when descripcion like'%HALFSIM TUENTI PREPAGO CELOF%N NUMERADA%' 
			then 'HALFSIM TUENTI PREPAGO CELOFAN NUMERADA'
			when descripcion like'%USIM TUENTI PREPAGO BL%STER NUMERADA $20.00%' 
			then 'USIM TUENTI PREPAGO BLISTER NUMERADA $20.00'
			when descripcion like'%USIM TUENTI PREPAGO BL%STER NUMERADA $10.00%' 
			then 'USIM TUENTI PREPAGO BLISTER NUMERADA $10.00'
			ELSE descripcion END) as descripcion
	, codigo_despacho
	, cliente
	, operadora
FROM
		db_cs_altas.despachos_nc_final
WHERE
        upper(operadora) = 'MOVISTAR'
;

--N30
--TABLA TEMPORAL PARA id_canal DESDE otc_t_catalogo_consolidado_id
DROP TABLE ${ESQUEMA_TEMP}.tmp_otc_t_cat_id_canal;
CREATE TABLE ${ESQUEMA_TEMP}.tmp_otc_t_cat_id_canal AS 
SELECT
	tipo_movimiento
	,id_tipo_movimiento
	,nombre_id
	,extractor
	,crite
	,fechacarga
FROM
	db_reportes.otc_t_catalogo_consolidado_id
WHERE
	upper(extractor) IN ('MOVIMIENTOS', 'TODOS')
	AND upper(nombre_id)= UPPER('ID_CANAL');


--N31
--TABLA TEMPORAL PARA id_sub_canal DESDE otc_t_catalogo_consolidado_id
DROP TABLE ${ESQUEMA_TEMP}.tmp_otc_t_cat_id_sub_canal;
CREATE TABLE ${ESQUEMA_TEMP}.tmp_otc_t_cat_id_sub_canal AS 
SELECT
	tipo_movimiento
	,id_tipo_movimiento
	,nombre_id
	,extractor
	,crite
	,fechacarga
FROM
	db_reportes.otc_t_catalogo_consolidado_id
WHERE
	upper(extractor) IN ('MOVIMIENTOS', 'TODOS')
	AND upper(nombre_id)= UPPER('ID_SUBCANAL');


--N32
--TABLA TEMPORAL PARA id_producto DESDE otc_t_catalogo_consolidado_id
DROP TABLE ${ESQUEMA_TEMP}.tmp_otc_t_cat_id_producto;
CREATE TABLE ${ESQUEMA_TEMP}.tmp_otc_t_cat_id_producto AS 
SELECT
	tipo_movimiento
	,id_tipo_movimiento
	,nombre_id
	,extractor
	,crite
	,fechacarga
FROM
	db_reportes.otc_t_catalogo_consolidado_id
WHERE
	upper(extractor) IN ('MOVIMIENTOS', 'TODOS')
	AND upper(nombre_id)= UPPER('ID_PRODUCTO');


--N33
--TABLA TEMPORAL PARA id_tipo_movimiento DESDE otc_t_catalogo_consolidado_id
drop table ${ESQUEMA_TEMP}.tmp_otc_t_cat_id_tipo_mov;
create table ${ESQUEMA_TEMP}.tmp_otc_t_cat_id_tipo_mov as 
SELECT 
id_tipo_movimiento
, nombre_id
, extractor 
, tipo_movimiento
, case 
	when upper(tipo_movimiento) like '%TRANSFER%IN%' then 'PRE_POS'
	when upper(tipo_movimiento) like '%TRANSFER%OUT%' then 'POS_PRE'
	when upper(tipo_movimiento) like '%CAMBIO%DE%PLAN%' then 'UPSELL'
	when upper(tipo_movimiento) like '%NO%RECICLABLE%' then 'NO_RECICLABLE'
	when upper(tipo_movimiento) like '%ALTAS%BAJAS%REPROCESO%' then 'ALTA_BAJA'
	ELSE tipo_movimiento END AS auxiliar
from db_reportes.otc_t_catalogo_consolidado_id  
where upper(extractor) IN ('MOVIMIENTOS', 'TODOS')
and upper(nombre_id)=UPPER('ID_tipo_MOVIMIENTO')
union ALL SELECT
id_tipo_movimiento
, nombre_id
, extractor 
, tipo_movimiento
, 'DOWNSELL'
FROM db_reportes.otc_t_catalogo_consolidado_id  
where upper(tipo_movimiento) like '%CAMBIO%DE%PLAN%'
and upper(nombre_id)=UPPER('ID_tipo_MOVIMIENTO')
union ALL SELECT
id_tipo_movimiento
, nombre_id
, extractor 
, tipo_movimiento
, 'MISMA_TARIFA'
FROM db_reportes.otc_t_catalogo_consolidado_id  
where upper(tipo_movimiento) like '%CAMBIO%DE%PLAN%'
and upper(nombre_id)=UPPER('ID_tipo_MOVIMIENTO')
;


--N34
-- tabla temporal para inclusion de SOLICITUDES DE PORTABILIDAD
DROP TABLE ${ESQUEMA_TEMP}.tmp_rdb_solic_port_in;
CREATE TABLE ${ESQUEMA_TEMP}.tmp_rdb_solic_port_in AS
	SELECT distinct 
	ln_origen
	,telefono
	,fvc
	,created_when
	,salesorderprocesseddate
	,requeststatus
	,doc_number
	--LA SIGUIENTE TABLA FUE TRAIDA DESDE ORACLE CON SPARK CON EL QUERY DE CARLOS CASTILLO
	FROM db_desarrollo2021.sol_port_in_3
	--FROM db_desarrollo2021.r_om_portin_co 
	--cambiar por la tabla generada en el proceso SOLICITUDES DE PORTABILIDAD IN en SPARK con tablas de hive
	WHERE nvl(salesorderprocesseddate, created_when) BETWEEN '{fecha_port_ini}' AND '{fecha_port_fin}'
	and requeststatus in ('Approved','Partially Rejected','Pending in ASCP')
; 

--N35
DROP TABLE ${ESQUEMA_TEMP}.tmp_FECHA_ALTA_POS_HIST;
CREATE TABLE ${ESQUEMA_TEMP}.tmp_fecha_alta_pos_hist AS
SELECT 
	telefono
	, CAST(fecha_alta AS date) AS fecha_alta
	,linea_negocio_homologado
	,tipo_movimiento_mes
	, datediff(fecha_movimiento_mes, fecha_alta) as dias_transcurridos_baja
	,cliente
from db_desarrollo2021.otc_t_360_general_temp_final 
where linea_negocio_homologado='POSPAGO'
and tipo_movimiento_mes = 'TRANSFER_OUT'
and es_parque ='NO';


--N36
-- tabla final OTC_T_360_GENERAL
INSERT
	overwrite 
	TABLE db_desarrollo2021.d_otc_t_360_general 
	PARTITION(fecha_proceso)
SELECT
	DISTINCT 
	t1.telefono AS num_telefonico
	, t1.codigo_plan
	, (CASE
		WHEN t1.estado_abonado NOT IN('BAA', 'BAP') 
		THEN COALESCE(pp.usa_app
		, 'NO')
		ELSE 'NO'
	END) AS usa_app
	, (CASE
		WHEN t1.estado_abonado NOT IN('BAA', 'BAP')
		 THEN COALESCE(pp.usuario_app
		, 'NO')
		ELSE 'NO'
	END) AS usuario_app
	, t1.usa_movistar_play
	, t1.usuario_movistar_play
	, t1.fecha_alta
	, t1.nse
	, t1.sexo
	, t1.edad
	, t1.mes
	, t1.anio
	, t1.segmento
	, t1.linea_negocio
	, t1.linea_negocio_homologado
	, t1.forma_pago_factura
	, t1.forma_pago_alta
	, t1.estado_abonado
	, t1.sub_segmento
	, t1.numero_abonado
	, t1.account_num
	, t1.identificacion_cliente
	, t1.customer_ref
	, t1.tac
	, t1.tiene_bono
	, t1.valor_bono
	, t1.codigo_bono
	, t1.probabilidad_churn
	, t1.counted_days
	, t1.categoria_plan
	, t1.tarifa
	, t1.nombre_plan
	, t1.marca
	, t1.grupo_prepago
	, t1.fidelizacion_megas
	, t1.fidelizacion_dumy
	, t1.bancarizado
	, nvl(t1.bono_combero, '') AS bono_combero
	, t1.ticket_recarga
	, nvl(t1.tiene_score_tiaxa, 'NO') AS tiene_score_tiaxa
	, t1.score_1_tiaxa
	, t1.score_2_tiaxa
	, t1.tipo_doc_cliente
	, t1.cliente AS nombre_cliente
	, t1.ciclo_fact AS ciclo_facturacion
	, t1.email
	, t1.telefono_contacto
	, t1.fecha_ultima_renovacion
	, t1.address_2
	, t1.address_3
	, t1.address_4
	, t1.fecha_fin_contrato_definitivo
	, t1.vigencia_contrato
	, t1.version_plan
	, t1.fecha_ultima_renovacion_jn
	, t1.fecha_ultimo_cambio_plan
	, t1.tipo_movimiento_mes
	--nvl aumentado en REFACTORING para incluir fecha_movimiento_mes para NO_RECICLABLE 
	--cuya fecha_movimiento_mes viene null en otc_t_360_general_temp_final
	--, a1.fecha_movimiento_mes AS fecha_movimiento_mes
	, NVL(t1.fecha_movimiento_mes, a1.fecha_movimiento_mes) AS fecha_movimiento_mes
	, t1.es_parque
	, t1.banco
	, t1.parque_recargador
	, t1.segmento_fin AS segmento_parque
	, t1.susp_cobranza
	, t1.susp_911
	, t1.susp_cobranza_puntual
	, t1.susp_fraude
	, t1.susp_robo
	, t1.susp_voluntaria
	, t1.vencimiento_cartera
	, t1.saldo_cartera
	, a2.fecha_alta_historica as fecha_alta_historia
	, a2.canal_alta
	, a2.sub_canal_alta
	--, a2.nuevo_sub_canal_alta
	, a2.distribuidor_alta
	, a2.oficina_alta
	, a2.portabilidad
	, a2.operadora_origen
	, a2.operadora_destino
	, a2.motivo
	, a2.fecha_pre_pos
	, a2.canal_pre_pos
	, a2.sub_canal_pre_pos
	--, a2.nuevo_sub_canal_pre_pos
	, a2.distribuidor_pre_pos
	, a2.oficina_pre_pos
	, a2.fecha_pos_pre
	, a2.canal_pos_pre
	, a2.sub_canal_pos_pre
	--, a2.nuevo_sub_canal_pos_pre
	, a2.distribuidor_pos_pre
	, a2.oficina_pos_pre
	, a2.fecha_cambio_plan
	, a2.canal_cambio_plan
	, a2.sub_canal_cambio_plan
	--, a2.nuevo_sub_canal_cambio_plan
	, a2.distribuidor_cambio_plan
	, a2.oficina_cambio_plan
	, a2.cod_plan_anterior
	, a2.des_plan_anterior
	, a2.tb_descuento AS tb_descuento
	, a2.tb_override
	, a2.delta
	, a1.canal_comercial AS canal_movimiento_mes
	, a1.sub_canal AS sub_canal_movimiento_mes
	--, a1.nuevo_sub_canal_movimiento_mes
	, a1.nom_distribuidor AS distribuidor_movimiento_mes
	, a1.oficina_movimiento_mes
	, a1.portabilidad_movimiento_mes
	, a1.operadora_origen_movimiento_mes
	, a1.operadora_destino_movimiento_mes
	, a1.motivo_movimiento_mes
	, a1.cod_plan_anterior_movimiento_mes
	, a1.des_plan_anterior_movimiento_mes
	, a1.tb_descuento_movimiento_mes
	, a1.tb_override_movimiento_mes
	, a1.delta_movimiento_mes
	, a3.fecha_alta_cuenta
	, t1.fecha_inicio_pago_actual
	, t1.fecha_fin_pago_actual
	, t1.fecha_inicio_pago_anterior
	, t1.fecha_fin_pago_anterior
	, t1.forma_pago_anterior
	, a4.origen_alta_segmento
	, a4.fecha_alta_segmento
	, a5.dias_voz
	, a5.dias_datos
	, a5.dias_sms
	, a5.dias_conenido
	, a5.dias_total
	, t1.limite_credito
	, CAST(p1.adendum AS double)
	--, cast(t1.fecha_proceso as bigint) fecha_proceso
	, (CASE
		WHEN t1.estado_abonado NOT IN('BAA', 'BAP') 
		THEN pp.fecha_registro_app
		ELSE NULL
	END) AS fecha_registro_app
	, (CASE
		WHEN t1.estado_abonado NOT IN('BAA', 'BAP') 
		THEN pp.perfil
		ELSE 'NO'
	END) AS perfil
	, (CASE
		WHEN t1.estado_abonado NOT IN('BAA', 'BAP') 
		THEN COALESCE(wb.usuario_web, 'NO')
		ELSE 'NO'
	END) AS usuario_web
	, (CASE
		WHEN t1.estado_abonado NOT IN('BAA', 'BAP') 
		THEN wb.fecha_registro_web
		ELSE NULL
	END) AS fecha_registro_web
	--20210629 - SE AGREGA CAMPO FECHA NACIMIENTO
	--20210712 - Giovanny Cholca,  valida que la fecha actual -
	-- fecha de nacimiento no sea menor a 18 años,  si se cumple colocamos null al a la fecha de nacimiento
	, CASE
		WHEN round(datediff('{vIFechaEje1}'
		, COALESCE(CAST(cs.fecha_nacimiento AS varchar(12))
		, '{vIFechaEje1}'))/ 365.25) <18
		OR round(datediff('{vIFechaEje1}'
		, COALESCE(CAST(cs.fecha_nacimiento AS varchar(12))
		, '{vIFechaEje1}'))/ 365.25) > 120 THEN NULL
		ELSE cs.fecha_nacimiento
	END AS fecha_nacimiento
	-----------------------------------
	----------------Insertado en RF
	-------------------------------------
	, cat_tm.id_tipo_movimiento AS id_tipo_movimiento
	, a1.tipo AS tipo_movimiento
	, cat_sc.id_tipo_movimiento AS id_subcanal
	, cat_p.id_tipo_movimiento AS id_producto
	, a1.sub_movimiento
	, tec.tecnologia
	, (CASE WHEN a1.tipo in ('BAJA') 
	THEN datediff(a2.fecha_movimiento_baja, t1.fecha_alta)
			WHEN a1.tipo in ('POS_PRE') 
			THEN faph.dias_transcurridos_baja END) AS dias_transcurridos_baja
	, a2.dias_en_parque
	, a2.dias_en_parque_prepago
	, (CASE
		when a1.tipo IN ('ALTA','PRE_POS') 
		then  nvl(dnpy.detalle, descu.desc_conadis)
		WHEN a1.tipo IN ('DOWNSELL','UPSELL','MISMA_TARIFA') 
		THEN descu.desc_conadis
		ELSE ''	END) AS tipo_descuento_conadis
	, (CASE	when a1.tipo IN ('ALTA','PRE_POS','DOWNSELL','UPSELL','MISMA_TARIFA') 
		THEN descu.descripcion_descuento END) AS tipo_descuento
	, a1.ciudad
	, a1.provincia_activacion
	, a2.cod_categoria
	, a2.cod_da
	, a1.nom_usuario
	, a2.provincia_ivr
	, a2.provincia_ms
	, (CASE WHEN a1.tipo in ('BAJA') 
			THEN cast(t1.fecha_alta as date)
			WHEN a1.tipo in ('POS_PRE') 
			THEN faph.FECHA_ALTA END)  AS fecha_alta_pospago_historica
	, a2.vol_invol
	, a2.account_num_anterior
	--, a1.fecha_movimiento_mes
	, a1.imei
	, a1.equipo
	, a1.icc
	, a1.domain_login_ow
	, a1.nombre_usuario_ow
	, a1.domain_login_sub
	, a1.nombre_usuario_sub
	--, a1.oficina_movimiento_mes
	, a1.forma_pago
	, cat_c.id_tipo_movimiento AS id_canal
	, a1.campania_homologada AS campania
	, a1.codigo_distribuidor as codigo_distribuidor_movimiento_mes
	, a1.codigo_plaza
	, a1.nom_plaza as nom_plaza_movimiento_mes
	, a1.region_homologada AS region 
	, a1.ruc_distribuidor
	, (case when a1.tipo IN ('ALTA','PRE_POS') 
			then pati.ejecutivo_asignado
			when a1.tipo in ('BAJA','POS_PRE') 
			then pbto.ejecutivo_asignado end) as ejecutivo_asignado_ptr
	, (case when a1.tipo IN ('ALTA','PRE_POS') 
			then pati.area
			when a1.tipo in ('BAJA','POS_PRE') 
			then pbto.area end) AS area_ptr
	, (case when a1.tipo IN ('ALTA','PRE_POS') 
			then pati.codigo_vendedor_da
			when a1.tipo in ('BAJA','POS_PRE') 
			then pbto.codigo_vendedor_da end) AS codigo_vendedor_da_ptr
	, (case when a1.tipo IN ('ALTA','PRE_POS') 
			then pati.jefatura
			when a1.tipo in ('BAJA','POS_PRE') 
			then pbto.jefatura end) AS jefatura_ptr
	, a1.codigo_usuario
	, desp.descripcion AS descripcion_desp
	, a1.calf_riesgo
	, a1.cap_endeu
	, a1.valor_cred
	, a1.ciudad_usuario
	, a1.provincia_usuario
	, a2.linea_de_negocio_anterior
	, a2.cliente_anterior
	, a2.dias_reciclaje
	, a2.fecha_baja_reciclada
	, a2.tarifa_basica_anterior
	, a2.fecha_inicio_plan_anterior
	, (case when a1.tipo IN ('PRE_POS','DOWNSELL','UPSELL','MISMA_TARIFA') 
		THEN (nvl(t1.tarifa, ovw.mrc_ov_price) - nvl(descu.discount_value, 0))
			WHEN a1.tipo IN ('ALTA') 
			then (nvl(t1.tarifa, ovw.mrc_base_price) - nvl(descu.discount_value, 0)) end) as tarifa_final_plan_act
	--, a2.TARIFA_FINAL_PLAN_ACT
	--, (case when a1.tipo IN ('DOWNSELL','UPSELL','MISMA_TARIFA') THEN (a2.TARIFA_BASICA_ANTERIOR-) ) 
	, a2.tarifa_final_plan_ant
	, a2.mismo_cliente
	, (a2.tarifa_final_plan_act - a2.tarifa_final_plan_ant) AS delta_tarifa_final
	, (CASE 
			WHEN upper(spi.ln_origen) like '%POSTPAID%' THEN 'POSPAGO'
			WHEN upper(spi.ln_origen) like '%PREPAID%' THEN 'PREPAGO'
			ELSE '' END) AS tipo_de_cuenta_en_operador_donante
	, a2.fecha_alta_prepago
	, (case when UPPER(t1.es_parque) = 'NO' THEN t1.tarifa END) AS tarifa_basica_baja
	, a1.canal_transacc
	, a1.distribuidor_crm
	, (CASE	when a1.tipo IN ('ALTA','PRE_POS','DOWNSELL','UPSELL','MISMA_TARIFA') 
		THEN descu.discount_value END) AS descuento_tarifa_plan_act
	, (CASE	when a1.tipo IN ('PRE_POS','DOWNSELL','UPSELL','MISMA_TARIFA') 
			THEN ovw.mrc_ov_price
			WHEN a1.tipo IN ('ALTA') 
			THEN ovw.mrc_base_price END) AS tarifa_plan_actual_ov
	-------------------------------------
	---------FIN REFACTORING
	-------------------------------------
	, {vIFechaEje} AS fecha_proceso
FROM
----- tabla final del proceso OTC_360_GENERAL SQL 1-- proviene de PIVOT PARQUE
	{vTblInt23} t1
	-----------TABLA PRINCIPAL GENERADA EN MOVI PARQUE
LEFT JOIN db_desarrollo2021.otc_t_360_parque_1_tmp_t_mov a2 --vTblExt16
ON
	(t1.telefono = a2.num_telefonico)
	AND (t1.linea_negocio = a2.linea_negocio)
-----------TABLA SECUNDARIA GENERADA EN MOVI PARQUE:   CONTIENE RESULTADO DE UNIONS
LEFT JOIN db_desarrollo2021.otc_t_360_parque_1_mov_mes_tmp a1 --vTblExt17
ON
	(t1.telefono = a1.telefono)
	---LA LINEA DE ABAJO SE HA COMENTADO PARA QUE SE INCLUYAN LOS MOVIMIENTOS NO_RECICLABLE 
	--- LOS CUALES VIENEN SIN EL CAMPO fecha_movimiento_mes QUE GENERA EL CRUCE:
	--AND (t1.fecha_movimiento_mes = a1.fecha_movimiento_mes)
LEFT JOIN {vTblExt18} a3 
ON
	(t1.account_num = a3.cta_fact)
	-----------TERCERA TABLA GENERADA EN MOVI PARQUE
LEFT JOIN db_desarrollo2021.otc_t_360_parque_1_mov_seg_tmp a4   --vTblExt19
ON
	(t1.telefono = a4.telefono)
	--AND (t1.es_parque = 'SI')
LEFT JOIN {vTblExt20} a5 
ON
	(t1.telefono = a5.telefono)
	AND ({vIFechaEje} = a5.fecha_corte)
LEFT JOIN {vTblExt21} p1 
ON
	(t1.telefono = p1.phone_number)
LEFT JOIN {vTblExt22} pp 
ON
	(t1.telefono = pp.num_telefonico)
LEFT JOIN {vTblExt23} wb 
ON
	(t1.customer_ref = wb.cust_ext_ref)
	--20210629 - SE REALIZA EL CRUCE CON LA TEMPORAL PARA AGREGAR CAMPO FECHA NACIMIENTO
LEFT JOIN {vTblExt24} cs ON
	(t1.identificacion_cliente = cs.cedula)
	----------INSERTADO EN REFACTORING-------------------
	-------------\/\/\/\/\/\/\/\/\/\/--------------------------
LEFT JOIN db_reportes.otc_t_360_modelo tec ON
	t1.telefono = tec.num_telefonico
	AND ({vIFechaEje} = tec.fecha_proceso)
LEFT JOIN {vTblInt29} desp ON
	a1.icc = desp.icc
LEFT JOIN {vTblInt34} spi ON
	t1.telefono = spi.telefono
LEFT JOIN {vTblInt30} cat_c ON
	upper(a1.canal_comercial) = upper(cat_c.tipo_movimiento)
LEFT JOIN {vTblInt31} cat_sc ON
	upper(a1.sub_canal) = upper(cat_sc.tipo_movimiento)
LEFT JOIN {vTblInt32} cat_p ON
	upper(a1.sub_movimiento) = rtrim(upper(cat_p.tipo_movimiento))
LEFT JOIN {vTblInt33} cat_tm ON
	upper(a1.tipo) = upper(cat_tm.auxiliar)
LEFT JOIN  {vTblInt28} dnpy ON
	t1.telefono = dnpy.telefono
LEFT JOIN {vTblInt26} pati ON
	(t1.identificacion_cliente = pati.identificador)
LEFT JOIN {vTblInt27} pbto ON
	(t1.identificacion_cliente = pbto.identificador)
LEFT JOIN {vTblInt24} descu ON
	(t1.telefono = descu.phone_number)
	and (descu.tariff_plan_id=t1.codigo_plan)
LEFT JOIN {vTblInt25} ovw ON
	(t1.telefono = ovw.phone_number)
	and (ovw.tariff_plan_id=t1.codigo_plan)
LEFT JOIN {vTblInt35} faph ON
	(faph.telefono=t1.telefono)
----------/\/\/\/\/\/\/\/\/\/\/\/\-----------------
----------FIN DE REFACTORING-------------------
;

