INSERT
	overwrite TABLE db_desarrollo2021.d_otc_t_360_general PARTITION(fecha_proceso)
SELECT
	DISTINCT 
t1.telefono AS num_telefonico
	, t1.codigo_plan
	, (CASE
		WHEN t1.estado_abonado NOT IN('BAA', 'BAP') THEN COALESCE(pp.usa_app
		, 'NO')
		ELSE 'NO'
	END) AS usa_app
	, (CASE
		WHEN t1.estado_abonado NOT IN('BAA', 'BAP') THEN COALESCE(pp.usuario_app
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
	--, A1.fecha_movimiento_mes AS fecha_movimiento_mes
	, NVL(t1.fecha_movimiento_mes, A1.fecha_movimiento_mes) AS fecha_movimiento_mes
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
	, A2.fecha_alta_historica as fecha_alta_historia
	, A2.CANAL_ALTA
	, A2.SUB_CANAL_ALTA
	--, A2.NUEVO_SUB_CANAL_ALTA
	, A2.DISTRIBUIDOR_ALTA
	, A2.OFICINA_ALTA
	, A2.PORTABILIDAD
	, A2.OPERADORA_ORIGEN
	, A2.OPERADORA_DESTINO
	, A2.MOTIVO
	, A2.FECHA_PRE_POS
	, A2.CANAL_PRE_POS
	, A2.SUB_CANAL_PRE_POS
	--, A2.NUEVO_SUB_CANAL_PRE_POS
	, A2.DISTRIBUIDOR_PRE_POS
	, A2.OFICINA_PRE_POS
	, A2.FECHA_POS_PRE
	, A2.CANAL_POS_PRE
	, A2.SUB_CANAL_POS_PRE
	--, A2.NUEVO_SUB_CANAL_POS_PRE
	, A2.DISTRIBUIDOR_POS_PRE
	, A2.OFICINA_POS_PRE
	, A2.FECHA_CAMBIO_PLAN
	, A2.CANAL_CAMBIO_PLAN
	, A2.SUB_CANAL_CAMBIO_PLAN
	--, A2.NUEVO_SUB_CANAL_CAMBIO_PLAN
	, A2.DISTRIBUIDOR_CAMBIO_PLAN
	, A2.OFICINA_CAMBIO_PLAN
	, A2.COD_PLAN_ANTERIOR
	, A2.DES_PLAN_ANTERIOR
	, A2.TB_DESCUENTO AS TB_DESCUENTO
	, A2.TB_OVERRIDE
	, A2.DELTA
	, nvl(A1.CANAL_MOVIMIENTO_MES, A1.CANAL_COMERCIAL) as CANAL_MOVIMIENTO_MES
	, nvl (A1.SUB_CANAL_MOVIMIENTO_MES, a1.sub_canal) as SUB_CANAL_MOVIMIENTO_MES
	--, A1.NUEVO_SUB_CANAL_MOVIMIENTO_MES
	, nvl (A1.DISTRIBUIDOR_MOVIMIENTO_MES, A1.NOM_DISTRIBUIDOR) as DISTRIBUIDOR_MOVIMIENTO_MES
	, A1.OFICINA_MOVIMIENTO_MES
	, A1.PORTABILIDAD_MOVIMIENTO_MES
	, A1.OPERADORA_ORIGEN_MOVIMIENTO_MES
	, A1.OPERADORA_DESTINO_MOVIMIENTO_MES
	, A1.MOTIVO_MOVIMIENTO_MES
	, A1.COD_PLAN_ANTERIOR_MOVIMIENTO_MES
	, A1.DES_PLAN_ANTERIOR_MOVIMIENTO_MES
	, A1.TB_DESCUENTO_MOVIMIENTO_MES
	, A1.TB_OVERRIDE_MOVIMIENTO_MES
	, A1.DELTA_MOVIMIENTO_MES
	, A3.Fecha_Alta_Cuenta
	, t1.fecha_inicio_pago_actual
	, t1.fecha_fin_pago_actual
	, t1.fecha_inicio_pago_anterior
	, t1.fecha_fin_pago_anterior
	, t1.forma_pago_anterior
	, A4.origen_alta_segmento
	, A4.fecha_alta_segmento
	, A5.dias_voz
	, A5.dias_datos
	, A5.dias_sms
	, A5.dias_conenido
	, A5.dias_total
	, t1.limite_credito
	, CAST(p1.adendum AS double)
	--, cast(t1.fecha_proceso as bigint) fecha_proceso
	, (CASE
		WHEN t1.estado_abonado NOT IN('BAA', 'BAP') THEN pp.fecha_registro_app
		ELSE NULL
	END) AS fecha_registro_app
	, (CASE
		WHEN t1.estado_abonado NOT IN('BAA', 'BAP') THEN pp.perfil
		ELSE 'NO'
	END) AS perfil
	, (CASE
		WHEN t1.estado_abonado NOT IN('BAA', 'BAP') THEN COALESCE(wb.usuario_web, 'NO')
		ELSE 'NO'
	END) AS usuario_web
	, (CASE
		WHEN t1.estado_abonado NOT IN('BAA', 'BAP') THEN wb.fecha_registro_web
		ELSE NULL
	END) AS fecha_registro_web
	--20210629 - SE AGREGA CAMPO FECHA NACIMIENTO
	--20210712 - Giovanny Cholca,  valida que la fecha actual -
	-- fecha de nacimiento no sea menor a 18 a??os,  si se cumple colocamos null al a la fecha de nacimiento
	, CASE
		WHEN round(datediff('2022-08-01'
		, COALESCE(CAST(cs.fecha_nacimiento AS varchar(12))
		, '2022-08-01'))/ 365.25) <18
		OR round(datediff('2022-08-01'
		, COALESCE(CAST(cs.fecha_nacimiento AS varchar(12))
		, '2022-08-01'))/ 365.25) > 120 THEN NULL
		ELSE cs.fecha_nacimiento
	END AS fecha_nacimiento
	-----------------------------------
	----------------Insertado en RF
	-------------------------------------
	, cat_tm.id_tipo_movimiento AS id_tipo_movimiento
	, A1.TIPO AS tipo_movimiento
	, cat_sc.id_tipo_movimiento AS id_subcanal
	, cat_p.ID_TIPO_MOVIMIENTO AS id_producto
	, A1.SUB_MOVIMIENTO
	, TEC.TECNOLOGIA
	, datediff(A2.FECHA_MOVIMIENTO_BAJA, t1.fecha_alta) AS DIAS_TRANSCURRIDOS_BAJA
	, A2.DIAS_EN_PARQUE
	, A2.DIAS_EN_PARQUE_PREPAGO
	, (CASE
		when A1.TIPO IN ('ALTA','PRE_POS') AND t1.linea_negocio_homologado='POSPAGO' then  DNPY.DETALLE
		WHEN upper(A2.descripcion_descuento_plan_act) LIKE '%CONADIS%' THEN 'CONADIS'
		WHEN upper(A2.descripcion_descuento_plan_act) LIKE '%ADULTO%MAYOR%' THEN 'CONADIS'
		ELSE ''	END) AS TIPO_DESCUENTO_CONADIS
	, A2.descripcion_descuento_plan_act AS TIPO_DESCUENTO
	, A1.CIUDAD
	, A1.PROVINCIA_ACTIVACION
	, A2.COD_CATEGORIA
	, A2.COD_DA
	, A1.NOM_USUARIO
	, A2.PROVINCIA_IVR
	, A2.PROVINCIA_MS
	, (CASE WHEN A1.TIPO in ('BAJA','POS_PRE') 
		THEN t1.fecha_alta END)  AS FECHA_ALTA_POSPAGO_HISTORICA
	, A2.VOL_INVOL
	, A2.ACCOUNT_NUM_ANTERIOR
	--, A1.FECHA_MOVIMIENTO_MES
	, A1.IMEI
	, A1.EQUIPO
	, A1.ICC
	, A1.DOMAIN_LOGIN_OW
	, A1.NOMBRE_USUARIO_OW
	, A1.DOMAIN_LOGIN_SUB
	, A1.NOMBRE_USUARIO_SUB
	--, A1.OFICINA_MOVIMIENTO_MES
	, A1.FORMA_PAGO
	, cat_c.id_tipo_movimiento AS id_canal
	, A1.CAMPANIA
	, nvl (A1.CODIGO_DISTRIBUIDOR_MOVIMIENTO_MES, A1.CODIGO_DISTRIBUIDOR) as CODIGO_DISTRIBUIDOR_MOVIMIENTO_MES
	, A1.CODIGO_PLAZA
	, nvl (A1.nom_plaza_MOVIMIENTO_MES, A1.NOM_PLAZA) as nom_plaza_MOVIMIENTO_MES
	, A1.REGION
	, A1.RUC_DISTRIBUIDOR
	, (CASE WHEN A1.EJECUTIVO_ASIGNADO_PTR='' THEN nvl(PBTO.EJECUTIVO_ASIGNADO, PATI.EJECUTIVO_ASIGNADO) ELSE 
	nvl(A1.EJECUTIVO_ASIGNADO_PTR, nvl(PBTO.EJECUTIVO_ASIGNADO, PATI.EJECUTIVO_ASIGNADO)) end ) as EJECUTIVO_ASIGNADO_PTR
	--, nvl(A1.EJECUTIVO_ASIGNADO_PTR, nvl(PBTO.EJECUTIVO_ASIGNADO, PATI.EJECUTIVO_ASIGNADO)) as EJECUTIVO_ASIGNADO_PTR
	, (CASE WHEN A1.AREA_PTR='' THEN NVL(PBTO.AREA, PATI.AREA) ELSE
	NVL(A1.AREA_PTR,NVL(PBTO.AREA, PATI.AREA)) end ) AS AREA_PTR
	--, NVL(A1.AREA_PTR,NVL(PBTO.AREA, PATI.AREA)) AS AREA_PTR
	, (CASE WHEN A1.CODIGO_VENDEDOR_DA_PTR='' THEN NVL (PBTO.CODIGO_VENDEDOR_DA, PATI.CODIGO_VENDEDOR_DA) ELSE 
	NVL(A1.CODIGO_VENDEDOR_DA_PTR, NVL (PBTO.CODIGO_VENDEDOR_DA, PATI.CODIGO_VENDEDOR_DA)) end ) AS CODIGO_VENDEDOR_DA_PTR
	--, NVL(A1.CODIGO_VENDEDOR_DA_PTR, NVL (PBTO.CODIGO_VENDEDOR_DA, PATI.CODIGO_VENDEDOR_DA)) AS CODIGO_VENDEDOR_DA_PTR
	, (CASE WHEN A1.JEFATURA_PTR='' THEN NVL(PBTO.JEFATURA, PATI.JEFATURA) ELSE
	NVL(A1.JEFATURA_PTR, NVL(PBTO.JEFATURA, PATI.JEFATURA)) end ) AS JEFATURA_PTR
	--, NVL(A1.JEFATURA_PTR, NVL(PBTO.JEFATURA, PATI.JEFATURA)) AS JEFATURA_PTR
	, A1.CODIGO_USUARIO
	, desp.DESCRIPCION AS DESCRIPCION_DESP
	, A1.CALF_RIESGO
	, A1.CAP_ENDEU
	, A1.VALOR_CRED
	, A1.CIUDAD_USUARIO
	, A1.PROVINCIA_USUARIO
	, A2.LINEA_DE_NEGOCIO_ANTERIOR
	, A2.CLIENTE_ANTERIOR
	, A2.DIAS_RECICLAJE
	, A2.FECHA_BAJA_RECICLADA
	, A2.TARIFA_BASICA_ANTERIOR
	, A2.FECHA_INICIO_PLAN_ANTERIOR
	, A2.TARIFA_FINAL_PLAN_ACT
	, A2.TARIFA_FINAL_PLAN_ANT
	, A2.MISMO_CLIENTE
	, (A2.TARIFA_FINAL_PLAN_ACT - A2.TARIFA_FINAL_PLAN_ANT) AS DELTA_TARIFA_FINAL
	, (CASE 
			WHEN upper(SPI.ln_origen) like '%POSTPAID%' THEN 'POSPAGO'
			WHEN upper(SPI.ln_origen) like '%PREPAID%' THEN 'PREPAGO'
			ELSE '' END) AS TIPO_DE_CUENTA_EN_OPERADOR_DONANTE
	, A2.FECHA_ALTA_PREPAGO
	, (case when UPPER(t1.es_parque) = 'NO' THEN t1.tarifa END) AS TARIFA_BASICA_BAJA
	, A1.canal_transacc
	, A1.distribuidor_crm
	, A2.descuento_tarifa_plan_act
	, A2.tarifa_plan_actual_ov
	, DNPY.DETALLE AS DESC_NO_PYMES
	-------------------------------------
	---------FIN REFACTORING
	-------------------------------------
	, ${FECHAEJE} AS fecha_proceso
FROM
----- tabla final del proceso OTC_360_GENERAL SQL 1-- proviene de PIVOT PARQUE
	${ESQUEMA_TEMP}.otc_t_360_general_temp_final t1
	-----------TABLA PRINCIPAL GENERADA EN MOVI PARQUE
LEFT JOIN ${ESQUEMA_TEMP}.otc_t_360_parque_1_tmp_t_mov A2 
ON
	(t1.TELEFONO = A2.NUM_TELEFONICO)
	AND (t1.LINEA_NEGOCIO = a2.LINEA_NEGOCIO)
-----------TABLA SECUNDARIA GENERADA EN MOVI PARQUE:   CONTIENE RESULTADO DE UNIONS
LEFT JOIN ${ESQUEMA_TEMP}.OTC_T_360_PARQUE_1_MOV_MES_TMP A1 
ON
	(t1.TELEFONO = A1.TELEFONO)
LEFT JOIN db_temporales.otc_t_cuenta_num_tmp A3 
ON
	(t1.account_num = A3.cta_fact)
	-----------TERCERA TABLA GENERADA EN MOVI PARQUE
LEFT JOIN ${ESQUEMA_TEMP}.otc_t_360_parque_1_mov_seg_tmp A4 
ON  (t1.TELEFONO = A4.TELEFONO)
--AND (t1.es_parque = 'SI')
LEFT JOIN db_temporales.OTC_T_parque_traficador_dias_tmp A5 
ON	(t1.TELEFONO = A5.TELEFONO)
	AND (${FECHAEJE} = A5.fecha_corte)
LEFT JOIN db_temporales.otc_t_360_general_temp_adendum p1 
ON	(t1.TELEFONO = p1.phone_number)
LEFT JOIN db_temporales.tmp_360_app_mi_movistar pp 
ON	(t1.telefono = pp.num_telefonico)
LEFT JOIN db_temporales.tmp_360_web wb 
ON	(t1.customer_ref = wb.cust_ext_ref)
	--20210629 - SE REALIZA EL CRUCE CON LA TEMPORAL PARA AGREGAR CAMPO FECHA NACIMIENTO
LEFT JOIN db_temporales.tmp_fecha_nacimiento_mvp cs ON
	(t1.identificacion_cliente = cs.cedula)
	----------INSERTADO EN REFACTORING-------------------
	-------------\/\/\/\/\/\/\/\/\/\/--------------------------
LEFT JOIN db_reportes.otc_t_360_modelo TEC ON
	t1.TELEFONO = TEC.num_telefonico
	AND (${FECHAEJE} = TEC.fecha_proceso)
LEFT JOIN ${ESQUEMA_TEMP}.tmp_desp_nc_final desp ON
	A1.icc = desp.icc
LEFT JOIN ${ESQUEMA_TEMP}.tmp_rdb_solic_port_in SPI ON
	t1.TELEFONO = SPI.telefono
LEFT JOIN ${ESQUEMA_TEMP}.tmp_otc_t_cat_id_canal cat_c ON
	upper(nvl(A1.CANAL_MOVIMIENTO_MES, A1.CANAL_COMERCIAL)) = upper(cat_c.tipo_movimiento)
LEFT JOIN ${ESQUEMA_TEMP}.tmp_otc_t_cat_id_sub_canal cat_sc ON
	upper(nvl (A1.SUB_CANAL_MOVIMIENTO_MES, a1.sub_canal)) = upper(cat_sc.tipo_movimiento)
LEFT JOIN ${ESQUEMA_TEMP}.tmp_otc_t_cat_id_producto cat_p ON
	upper(A1.SUB_MOVIMIENTO) = rtrim(upper(cat_p.tipo_movimiento))
LEFT JOIN ${ESQUEMA_TEMP}.tmp_otc_t_cat_id_tipo_mov cat_tm ON
	upper(A1.tipo) = upper(cat_tm.auxiliar)
LEFT JOIN  ${ESQUEMA_TEMP}.tmp_desc_no_pymes DNPY ON
	t1.TELEFONO = DNPY.TELEFONO
LEFT JOIN ${ESQUEMA_TEMP}.tmp_PRMT_ALTA_TI PATI ON
	(t1.identificacion_cliente = PATI.identificador)
LEFT JOIN ${ESQUEMA_TEMP}.tmp_PRMT_BAJA_TO PBTO ON
	(t1.identificacion_cliente = PBTO.identificador)
----------/\/\/\/\/\/\/\/\/\/\/\/\-----------------
----------FIN DE REFACTORING-------------------
;