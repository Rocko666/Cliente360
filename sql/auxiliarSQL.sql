DROP TABLE IF EXISTS db_desarrollo2021.d_otc_t_360_general;

CREATE TABLE db_desarrollo2021.d_otc_t_360_general(
  , codigo_plan string
, fecha_alta timestamp
, nse double
, sexo string
, edad double
, mes string
, anio string
, segmento string
, forma_pago_factura varchar(40)
, forma_pago_alta string
, sub_segmento string
, numero_abonado string
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

, nombre_cliente string
, ciclo_facturacion string
, email string
, telefono_contacto string
, fecha_ultima_renovacion date
, fecha_fin_contrato_definitivo date
, vigencia_contrato int
, version_plan int
, fecha_ultima_renovacion_jn date
, fecha_ultimo_cambio_plan date
, tipo_movimiento_mes string

, es_parque string
, banco string
, parque_recargador string
, segmento_parque string
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
, campania varchar(110)
, CODIGO_DISTRIBUIDOR_MOVIMIENTO_MES varchar(110)
, codigo_plaza varchar(110)
, nom_plaza_movimiento_mes varchar(110)
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



id_producto string
, linea_negocio string
, telefono string
, numero_abonado string
, account_num string
, fecha_movimiento date
, estado_abonado string
, cliente string
, DOCUMENTO_CLIENTE string
, tipo_doc_cliente string
CODIGO_PLAN





























