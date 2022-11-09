
--HQL de shell OTC_T_360_GENERAL


set hive.cli.print.header=false;	
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
			
--SE A?DE LA FORMA DE PAGO AL PARQUE L?EA A L?EA
--NO DEPENEDENCIA DE MOVI PARQUE 5AM EN PROMEDIO
drop table if exists ${ESQUEMA_TEMP}.otc_t_360_parque_mop_1_tmp;
create table ${ESQUEMA_TEMP}.otc_t_360_parque_mop_1_tmp as
select *
from(
SELECT num_telefonico,
forma_pago,
row_number() over (partition by num_telefonico order by fecha_alta asc) as orden
FROM db_cs_altas.otc_t_nc_movi_parque_v1
WHERE fecha_proceso = ${fechamas1}
) t
where t.orden=1;			

--SE OBTIENE A PARTIR DE LA 360 MODELO EL TAC DE TRAFICO DE CADA L?EA
--NO DEPENDE DE 360 MODELO
drop table if exists ${ESQUEMA_TEMP}.otc_t_360_imei_tmp;
create table ${ESQUEMA_TEMP}.otc_t_360_imei_tmp as
SELECT ime.num_telefonico num_telefonico, ime.tac tac
FROM db_reportes.otc_t_360_modelo ime
where fecha_proceso=${FECHAEJE};

--SE OBTIENEN LOS NUMEROS TELEFONICOS QUE USAN LA APP MI MOVISTAR
--NO FUENTE DESACTUALIZADA DESCONTINUADA DESDE JUNIO 2020, PARA ESTE CAMPO SE USA UNA NUEVA TABLA HAY QUE LIMPIAR Y QUITAR ESTE QUERY Y LO POSTERIOR QUE TENGA QUE VER CON ESTE
drop table if exists ${ESQUEMA_TEMP}.otc_t_360_usa_app_tmp;
create table ${ESQUEMA_TEMP}.otc_t_360_usa_app_tmp as
select numero_telefono, count(1) total
from ${ESQUEMA_TABLA_1}.otc_t_usuariosactivos
where fecha_proceso > ${fechamenos1mes}
and fecha_proceso < ${fechamas1}
group by numero_telefono
having count(1)>0;

--SE OBTIENEN LOS NUMEROS TELEFONICOS REGISTRADOS EN LA APP MI MOVISTAR					
--NO FUENTE DESACTUALIZADA DESCONTINUADA DESDE JUNIO 2020, PARA ESTE CAMPO SE USA UNA NUEVA TABLA HAY QUE LIMPIAR Y QUITAR ESTE QUERY Y LO POSTERIOR QUE TENGA QUE VER CON ESTE
drop table if exists ${ESQUEMA_TEMP}.otc_t_360_usuario_app_tmp;
create table ${ESQUEMA_TEMP}.otc_t_360_usuario_app_tmp as
select celular numero_telefono, count(1) total
-- desde db_thebox
from ${ESQUEMA_TABLA_1}.otc_t_rep_usuarios_registrados
group by celular
having count(1)>0;

--NO
drop table if exists ${ESQUEMA_TEMP}.otc_t_360_bonos_devengo_tmp;
create table ${ESQUEMA_TEMP}.otc_t_360_bonos_devengo_tmp as 
select 
a.num_telefono as numero_telefono,
sum(b.imp_coste/1.12)/1000 as valor_bono,
a.cod_bono as codigo_bono,
a.fec_alta
from db_rdb.otc_t_ppga_adquisiciones a
left join db_rdb.otc_t_ppga_actabopre b
on (b.fecha > ${fechamenos1mes} and b.fecha < ${fechamas1}
and a.fecha > ${fechamenos1mes} and a.fecha < ${fechamas1}
and a.num_telefono=b.num_telefono
and a.sec_actuacion=b.sec_actuacion
and a.cod_particion=b.cod_particion)
inner join db_rdb.otc_t_oferta_comercial_comberos t3 
on t3.cod_aa=a.cod_bono
where a.sec_baja is null
and b.cod_actuacio='AB'
and b.cod_estarec='EJ'
and b.fecha > ${fechamenos1mes} and b.fecha < ${fechamas1}
and a.fecha > ${fechamenos1mes} and a.fecha < ${fechamas1}
and b.imp_coste > 0
group by a.num_telefono, 
a.cod_bono,
a.fec_alta;

--NO, LA DEPENDENCIA ESTA A LAS 8 revisando Kathy
drop table if exists ${ESQUEMA_TEMP}.otc_t_360_bonos_all_tmp;
create table ${ESQUEMA_TEMP}.otc_t_360_bonos_all_tmp as
select t1.numero_telefono, sum(t1.valor_bono) as valor_bono, t1.codigo_bono, t1.fecha
from (select b.numero_telefono, b.valor_bono, b.codigo_bono,b.fecha
from(
select t.c_customer_id numero_telefono, t1.valor valor_bono, t1.cod_aa codigo_bono,cast(t.c_transaction_datetime as date) as fecha
,row_number() over (partition by t.c_customer_id order by t.c_transaction_datetime DESC) as id
from ${ESQUEMA_TABLA_3}.otc_t_pmg_bonos_combos t
-- LINEA COMENTADA en  REFACTORING por PROBLEMA CON TABLA TEMPORAL PIVOT PARQUE
--inner join db_temporales.otc_t_360_parque_1_tmp t2 on t2.num_telefonico= t.c_customer_id and upper(t2.linea_negocio) like 'PRE%'
inner join db_desarrollo2021.otc_t_360_parque_1_tmp t2 on t2.num_telefonico= t.c_customer_id and upper(t2.linea_negocio) like 'PRE%'
inner join db_dwec.OTC_T_CTL_BONOS t1 on t1.operacion=t.c_packet_code
where t.fecha_proceso > ${fechamenos2mes}
and t.fecha_proceso < ${fechamas1}
) b
where b.id=1
union all
select numero_telefono, valor_bono, codigo_bono, fec_alta as fecha from ${ESQUEMA_TEMP}.otc_t_360_bonos_devengo_tmp) as t1
group by t1.numero_telefono, t1.codigo_bono, t1.fecha;

--NO DEPENDE DEL ANTERIOR
drop table if exists ${ESQUEMA_TEMP}.otc_t_360_bonos_tmp;
create table ${ESQUEMA_TEMP}.otc_t_360_bonos_tmp as
select t1.numero_telefono, t1.valor_bono, t1.codigo_bono, t1.fecha
from (select numero_telefono, valor_bono, codigo_bono, fecha,
		row_number() over (partition by numero_telefono order by fecha DESC) as orden
	from ${ESQUEMA_TEMP}.otc_t_360_bonos_all_tmp
	) as t1
where orden=1;

--NO, DEPENDE DEL ANTERIOR Y DE PIVOTE PARQUE
drop table if exists ${ESQUEMA_TEMP}.otc_t_360_combero_all_tmp;
create table ${ESQUEMA_TEMP}.otc_t_360_combero_all_tmp as
select t1.numero_telefono, sum(t1.valor_bono) as valor_bono, t1.codigo_bono, t1.fecha
from (select b.numero_telefono, b.valor_bono, b.codigo_bono, b.fecha
from(
select t.c_customer_id numero_telefono, t1.valor_con_iva valor_bono, t1.bono codigo_bono,cast(t.c_transaction_datetime as date) as fecha
,row_number() over (partition by t.c_customer_id order by t.c_transaction_datetime DESC) as id
from db_payment_manager.otc_t_pmg_bonos_combos t
--LINEA COMENTADA en  REFACTORING por PROBLEMA CON TABLA TEMPORAL PIVOT PARQUE
--inner join db_temporales.otc_t_360_parque_1_tmp t2 on t2.num_telefonico= t.c_customer_id and upper(t2.linea_negocio) like 'PRE%'
inner join db_desarrollo2021.otc_t_360_parque_1_tmp t2 on t2.num_telefonico= t.c_customer_id and upper(t2.linea_negocio) like 'PRE%'
inner join db_reportes.cat_bonos_pdv t1 on t1.codigo_pm=t.c_packet_code
inner join db_rdb.otc_t_oferta_comercial_comberos t3 on t3.cod_aa=t1.bono
where t.fecha_proceso > ${fechamenos1mes}
and t.fecha_proceso < ${fechamas1}
) b
where b.id=1
union all
select numero_telefono, valor_bono, codigo_bono, fec_alta as fecha from ${ESQUEMA_TEMP}.otc_t_360_bonos_devengo_tmp) as t1
group by t1.numero_telefono, t1.codigo_bono, t1.fecha;

--NO, DEPENDE DEL ANTERIOR
drop table if exists ${ESQUEMA_TEMP}.otc_t_360_combero_tmp;
create table ${ESQUEMA_TEMP}.otc_t_360_combero_tmp as
select t1.numero_telefono, t1.valor_bono, t1.codigo_bono, t1.fecha
from (select numero_telefono, valor_bono, codigo_bono, fecha,
		row_number() over (partition by numero_telefono order by fecha DESC) as orden
	from ${ESQUEMA_TEMP}.otc_t_360_combero_all_tmp
	) as t1
where orden=1;					

--NO, POR DEPENDENCIA DE PIVOTE PARQUE
drop table if exists ${ESQUEMA_TEMP}.otc_t_360_homologacion_segmentos;
create table ${ESQUEMA_TEMP}.otc_t_360_homologacion_segmentos as
select distinct UPPER(a.sub_segmento) sub_segmento, b.segmento,b.segmento_fin
-- LINEA COMENTADA en  REFACTORING por PROBLEMA CON TABLA TEMPORAL PIVOT PARQUE
--from db_temporales.otc_t_360_parque_1_tmp a
from db_desarrollo2021.otc_t_360_parque_1_tmp a
--- EXISTE DEPENDENCIA: tabla desde proceso GENERAL PREVIO
inner join db_temporales.otc_t_360_homologacion_segmentos_1 b
on b.segmentacion = (case when UPPER(a.sub_segmento) = 'ROAMING' then 'ROAMING XDR'
						when UPPER(a.sub_segmento) like 'PEQUE%' then 'PEQUENAS'
						when UPPER(a.sub_segmento) like 'TELEFON%P%BLICA' then 'TELEFONIA PUBLICA'
						when UPPER(a.sub_segmento) like 'CANALES%CONSIGNACI%' then 'CANALES CONSIGNACION'
						when UPPER(a.sub_segmento) like '%CANALES%SIMCARDS%(FRANQUICIAS)%' then 'CANALES SIMCARDS (FRANQUICIAS)'
						else UPPER(a.sub_segmento) end);

--NO, POR DEPENDENCIA DE PIVOTE PARQUE
drop table if exists ${ESQUEMA_TEMP}.otc_t_360_general_temp_1;
create table ${ESQUEMA_TEMP}.otc_t_360_general_temp_1 as
select t.num_telefonico telefono,
t.codigo_plan,
t.fecha_proceso,
case when nvl(t6.total,0) > 0 then 'SI' else 'NO' end USA_APP,
case when nvl(t7.total,0) > 0 then 'SI' else 'NO' end USUARIO_APP,
case when t9.numero_telefono is not null then 'SI' else 'NO' end USA_MOVISTAR_PLAY,
case when t10.numero_telefono is not null then 'SI' else 'NO' end USUARIO_MOVISTAR_PLAY,					
t.fecha_alta,
t4.sexo,
t4.edad,
substr(t.fecha_proceso, 5, 2) mes,
substr(t.fecha_proceso, 1, 4) anio,
UPPER(t3.segmento) segmento,
upper(t3.segmento_fin) segmento_fin,
t.linea_negocio,
t14.payment_method_name forma_pago_factura,
t1.forma_pago forma_pago_alta,
t.estado_abonado,
UPPER(t.sub_segmento) sub_segmento,
t.numero_abonado,
t.account_num,
t.identificacion_cliente,
t.customer_ref,
t5.tac,
case when t8.numero_telefono is null then 'NO' else 'SI' end TIENE_BONO,
t8.valor_bono,
t8.codigo_bono,
case when upper(t.linea_negocio) like 'PRE%' then t11.prob_churn else t12.prob_churn end probabilidad_churn
,t.COUNTED_DAYS
,t.LINEA_NEGOCIO_HOMOLOGADO
,t.categoria_plan
,t.tarifa
,t.nombre_plan
,t.marca
,t.tipo_doc_cliente
,t.cliente
,t.ciclo_fact
,t.correo_cliente_pr
,t.telefono_cliente_pr
,t.tipo_movimiento_mes
,t.fecha_movimiento_mes
,t.es_parque
,t.banco
,t14.start_dat fecha_inicio_pago_actual
,t14.end_dat fecha_fin_pago_actual
,t13.start_dat fecha_inicio_pago_anterior
,t13.end_dat fecha_fin_pago_anterior
,t13.payment_method_name forma_pago_anterior
-- LINEA COMENTADA en  REFACTORING por PROBLEMA CON TABLA TEMPORAL PIVOT PARQUE
--from db_temporales.otc_t_360_parque_1_tmp t
from db_desarrollo2021.otc_t_360_parque_1_tmp t
left join ${ESQUEMA_TEMP}.otc_t_360_parque_mop_1_tmp t1 on t1.num_telefonico= t.num_telefonico
-- DEPENDENCIA PROCESO 360 GENERAL PREVIO
left join db_temporales.otc_t_360_mop_defecto_tmp t13 on t13.account_num= t.account_num and t13.orden=2
left join db_temporales.otc_t_360_mop_defecto_tmp t14 on t14.account_num= t.account_num and t14.orden=1
left outer join ${ESQUEMA_TEMP}.otc_t_360_homologacion_segmentos t3 on upper(t3.sub_segmento) = upper(t.sub_segmento)
left outer join db_temporales.otc_t_360_parque_edad_tmp t4 on t4.num_telefonico=t.num_telefonico
left outer join ${ESQUEMA_TEMP}.otc_t_360_imei_tmp t5 on t5.num_telefonico = t.num_telefonico
left outer join ${ESQUEMA_TEMP}.otc_t_360_usa_app_tmp t6 on t6.numero_telefono = t.num_telefonico
left outer join ${ESQUEMA_TEMP}.otc_t_360_usuario_app_tmp t7 on t7.numero_telefono = t.num_telefonico
left outer join db_temporales.otc_t_360_usa_app_movi_tmp t9 on t9.numero_telefono = t.num_telefonico
left outer join db_temporales.otc_t_360_usuario_app_movi_tmp t10 on t10.numero_telefono = t.num_telefonico
left outer join ${ESQUEMA_TEMP}.otc_t_360_bonos_tmp t8 on t8.numero_telefono = t.num_telefonico
left outer join db_temporales.otc_t_360_prob_churn_pre_temp t11 on t11.num_telefonico = t.num_telefonico
left outer join db_temporales.otc_t_360_prob_churn_pos_temp t12 on t12.num_telefonico = t.num_telefonico
where 1=1
group by t.num_telefonico,
t.codigo_plan,
t.fecha_proceso,
case when nvl(t6.total,0) > 0 then 'SI' else 'NO' end,
case when nvl(t7.total,0) > 0 then 'SI' else 'NO' end,
case when t9.numero_telefono is not null then 'SI' else 'NO' end,
case when t10.numero_telefono is not null then 'SI' else 'NO' end,
t.fecha_alta,
t4.sexo,
t4.edad,
substr(t.fecha_proceso, 5, 2),
substr(t.fecha_proceso, 1, 4),
UPPER(t3.segmento),
t.linea_negocio,
t14.payment_method_name,
t1.forma_pago,
t.estado_abonado,
UPPER(t.sub_segmento),
UPPER(t3.segmento_fin),		
t.numero_abonado,
t.account_num,
t.identificacion_cliente,
t.customer_ref,
t5.tac,
case when t8.numero_telefono is null then 'NO' else 'SI' end,
t8.valor_bono,
t8.codigo_bono,
case when upper(t.linea_negocio) like 'PRE%' then t11.prob_churn else t12.prob_churn end
,t.COUNTED_DAYS
,t.LINEA_NEGOCIO_HOMOLOGADO
,t.categoria_plan
,t.tarifa
,t.nombre_plan
,t.marca
,t.tipo_doc_cliente
,t.cliente
,t.ciclo_fact
,t.correo_cliente_pr
,t.telefono_cliente_pr
,t.tipo_movimiento_mes
,t.fecha_movimiento_mes
,t.es_parque
,t.banco
,t14.start_dat
,t14.end_dat
,t13.start_dat
,t13.end_dat
,t13.payment_method_name;
										
--NO, POR DEPENDENCIA DE PIVOTE PARQUE Y RECARGAS PERO SE PUEDE HABRIR A UN TERCER JOB										
drop table if exists ${ESQUEMA_TEMP}.otc_t_360_general_temp;
create table ${ESQUEMA_TEMP}.otc_t_360_general_temp as
select 
a.*
,case when (coalesce(b.ingreso_recargas_m0,0)+coalesce(b.ingreso_combos,0)+coalesce(b.ingreso_bonos,0)) >0 then 'SI' else 'NO' end as PARQUE_RECARGADOR 
from ${ESQUEMA_TEMP}.otc_t_360_general_temp_1 a 
left join db_temporales.tmp_otc_t_360_recargas b
on a.telefono=b.numero_telefono;

--NO, POR DEPENDENCIA DE PIVOTE_PARQUE
drop table if exists ${ESQUEMA_TEMP}.otc_t_360_ticket_rec_tmp;
create table ${ESQUEMA_TEMP}.otc_t_360_ticket_rec_tmp as
select t1.mes,t2.linea_negocio,t1.telefono, 
sum(t1.total_rec_bono) as valor_recarga_base, 
sum(total_cantidad) as cantidad_recargas,
sum(t1.total_rec_bono)/sum(total_cantidad)  as ticket_mes,
count(telefono) as cant
--LINEA COMENTADA en  REFACTORING por PROBLEMA CON TABLA TEMPORAL PIVOT PARQUE
--from db_temporales.tmp_360_ticket_recarga t1, db_temporales.otc_t_360_parque_1_tmp t2 
from db_temporales.tmp_360_ticket_recarga t1, db_desarrollo2021.otc_t_360_parque_1_tmp t2 
where t2.num_telefonico=t1.telefono and t2.linea_negocio_homologado ='PREPAGO'
group by t1.mes,t2.linea_negocio,t1.telefono;

--NO, POR DEPENDENCIA DEL ALTERIOR QUERY
drop table if exists ${ESQUEMA_TEMP}.otc_t_360_ticket_fin_tmp;
create table ${ESQUEMA_TEMP}.otc_t_360_ticket_fin_tmp as
select telefono, 
sum(nvl(ticket_mes,0)) as ticket_mes, 
sum(nvl(cant,0)) as cant,
sum(nvl(ticket_mes,0))/sum(nvl(cant,0)) as ticket
from ${ESQUEMA_TEMP}.otc_t_360_ticket_rec_tmp
group by telefono;

--NO, DEPENEDE DE 360 NSE
drop table if exists ${ESQUEMA_TEMP}.otc_t_360_hog_nse_tmp_cal;
create table ${ESQUEMA_TEMP}.otc_t_360_hog_nse_tmp_cal as
select numero_telefono as telefono, nse 
from db_reportes.otc_t_360_nse where fecha_proceso=${FECHAEJE};

--NO, PARA UN TERCER PROCESO SI
drop table if exists ${ESQUEMA_TEMP}.otc_t_360_bonos_fidelizacion_row_temp;
create table ${ESQUEMA_TEMP}.otc_t_360_bonos_fidelizacion_row_temp as
select telefono,tipo,codigo_slo,mb,fecha
,row_number() over (partition by telefono,tipo order by mb,codigo_slo) as orden
from db_rdb.otc_t_bonos_fidelizacion a
inner join (select max(fecha) fecha_max from db_rdb.otc_t_bonos_fidelizacion where fecha < ${fechamas1}) b on b.fecha_max=a.fecha;

--NO, POR EL ANTERIOR
drop table if exists ${ESQUEMA_TEMP}.otc_t_360_bonos_fid_trans_megas_temp;
create table ${ESQUEMA_TEMP}.otc_t_360_bonos_fid_trans_megas_temp as
select telefono
,max(case when orden = 1 then concat(codigo_slo,'-',mb) else 'NO' end) M01
,max(case when orden = 2 then concat(codigo_slo,'-',mb) else 'NO' end) M02
,max(case when orden = 3 then concat(codigo_slo,'-',mb) else 'NO' end) M03
,max(case when orden = 4 then concat(codigo_slo,'-',mb) else 'NO' end) M04
from ${ESQUEMA_TEMP}.otc_t_360_bonos_fidelizacion_row_temp
where tipo='BONO_MEGAS'
group by telefono;

--NO, POR EL ANTERIOR
drop table if exists ${ESQUEMA_TEMP}.otc_t_360_bonos_fid_trans_megas_colum_temp;
create table ${ESQUEMA_TEMP}.otc_t_360_bonos_fid_trans_megas_colum_temp as
select telefono
,case when m01 <>'NO' 
	then case when m02 <>'NO' 
		  then case when m03 <>'NO' 
				then case when m04 <>'NO' 
					  then concat(m01,'|',m02,'|',m03,'|',m04)
					  else concat(m01,'|',m02,'|',m03)
					 end
			   else concat(m01,'|',m02)
			   end
		  else m01
		 end
	else ''
 end fide_megas
 from ${ESQUEMA_TEMP}.otc_t_360_bonos_fid_trans_megas_temp;

--NO, POR LO ANTERIOR
drop table if exists ${ESQUEMA_TEMP}.otc_t_360_bonos_fid_trans_dumy_temp ;
create table ${ESQUEMA_TEMP}.otc_t_360_bonos_fid_trans_dumy_temp as
select telefono
,max(case when orden = 1 then codigo_slo else 'NO' end) M01
,max(case when orden = 2 then codigo_slo else 'NO' end) M02
,max(case when orden = 3 then codigo_slo else 'NO' end) M03
,max(case when orden = 4 then codigo_slo else 'NO' end) M04
from ${ESQUEMA_TEMP}.otc_t_360_bonos_fidelizacion_row_temp
where tipo='BONO_DUMY'
group by telefono;

--NO, DEPENDENCIA DE CAMPOS ADICIONALES
drop table if exists ${ESQUEMA_TEMP}.otc_t_360_bonos_fid_trans_dumy_colum_temp;
create table ${ESQUEMA_TEMP}.otc_t_360_bonos_fid_trans_dumy_colum_temp as
select telefono
,case when m01 <>'NO' 
	then case when m02 <>'NO' 
		  then case when m03 <>'NO' 
				then case when m04 <>'NO' 
					  then concat(m01,'|',m02,'|',m03,'|',m04)
					  else concat(m01,'|',m02,'|',m03)
					 end
			   else concat(m01,'|',m02)
			   end
		  else m01
		 end
	else ''
 end fide_dumy
 from ${ESQUEMA_TEMP}.otc_t_360_bonos_fid_trans_dumy_temp;		

 
drop table if exists ${ESQUEMA_TEMP}.otc_t_360_general_temp_final_1;
create table ${ESQUEMA_TEMP}.otc_t_360_general_temp_final_1 as
select gen.telefono
,gen.codigo_plan
,gen.usa_app
,gen.usuario_app
,gen.usa_movistar_play
,gen.usuario_movistar_play
,gen.fecha_alta
,gen.sexo
,gen.edad
,gen.mes
,gen.anio
,gen.segmento
,gen.segmento_fin  
,gen.linea_negocio
,gen.linea_negocio_homologado
,gen.forma_pago_factura
,gen.forma_pago_alta
,gen.fecha_inicio_pago_actual
,gen.fecha_fin_pago_actual
,gen.fecha_inicio_pago_anterior
,gen.fecha_fin_pago_anterior
,gen.forma_pago_anterior
,gen.estado_abonado
,gen.sub_segmento
,gen.numero_abonado
,gen.account_num
,gen.identificacion_cliente
,gen.customer_ref
,gen.tac
,gen.TIENE_BONO
,gen.valor_bono
,gen.codigo_bono
,gen.probabilidad_churn
,gen.COUNTED_DAYS
,gen.categoria_plan
,gen.tarifa
,gen.nombre_plan
,gen.marca
,case --------------------------------4
 when upper(gen.linea_negocio) like 'PRE%' then
  case
	 when gen.TIENE_BONO ='SI' and upper(tra.categoria_uso) ='DATOS' then '1'
	 when gen.TIENE_BONO ='SI' and upper(tra.categoria_uso) ='MINUTOS' then '2'
	 when gen.TIENE_BONO ='NO' and upper(tra.categoria_uso) ='DATOS' then '3'
	 when gen.TIENE_BONO ='NO' and upper(tra.categoria_uso) ='MINUTOS' then '4'
	 else ''
	 end
  else ''
  end grupo_prepago
,nse.nse as nse
,fm.fide_megas fidelizacion_megas
,fd.fide_dumy fidelizacion_dumy
,case when nb.telefono is null then '0' else '1' end bancarizado
,nvl(tk.ticket,0) as ticket_recarga
,nvl(comb.codigo_bono,'') as bono_combero
,case when (tx.numero_telefono is null or tx.numero_telefono='') then 'NO' else 'SI' end as tiene_score_tiaxa
,tx.score1 as score_1_tiaxa
,tx.score2 as score_2_tiaxa
,tx.limite_credito
,gen.tipo_doc_cliente
,gen.cliente
,gen.ciclo_fact
,gen.correo_cliente_pr as email
,gen.telefono_cliente_pr as telefono_contacto
,ca.fecha_renovacion as fecha_ultima_renovacion
,ca.ADDRESS_2,ca.ADDRESS_3,ca.ADDRESS_4
,ca.FECHA_FIN_CONTRATO_DEFINITIVO
,ca.VIGENCIA_CONTRATO
,ca.VERSION_PLAN
,ca.FECHA_ULTIMA_RENOVACION_JN
,ca.FECHA_ULTIMO_CAMBIO_PLAN
,gen.tipo_movimiento_mes
,gen.fecha_movimiento_mes
,gen.es_parque
,gen.banco				
,gen.fecha_proceso
from ${ESQUEMA_TEMP}.otc_t_360_general_temp gen
left outer join ${ESQUEMA_TEMP}.otc_t_360_hog_nse_tmp_cal nse on nse.telefono = gen.telefono					
left outer join db_reportes.otc_t_360_trafico tra on tra.telefono = gen.telefono and tra.fecha_proceso = gen.fecha_proceso
left join ${ESQUEMA_TEMP}.otc_t_360_bonos_fid_trans_megas_colum_temp fm on fm.telefono=gen.telefono
left join ${ESQUEMA_TEMP}.otc_t_360_bonos_fid_trans_dumy_colum_temp fd on fd.telefono=gen.telefono
left join db_temporales.otc_t_360_num_bancos_tmp nb on nb.telefono=gen.telefono
left join ${ESQUEMA_TEMP}.otc_t_360_ticket_fin_tmp tk on tk.telefono=gen.telefono
left join ${ESQUEMA_TEMP}.otc_t_360_combero_tmp comb on comb.numero_telefono=gen.telefono
left join db_temporales.otc_t_scoring_tiaxa_tmp tx on tx.numero_telefono=gen.telefono
left join db_temporales.tmp_360_campos_adicionales ca on gen.telefono=ca.telefono
group by gen.telefono
,gen.codigo_plan
,gen.usa_app
,gen.usuario_app
,gen.usa_movistar_play
,gen.usuario_movistar_play
,gen.fecha_alta
,gen.sexo
,gen.edad
,gen.mes
,gen.anio
,gen.segmento
,gen.linea_negocio
,gen.linea_negocio_homologado
,gen.forma_pago_factura
,gen.forma_pago_alta
,gen.fecha_inicio_pago_actual
,gen.fecha_fin_pago_actual
,gen.fecha_inicio_pago_anterior
,gen.fecha_fin_pago_anterior
,gen.forma_pago_anterior
,gen.estado_abonado
,gen.sub_segmento
,gen.segmento_fin 
,gen.numero_abonado
,gen.account_num
,gen.identificacion_cliente
,gen.customer_ref
,gen.tac
,gen.TIENE_BONO
,gen.valor_bono
,gen.codigo_bono
,gen.probabilidad_churn
,gen.counted_days
,gen.categoria_plan
,gen.tarifa
,gen.nombre_plan
,gen.marca
,case
 when upper(gen.linea_negocio) like 'PRE%' then
  case
	 when gen.TIENE_BONO ='SI' and upper(tra.categoria_uso) ='DATOS' then '1'
	 when gen.TIENE_BONO ='SI' and upper(tra.categoria_uso) ='MINUTOS' then '2'
	 when gen.TIENE_BONO ='NO' and upper(tra.categoria_uso) ='DATOS' then '3'
	 when gen.TIENE_BONO ='NO' and upper(tra.categoria_uso) ='MINUTOS' then '4'
	 else ''
	 end
  else ''
  end
,nse.nse
,fm.fide_megas
,fd.fide_dumy
,case when nb.telefono is null then '0' else '1' end
,nvl(tk.ticket,0)
,comb.codigo_bono
,case when (tx.numero_telefono is null or tx.numero_telefono='') then 'NO' else 'SI' end
,tx.score1
,tx.score2
,tx.limite_credito
,gen.tipo_doc_cliente
,gen.cliente
,gen.ciclo_fact
,gen.correo_cliente_pr
,gen.telefono_cliente_pr
,ca.fecha_renovacion
,ca.ADDRESS_2,ca.ADDRESS_3,ca.ADDRESS_4
,ca.FECHA_FIN_CONTRATO_DEFINITIVO
,ca.VIGENCIA_CONTRATO
,ca.VERSION_PLAN
,ca.FECHA_ULTIMA_RENOVACION_JN
,ca.FECHA_ULTIMO_CAMBIO_PLAN
,gen.tipo_movimiento_mes
,gen.fecha_movimiento_mes
,gen.es_parque
,gen.banco
,gen.fecha_proceso;

--NO, UNION DE LO ANTERIOR					  
drop table if exists ${ESQUEMA_TEMP}.otc_t_360_general_temp_final_2;
create table ${ESQUEMA_TEMP}.otc_t_360_general_temp_final_2	as 	  
select 
a.telefono
,a.codigo_plan
,a.usa_app
,a.usuario_app
,a.usa_movistar_play
,a.usuario_movistar_play
,a.fecha_alta
,a.sexo
,a.edad
,a.mes
,a.anio
,a.segmento
,a.segmento_fin
,a.linea_negocio
,a.linea_negocio_homologado
,a.forma_pago_factura
,a.forma_pago_alta
,a.fecha_inicio_pago_actual
,a.fecha_fin_pago_actual
,a.fecha_inicio_pago_anterior
,a.fecha_fin_pago_anterior
,a.forma_pago_anterior
,a.estado_abonado
,a.sub_segmento
,a.numero_abonado
,a.account_num
,a.identificacion_cliente
,a.customer_ref
,a.tac
,a.tiene_bono
,a.valor_bono
,a.codigo_bono
,a.probabilidad_churn
,case 
when a.linea_negocio_homologado = 'PREPAGO' and (coalesce(b.ingreso_recargas_m0,0)+coalesce(b.ingreso_combos,0)+coalesce(b.ingreso_bonos,0)) >0 and a.counted_days>30 
then 0 else a.counted_days end as counted_days
,a.categoria_plan
,a.tarifa
,a.nombre_plan
,a.marca
,a.grupo_prepago
,a.nse
,a.fidelizacion_megas
,a.fidelizacion_dumy
,a.bancarizado
,a.ticket_recarga
,a.bono_combero
,a.tiene_score_tiaxa
,a.score_1_tiaxa
,a.score_2_tiaxa
,a.limite_credito
,a.tipo_doc_cliente
,a.cliente
,a.ciclo_fact
,a.email
,a.telefono_contacto
,a.fecha_ultima_renovacion
,a.address_2
,a.address_3
,a.address_4
,a.fecha_fin_contrato_definitivo
,a.vigencia_contrato
,a.version_plan
,a.fecha_ultima_renovacion_jn
,a.fecha_ultimo_cambio_plan
,a.tipo_movimiento_mes
,a.fecha_movimiento_mes
,a.es_parque
,a.banco 
,case 
when a.linea_negocio_homologado = 'PREPAGO' and (coalesce(b.ingreso_recargas_m0,0)+coalesce(b.ingreso_combos,0)+coalesce(b.ingreso_bonos,0)) >0 then 'SI' 
when a.linea_negocio_homologado = 'PREPAGO' and (coalesce(b.ingreso_recargas_m0,0)+coalesce(b.ingreso_combos,0)+coalesce(b.ingreso_bonos,0)) =0 then 'NO' 
else 'NA' end as PARQUE_RECARGADOR 
,c.motivo_suspension as susp_cobranza
,d.susp_911
,d.susp_cobranza_puntual
,d.susp_fraude
,d.susp_robo
,d.susp_voluntaria
,e.vencimiento as vencimiento_cartera
,e.ddias_total as saldo_cartera
,a.fecha_proceso
from ${ESQUEMA_TEMP}.otc_t_360_general_temp_final_1 a 
left join db_temporales.tmp_otc_t_360_recargas b
on a.telefono=b.numero_telefono
left join db_temporales.otc_t_360_susp_cobranza c
on a.telefono=c.name and a.estado_abonado='SAA'
left join db_temporales.tmp_360_otras_suspensiones d
on a.telefono=d.name and a.estado_abonado='SAA'
left join db_temporales.otc_t_360_cartera_vencimiento e
on a.account_num=e.cuenta_facturacion;

--NO, DEPENDENCIA DE LO ANTERIOR
drop table if exists ${ESQUEMA_TEMP}.otc_t_360_general_temp_final;
create table ${ESQUEMA_TEMP}.otc_t_360_general_temp_final as
select * from (select *,
row_number() over (partition by es_parque, telefono order by fecha_alta desc) as orden
from 
${ESQUEMA_TEMP}.otc_t_360_general_temp_final_2) as t1
where orden=1;
 
