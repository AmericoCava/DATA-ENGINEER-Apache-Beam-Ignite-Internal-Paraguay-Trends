import configure.configure as C

tabla_info_positiva = f"{C.project_id}.{C.dataset_id}.{C.tb_info_positiva}"
tabla_info_negativa = f"{C.project_id}.{C.dataset_id}.{C.tb_info_negativa}"
tabla_faja_atr = f"{C.project_id}.{C.dataset_id}.{C.tb_score_faja_atr}"
cod_afiliado = f"{C.cod_afiliado}"

def sql_info_positiva():
        
    qry = """with dat as (
             select
             pers_id ,
             tipo_doc_id ,
             pers_tipo ,
             prestamos ,
             tarjetas_credito
             from {tabla_info_positiva}
             where pers_id is not null
             and pers_id = 'efbcbe1824fc5ccc389f938456151935aad790330403ffb4b76dc36e53f71978'
             )
             , bp_data_raw as (
             select
                 bp.pers_id                   as pers_id
                 , bp.tipo_doc_id             as tipo_doc_id
                 , d.dias_atraso              as dias_atraso
                 , 0                          as pago_minimo_arbitrado
                 , 0                          as limite_credito_arbitrado
                 , d.saldo_total_arbitrado    as saldo_total_arbitrado
                 , d.compromiso_mes_arbitrado as compromiso_mes_arbitrado
                 , case when p.afiliado.afiliado  = {cod_afiliado} then 1 else 0 end es_afiliado_obs
             from
                 dat bp
                 , bp.prestamos                   p
                 , p.detalle                      d
             where
                 bp.pers_tipo                            = 'F'
                 and bp.pers_id                          is not null
                 and coalesce(bp.tipo_doc_id     , -1)   = 1
                 and coalesce(d.operacion_cerrada, '')   != 'S'
                 and d.periodo = (
                 select max(h.periodo)
                 from dat bp, bp.prestamos d,
                 unnest (d.detalle) as h)
             union all
             select
                 bp.pers_id                      as pers_id
                 , bp.tipo_doc_id                as tipo_doc_id
                 , d.dias_atraso                 as dias_atraso
                 , d.pago_minimo_arbitrado       as pago_minimo_arbitrado
                 , tc.limite_credito_arbitrado   as limite_credito_arbitrado
                 , d.saldo_total_arbitrado       as saldo_total_arbitrado
                 , 0                             as compromiso_mes_arbitrado
                 , case when tc.afiliado.afiliado  = {cod_afiliado} then 1 else 0 end es_afiliado_obs
             from
                 dat bp
                 , bp.tarjetas_credito            tc
                 , tc.detalle                     d
             where
                 bp.pers_tipo                            = 'F'
                 and bp.pers_id                          is not null
                 and coalesce(bp.tipo_doc_id     , -1)   = 1
                 and coalesce(d.operacion_cerrada, '')   != 'S'
                 and d.periodo = (
                 select max(h.periodo)
                 from dat bp, bp.tarjetas_credito d,
                 unnest (d.detalle) as h)
             )
             , attrs_bp as (
             select
                 bp.pers_id       as pers_id
                 , bp.tipo_doc_id as tipo_doc_id
                 
                 , count(1)                          as cant_prod
                 , max(bp.dias_atraso)               as dias_atraso
                 , sum(case when es_afiliado_obs = 1 
                            then bp.saldo_total_arbitrado 
                            end
                       ) saldo_total_entidad
                       
                 , sum(bp.saldo_total_arbitrado) as saldo_total
                 
                 , sum(case when es_afiliado_obs = 1 
                            then  (coalesce(bp.pago_minimo_arbitrado, 0) + coalesce(bp.compromiso_mes_arbitrado)) 
                            end
                       ) compromiso_mes_entidad
                 , coalesce(sum(bp.pago_minimo_arbitrado), 0) + coalesce(sum(bp.compromiso_mes_arbitrado), 0) as compromiso_mes
                 , sum(bp.limite_credito_arbitrado)  as limite_credito
             from
                 bp_data_raw bp
             group by
                 bp.pers_id
                 , bp.tipo_doc_id
             )
             SELECT * FROM attrs_bp
          """.format(tabla_info_positiva = tabla_info_positiva, cod_afiliado = cod_afiliado)

    return qry

def sql_info_negativa():
    
    qry = """select
             pers_id ,
             tipo_doc_id ,
             pers_tipo ,
             ruc ,
             primer_nombre ,
             segundo_nombre ,
             primer_apellido ,
             segundo_apellido ,
             MOROSIDADES ,
             MOROSIDADES_CE ,
             DEMANDAS ,
             CONVOCATORIAS ,
             QUIEBRAS ,
             REMATES ,
             INHIBICIONES ,
             INHABILITACIONES
             from {tabla_info_negativa}
             where pers_id ='efbcbe1824fc5ccc389f938456151935aad790330403ffb4b76dc36e53f71978'
             and pers_id is not null
          """.format(tabla_info_negativa = tabla_info_negativa)
    
    return qry

def sql_faja_atr():
    
    qry = """select
             pers_id ,
             faja_f1 as SC_FAJA_0
             from {tabla_faja_atr}
             where pers_id ='efbcbe1824fc5ccc389f938456151935aad790330403ffb4b76dc36e53f71978'
             and pers_id is not null
          """.format(tabla_faja_atr = tabla_faja_atr)
    
    return qry