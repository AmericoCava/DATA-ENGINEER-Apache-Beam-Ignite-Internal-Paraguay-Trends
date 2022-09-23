import configure.configure as C

table_spec = f'{C.project_id}:{C.dataset_id}.{C.table_id}'
    
table_schema = 'id:STRING, \
                cedula_identidad:STRING, \
                primer_nombre:STRING, \
                segundo_nombre:STRING, \
                primer_apellido:STRING, \
                segundo_apellido:STRING, \
                producto_en_base_positiva:INTEGER, \
                atrasos_30_dias:STRING, \
                saldo_total_entidad:INTEGER, \
                saldo_de_deuda_total_en_BP:INTEGER, \
                compromiso_mes_entidad:INTEGER, \
                compromisos_en_el_mes_BP:INTEGER, \
                linea_de_credito_total_en_BP:INTEGER, \
                positivo_negativo:STRING, \
                SC_FAJA_0:STRING, \
                time_stamp:TIMESTAMP'