import apache_beam as beam
import configure.configure as C
import query.query as Q
import query.schema as SC
import transformation.array as A
import transformation.transformation as T
import transformation.link as L
import transformation.record as R
import transformation.extraction as E
import util.util as U
import util.join as J
        
class sd_py_coomecipar_225_db(beam.PTransform):
    
    def __init__(self, pipeline):
        self.p = pipeline
    
    def expand(self, pcolls):
        
        table = SC.table_spec
        
        pcol_info_negativa = (self.p
        | "read_table_info_negativa" >> beam.io.ReadFromBigQuery(query=Q.sql_info_negativa(), use_standard_sql=True, temp_dataset=C.dataset_reference))
        
        pcol_attrs_morosidades = (pcol_info_negativa
        | "extract_attrs_morosidades" >> beam.ParDo(A.extraccion_attrs_morosidades())
        | "filter_attrs_morosidades" >> beam.Filter(lambda element: element['pers_tipo'] == C.pers_tipo
                                                    and element['tipo_doc_id'] == C.tipo_doc_id)
        | "transformation_attrs_morosidades" >> beam.Map(L.modulos_conca)
        | "Sum_attrs_morosidades" >> beam.CombinePerKey(sum))
        
        pcol_attrs_morosidades_c30 = (pcol_info_negativa
        | "extract_attrs_morosidades_c30" >> beam.ParDo(A.extraccion_attrs_morosidades_c30())
        | "filter_attrs_morosidades_c30" >> beam.Filter(lambda element: element['pers_tipo'] == C.pers_tipo
                                                        and element['tipo_doc_id'] == C.tipo_doc_id)
        | "transformation_attrs_morosidades_c30" >> beam.Map(L.modulos_conca)
        | "Sum_attrs_morosidades_c30" >> beam.CombinePerKey(sum))
        
        pcol_attrs_demandas = (pcol_info_negativa
        | "extract_attrs_demandas" >> beam.ParDo(A.extraccion_attrs_demandas())
        | "filter_attrs_demandas" >> beam.Filter(lambda element: element['pers_tipo'] == C.pers_tipo
                                                 and element['tipo_doc_id'] == C.tipo_doc_id
                                                 and element['fecha_fniquito'] == C.fecha_fniquito
                                                 and element['fecha_ai'] == C.fecha_ai)
        | "transformation_attrs_demandas" >> beam.Map(L.modulos_conca)
        | "Sum_attrs_demandas" >> beam.CombinePerKey(sum))
        
        pcol_attrs_convocatorias = (pcol_info_negativa
        | "extract_attrs_convocatorias" >> beam.ParDo(A.extraccion_attrs_convocatorias())
        | "filter_attrs_convocatorias" >> beam.Filter(lambda element: element['pers_tipo'] == C.pers_tipo
                                                      and element['tipo_doc_id'] == C.tipo_doc_id
                                                      and element['fecha_sd'] == C.fecha_sd)
        | "transformation_attrs_convocatorias" >> beam.Map(L.modulos_conca)
        | "Sum_attrs_convocatorias" >> beam.CombinePerKey(sum))
        
        pcol_attrs_quiebras = (pcol_info_negativa
        | "extract_attrs_quiebras" >> beam.ParDo(A.extraccion_attrs_quiebras())
        | "filter_attrs_quiebras" >> beam.Filter(lambda element: element['pers_tipo'] == C.pers_tipo
                                                 and element['tipo_doc_id'] == C.tipo_doc_id
                                                 and element['fecha_sd'] == C.fecha_sd)
        | "transformation_attrs_quiebras" >> beam.Map(L.modulos_conca)
        | "Sum_attrs_quiebras" >> beam.CombinePerKey(sum))
        
        pcol_attrs_remates = (pcol_info_negativa
        | "extract_attrs_remates" >> beam.ParDo(A.extraccion_attrs_remates())
        | "filter_attrs_remates" >> beam.Filter(lambda element: element['pers_tipo'] == C.pers_tipo
                                                and element['tipo_doc_id'] == C.tipo_doc_id
                                                and element['fecha_finiquito'] == C.fecha_fniquito)
        | "transformation_attrs_remates" >> beam.Map(L.modulos_conca)
        | "Sum_attrs_remates" >> beam.CombinePerKey(sum))
        
        pcol_attrs_inhibiciones = (pcol_info_negativa
        | "extract_attrs_inhibiciones" >> beam.ParDo(A.extraccion_attrs_inhibiciones())
        | "filter_attrs_inhibiciones" >> beam.Filter(lambda element: element['pers_tipo'] == C.pers_tipo
                                                     and element['tipo_doc_id'] == C.tipo_doc_id
                                                     and element['fecha_ai'] == C.fecha_ai
                                                     and element['fecha_fniquito'] == C.fecha_fniquito)
        | "transformation_attrs_inhibiciones" >> beam.Map(L.modulos_conca)
        | "Sum_attrs_inhibiciones" >> beam.CombinePerKey(sum))
        
        pcol_attrs_inhabilitaciones = (pcol_info_negativa
        | "extract_attrs_inhabilitaciones" >> beam.ParDo(A.extraccion_attrs_inhabilitaciones())
        | "filter_attrs_inhabilitaciones" >> beam.Filter(lambda element: element['pers_tipo'] == C.pers_tipo
                                                         and element['tipo_doc_id'] == C.tipo_doc_id)
        | "transformation_attrs_inhabilitaciones" >> beam.Map(L.modulos_conca)
        | "Sum_attrs_inhabilitaciones" >> beam.CombinePerKey(sum))
        
        antecedentes = ( 
        {'attrs_morosidades': pcol_attrs_morosidades,
         'attrs_morosidades_c30': pcol_attrs_morosidades_c30,
         'attrs_demandas': pcol_attrs_demandas,
         'attrs_convocatorias': pcol_attrs_convocatorias,
         'attrs_quiebras': pcol_attrs_quiebras,
         'attrs_remates': pcol_attrs_remates,
         'attrs_inhibiciones': pcol_attrs_inhibiciones,
         'attrs_inhabilitaciones': pcol_attrs_inhabilitaciones}
        | "antecedentes_union" >> beam.CoGroupByKey()
        | "record_antecedentes" >> beam.Map(R.record_antecedentes)
        | "key_value_map_antecedentes" >> beam.Map(U.map_to_pair,'pers_id')) 
        
        attrs_bp = (self.p
        | "read_table_info_positiva" >> beam.io.ReadFromBigQuery(query=Q.sql_info_positiva(), use_standard_sql=True, temp_dataset=C.dataset_reference)
        | "none_attrs_bp_saldo_total_entidad" >> beam.Map(T.none_attrs_bp_saldo_total_entidad)
        | "none_attrs_bp_compromiso_mes_entidad" >> beam.Map(T.none_attrs_bp_compromiso_mes_entidad)
        | "key_value_map_attrs_bp" >> beam.Map(U.map_to_pair,'pers_id'))
        
        faja_atr = (self.p
        | "read_table_faja_atr" >> beam.io.ReadFromBigQuery(query=Q.sql_faja_atr(), use_standard_sql=True, temp_dataset=C.dataset_reference)       
        | "key_value_map_faja_atr" >> beam.Map(U.map_to_pair,'pers_id'))
        
        info_negativa = (pcol_info_negativa
        | "extraction_info_negativa" >> beam.ParDo(E.info_negativa())
        | "key_value_map_info_negativa" >> beam.Map(U.map_to_pair,'pers_id'))
        
        info_negativa_join_attrs_bp = ({'info_negativa': info_negativa, 'attrs_bp': attrs_bp} 
        | "Join_1" >> J.LeftJoin('info_negativa', 'attrs_bp', 'pers_id')
        | "append_attrs_bp" >> beam.Map(T.append_bp_data)
        | "join_1_key_value_map" >> beam.Map(U.map_to_pair,'pers_id'))
        
        info_negativa_join_antecedentes = ({'info_negativa_join_attrs_bp': info_negativa_join_attrs_bp, 'antecedentes': antecedentes} 
        | "Join_2" >> J.LeftJoin('info_negativa_join_attrs_bp', 'antecedentes', 'pers_id')
        | "append_antecedentes" >> beam.Map(T.append_antecedentes)
        | "join_2_key_value_map" >> beam.Map(U.map_to_pair,'pers_id'))
        
        info_negativa_join_faja = ({'info_negativa_join_antecedentes': info_negativa_join_antecedentes, 'faja_atr': faja_atr} 
        | "Join_3" >> J.LeftJoin('info_negativa_join_antecedentes', 'faja_atr', 'pers_id')
        | "append_fajas" >> beam.Map(T.append_fajas)
        | "transformation_dias_atraso" >> beam.Map(T.dias_atraso)
        | "transformation_positivo_negativo" >> beam.Map(T.positivo_negativo)
        | "extraction_final_join" >> beam.ParDo(E.final_join())
        | "join_3_key_value_map" >> beam.Map(U.map_to_pair,'pers_id'))
        
        read_in = (pcol_info_negativa
        | "extraction_read_in" >> beam.ParDo(E.entrada_bp())
#         | "read_in" >> beam.io.ReadFromText('gs://dfa-dna-ws0020-la-prd-landing-zone/Ignite_Portfolio_trends/in/entrada_bp.csv', skip_header_lines=1) 
#         | "dic_read_in" >> beam.Map(lambda line: line.split('\t'))
#         | "record_read_in" >> beam.Map(R.entrada_bp)
        | "read_in_key_value_map" >> beam.Map(U.map_to_pair,'pers_id'))
        
        coomecipar = ({'read_in': read_in, 'info_negativa_join_faja': info_negativa_join_faja} 
        | "Join_4" >> J.LeftJoin('read_in', 'info_negativa_join_faja', 'pers_id')
        | "extraction_coomecipar" >> beam.ParDo(E.sd_py_coomecipar_225_db()))
        
        sd_py_coomecipar_225_db = (coomecipar
        | "add_time_stamp" >> beam.Map(T.time_stamp)
        | 'write_table_GCP' >> beam.io.WriteToBigQuery(
            table,
            schema=SC.table_schema,
            additional_bq_parameters={'timePartitioning': {'type': 'MONTH', 'field':'time_stamp'}}))
        
        return sd_py_coomecipar_225_db