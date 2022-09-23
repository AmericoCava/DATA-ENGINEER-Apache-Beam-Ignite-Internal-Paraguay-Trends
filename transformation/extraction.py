import apache_beam as beam

class info_negativa(beam.DoFn):
    
    def process(self, elements):
                yield {'pers_id': elements['pers_id'],
                       'tipo_doc_id': elements['tipo_doc_id'],
                       'positivo_negativo': None
                      }

class final_join(beam.DoFn):
    
    def process(self, elements):
                yield {'pers_id': elements['pers_id'],
                       'tipo_doc_id': elements['tipo_doc_id'],
                       'dias_atraso': elements['dias_atraso'],
                       'positivo_negativo': elements['positivo_negativo'],
                       'cant_prod': elements['cant_prod'],
                       'saldo_total_entidad': elements['saldo_total_entidad'],
                       'saldo_total': elements['saldo_total'],
                       'compromiso_mes_entidad': elements['compromiso_mes_entidad'],
                       'compromiso_mes': elements['compromiso_mes'],
                       'limite_credito': elements['limite_credito'],
                       'SC_FAJA_0': elements['SC_FAJA_0']
                      }
            
class sd_py_coomecipar_225_db(beam.DoFn):
    
    def process(self, elements):
                yield {'id': elements['pers_id'],
                       'cedula_identidad': elements['cedula_identidad'],
                       'primer_nombre': elements['primer_nombre'],
                       'segundo_nombre': elements['segundo_nombre'],
                       'primer_apellido': elements['primer_apellido'],
                       'segundo_apellido': elements['segundo_apellido'],
                       'producto_en_base_positiva': elements['cant_prod'],
                       'atrasos_30_dias': elements['dias_atraso'],
                       'saldo_total_entidad': elements['saldo_total_entidad'],
                       'saldo_de_deuda_total_en_BP': elements['saldo_total'],
                       'compromiso_mes_entidad': elements['compromiso_mes_entidad'],
                       'compromisos_en_el_mes_BP': elements['compromiso_mes'],
                       'linea_de_credito_total_en_BP': elements['limite_credito'],
                       'positivo_negativo': elements['positivo_negativo'],
                       'SC_FAJA_0': elements['SC_FAJA_0']
                      }
            
class entrada_bp(beam.DoFn):
    
    def process(self, elements):
                yield {'pers_id': elements['pers_id'],
                       'cedula_identidad': elements['ruc'],
                       'primer_nombre': elements['primer_nombre'],
                       'segundo_nombre': elements['segundo_nombre'],
                       'primer_apellido': elements['primer_apellido'],
                       'segundo_apellido': elements['segundo_apellido']
                      }