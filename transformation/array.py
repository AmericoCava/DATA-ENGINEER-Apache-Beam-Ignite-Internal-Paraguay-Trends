import apache_beam as beam

class extraccion_attrs_morosidades(beam.DoFn):
    
    def process(self, elements):
        
        if len(elements['MOROSIDADES']) > 0:
            for items in elements['MOROSIDADES']:
                yield {'pers_id': elements['pers_id'],
                       'tipo_doc_id': elements['tipo_doc_id'],
                       'pers_tipo': elements['pers_tipo']
                      }
                
class extraccion_attrs_morosidades_c30(beam.DoFn):
    
    def process(self, elements):
        
        if len(elements['MOROSIDADES_CE']) > 0:
            for items in elements['MOROSIDADES_CE']:
                yield {'pers_id': elements['pers_id'],
                       'tipo_doc_id': elements['tipo_doc_id'],
                       'pers_tipo': elements['pers_tipo']
                      }
class extraccion_attrs_demandas(beam.DoFn):
    
    def process(self, elements):
        
        if len(elements['DEMANDAS']) > 0:
            for items in elements['DEMANDAS']:
                yield {'pers_id': elements['pers_id'],
                       'tipo_doc_id': elements['tipo_doc_id'],
                       'pers_tipo': elements['pers_tipo'],
                       'fecha_fniquito': items['fecha_fniquito'],
                       'fecha_ai': items['fecha_ai']
                      }
        
class extraccion_attrs_convocatorias(beam.DoFn):
    
    def process(self, elements):
        
        if len(elements['CONVOCATORIAS']) > 0:
            for items in elements['CONVOCATORIAS']:
                yield {'pers_id': elements['pers_id'],
                       'tipo_doc_id': elements['tipo_doc_id'],
                       'pers_tipo': elements['pers_tipo'],
                       'fecha_sd': items['fecha_sd']
                      }
                
class extraccion_attrs_quiebras(beam.DoFn):
    
    def process(self, elements):
        
        if len(elements['QUIEBRAS']) > 0:
            for items in elements['QUIEBRAS']:
                yield {'pers_id': elements['pers_id'],
                       'tipo_doc_id': elements['tipo_doc_id'],
                       'pers_tipo': elements['pers_tipo'],
                       'fecha_sd': items['fecha_sd']
                      }
                
class extraccion_attrs_remates(beam.DoFn):
    
    def process(self, elements):
        
        if len(elements['REMATES']) > 0:
            for items in elements['REMATES']:
                yield {'pers_id': elements['pers_id'],
                       'tipo_doc_id': elements['tipo_doc_id'],
                       'pers_tipo': elements['pers_tipo'],
                       'fecha_finiquito': items['fecha_finiquito']
                      }
                
class extraccion_attrs_inhibiciones(beam.DoFn):
    
    def process(self, elements):
        
        if len(elements['INHIBICIONES']) > 0:
            for items in elements['INHIBICIONES']:
                yield {'pers_id': elements['pers_id'],
                       'tipo_doc_id': elements['tipo_doc_id'],
                       'pers_tipo': elements['pers_tipo'],
                       'fecha_fniquito': items['fecha_fniquito'],
                       'fecha_ai': items['fecha_ai']
                      }
                
class extraccion_attrs_inhabilitaciones(beam.DoFn):
    
    def process(self, elements):
        
        if len(elements['INHABILITACIONES']) > 0:
            for items in elements['INHABILITACIONES']:
                yield {'pers_id': elements['pers_id'],
                       'tipo_doc_id': elements['tipo_doc_id'],
                       'pers_tipo': elements['pers_tipo']
                      }