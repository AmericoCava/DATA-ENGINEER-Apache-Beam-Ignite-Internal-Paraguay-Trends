def record_antecedentes(row):
    record = {}
    record['pers_id'] = row[0].split(',')[0]
    attrs_morosidades = sum(row[1]['attrs_morosidades'])
    attrs_morosidades_c30 = sum(row[1]['attrs_morosidades_c30'])
    attrs_demandas = sum(row[1]['attrs_demandas'])
    attrs_convocatorias = sum(row[1]['attrs_convocatorias'])
    attrs_quiebras = sum(row[1]['attrs_quiebras'])
    attrs_remates = sum(row[1]['attrs_remates'])
    attrs_inhibiciones = sum(row[1]['attrs_inhibiciones'])
    attrs_inhabilitaciones = sum(row[1]['attrs_inhabilitaciones'])
    record['attrs_morosidades'] = attrs_morosidades
    record['attrs_morosidades_c30'] = attrs_morosidades_c30
    record['attrs_demandas'] = attrs_demandas
    record['attrs_convocatorias'] = attrs_convocatorias                        
    record['attrs_quiebras'] = attrs_quiebras
    record['attrs_remates'] = attrs_remates
    record['attrs_inhibiciones'] = attrs_inhibiciones
    record['attrs_inhabilitaciones'] = attrs_inhabilitaciones
                                 
    return record

def entrada_bp(row):
    record = {}
    record['id'] = row[0].split(',')[0]
    record['cedula_identidad'] = row[0].split(',')[1]
    record['primer_nombre'] = row[0].split(',')[2]
    record['segundo_nombre'] = row[0].split(',')[3]
    record['primer_apellido'] = row[0].split(',')[4]
    record['segundo_apellido'] = row[0].split(',')[5]
    
    return record