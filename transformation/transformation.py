from datetime import time, datetime
fecha_archive = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def none_attrs_bp_saldo_total_entidad(row):
    if row['saldo_total_entidad'] == None:
        row['saldo_total_entidad'] = 0
        
    return row

def none_attrs_bp_compromiso_mes_entidad(row):
    if row['compromiso_mes_entidad'] == None:
        row['compromiso_mes_entidad'] = 0
        
    return row

def append_bp_data(row):
    if (('cant_prod' in row) == False 
    and ('dias_atraso' in row) == False 
    and ('saldo_total_entidad' in row) == False
    and ('saldo_total' in row) == False
    and ('compromiso_mes_entidad' in row) == False
    and ('compromiso_mes' in row) == False
    and ('limite_credito' in row) == False):
        row['cant_prod'] = 0
        row['dias_atraso'] = 0
        row['saldo_total_entidad'] = 0
        row['saldo_total'] = 0
        row['compromiso_mes_entidad'] = 0
        row['compromiso_mes'] = 0
        row['limite_credito'] = 0
    
    return row

def append_antecedentes(row):
    if (('attrs_morosidades' in row) == False 
    and ('attrs_morosidades_c30' in row) == False 
    and ('attrs_demandas' in row) == False
    and ('attrs_convocatorias' in row) == False
    and ('attrs_quiebras' in row) == False
    and ('attrs_remates' in row) == False
    and ('attrs_inhibiciones' in row) == False
    and ('attrs_inhabilitaciones' in row) == False):
        row['attrs_morosidades'] = 0
        row['attrs_morosidades_c30'] = 0
        row['attrs_demandas'] = 0
        row['attrs_convocatorias'] = 0
        row['attrs_quiebras'] = 0
        row['attrs_remates'] = 0
        row['attrs_inhibiciones'] = 0
        row['attrs_inhabilitaciones'] = 0
    
    return row

def append_fajas(row):
    if ('SC_FAJA_0' in row) == False :
        row['SC_FAJA_0'] = '0'
    
    return row

def dias_atraso(row):
    if row['dias_atraso'] > 30:
        row['dias_atraso'] = 'S'
    else: row['dias_atraso'] = 'N'
        
    return row
    
def positivo_negativo(row):
    if (row['attrs_morosidades'] > 0 or
    row['attrs_morosidades_c30'] > 0 or
    row['attrs_demandas'] > 0 or
    row['attrs_convocatorias'] > 0 or
    row['attrs_quiebras'] > 0 or
    row['attrs_remates'] > 0 or
    row['attrs_inhibiciones'] > 0 or
    row['attrs_inhabilitaciones'] > 0):
        row['positivo_negativo'] = 'N'
    else: row['positivo_negativo'] = 'P'
        
    return row

def time_stamp(row):
    if ('time_stamp' in row) == False:
        row['time_stamp'] = fecha_archive
        
    return row