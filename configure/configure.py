from apache_beam.io.gcp.internal.clients import bigquery as dfr

##NAME WORKSPACE
project_id = 'dfa-dna-ws0020-la-prd-a1b1'
dataset_id = 'dfa_dna_ws0020_la_prd_sandbox'
dataset_landing_id = 'dfa-dna-ws0020-la-prd-landing-zone'
table_id = 'sd_py_coomecipar_225_db'

##NAME TABLES
tb_info_positiva = 'py_efx_info_positiva_monthly_hash_la_prd'
tb_info_negativa = 'py_efx_info_negativa_monthly_hash_la_prd'
tb_score_faja_atr = 'py_efx_score_faja_atr_monthly_hash_la_prd'

##FILTER DESIGN
cod_afiliado = 38

##FILTER QUERY
pers_tipo = 'P'
tipo_doc_id = 1
fecha_fniquito = None
fecha_ai = None
fecha_sd = None


#VARIOS
dbase_temp = 'dfa_dna_ws0020_ignite_portfolio_trends_beam_temp'
dataset_reference = dfr.DatasetReference(projectId=project_id, datasetId=dbase_temp)