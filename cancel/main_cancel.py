import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import argparse
import sys
import os
    
#CALCULAR DATOS
def proceso(pipeline_options):

    #SE CREA EL PIPELINE DEL PROCESO
    with beam.Pipeline(options=pipeline_options) as p:
        
        os.system("gcloud dataflow jobs cancel 2022-08-31_07_47_50-1844994153930703359 --project dfa-dna-ws0020-la-prd-a1b1 --region northamerica-northeast1")
        
if __name__ == '__main__':
    
    #RECIBO ARGUMENTOS
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(sys.argv)
    pipeline_options = PipelineOptions(pipeline_args)
    
    #CALCULO MI PROCESO
    proceso(pipeline_options)