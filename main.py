import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import argparse
import sys
import modules.sd_py_coomecipar_225_db as DB
import configure.configure as C
import query.query as Q
import transformation.array as A
from datetime import datetime

class MisOpciones(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--fecha', help='Fecha de ejecucion', default=None, required=False)

def entrada(fecha):
    if fecha is None:
        return str(datetime.today().strftime('%Y-%m-%d'))
    return fecha
    
def proceso(pipeline_options, fecha):

    with beam.Pipeline(options=pipeline_options) as p:
        
        final = (p
        | 'read_final' >> DB.sd_py_coomecipar_225_db(p))

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(sys.argv)
    pipeline_options = PipelineOptions(pipeline_args)
    
    args = MisOpciones()
    fecha = entrada(args.fecha)
    proceso(pipeline_options, fecha)