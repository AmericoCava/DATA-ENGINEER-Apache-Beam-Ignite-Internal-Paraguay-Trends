import apache_beam as beam
from datetime import time, datetime

anio_mes_dia = datetime.now().strftime("%Y_%m_%d")

def map_to_pair(row, key):
    return row[key], row

def print_fn(elements):
    print(elements)
    return elements