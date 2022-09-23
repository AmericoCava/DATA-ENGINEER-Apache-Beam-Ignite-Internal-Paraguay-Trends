import apache_beam as beam

class LeftJoin(beam.PTransform):
    
    def __init__(self, left_name, right_name, key):
        self.left_name = left_name
        self.right_name = right_name
        self.key = key

    def expand(self, pcolls):
        return ({name: data for (name, data) in pcolls.items()}
                | "Agrupar {0} con {1} por llave {2}".format(self.left_name, self.right_name, self.key) >> beam.CoGroupByKey()
                | "Actualizar record {0} con {1} por {2}".format(self.left_name, self.right_name, self.key) >> beam.ParDo(UpdateLeftRecord(), self.left_name, self.right_name)
               )
    
class UpdateLeftRecord(beam.DoFn):

    def process(self, element, left_name, right_name):
        key, info = element
        right = info[right_name]
        left = info[left_name]
        for item_left in left:
            if len(right) > 0:
                item_left.update(right[0])
                yield item_left
            else:
                yield item_left
                
####################### INNER JOIN
class InnerJoin(beam.PTransform):
    
    def __init__(self, left_name, right_name, key):
        self.left_name = left_name
        self.right_name = right_name
        self.key = key

    def expand(self, pcolls):
        return ({name: data for (name, data) in pcolls.items()}
                | "Agrupar {0} con {1} por llave {2}".format(self.left_name, self.right_name, self.key) >> beam.CoGroupByKey()
                | "Actualizar record {0} con {1} por {2}".format(self.left_name, self.right_name, self.key) >> beam.ParDo(UpdateMatchingRecord(), self.left_name, self.right_name)
               )
    
class UpdateMatchingRecord(beam.DoFn):
    """This DoFn class unnests the CogroupBykey output and emits """

    def process(self, element, left_name, right_name):
        key, info = element
        right = info[right_name]
        left = info[left_name]
        for item_left in left:
            for item_right in right: 
                item_left.update(item_right)
                yield item_left       
            
####################### LEFT ANTI JOIN
class LeftAntiJoin(beam.PTransform):
    
    def __init__(self, left_name, right_name, key):
        self.left_name = left_name
        self.right_name = right_name
        self.key = key

    def expand(self, pcolls):
        return ({name: data for (name, data) in pcolls.items()}
                | "Agrupar {0} con {1} por llave {2}".format(self.left_name, self.right_name, self.key) >> beam.CoGroupByKey()
                | "Actualizar record {0} con {1} por {2}".format(self.left_name, self.right_name, self.key) >> beam.ParDo(UpdateNotMatchingRecord(), self.left_name, self.right_name)
               )
    
class UpdateNotMatchingRecord(beam.DoFn):
    """This DoFn class unnests the CogroupBykey output and emits """

    def process(self, element, left_name, right_name):
        key, info = element
        right = info[right_name]
        left = info[left_name]
        for item_left in left:
            if len(right) == 0:
                yield item_left