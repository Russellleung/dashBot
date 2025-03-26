from mappy import Mappy
from projects.australia.createMappings import AutraliaMapper

class MappingFactory:
    @staticmethod
    def getMappy(shape_type):
        if shape_type == AutraliaMapper.name:
            return AutraliaMapper()
        else:
            return Mappy()
        
        
        
        

    