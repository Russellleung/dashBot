from mappy import Mappy
from projects.australia.createMappings import AutraliaMapper
from projects.carsapi.createMappings import CarsApiMappy
from projects.test.createMappings import TestMappy

class MappingFactory:
    @staticmethod
    def getMappy(index_name):
        if index_name == AutraliaMapper.name:
            return AutraliaMapper()
        
        elif index_name == CarsApiMappy.name:
            return CarsApiMappy()
        
        elif index_name == TestMappy.name:
            return TestMappy()
        else:
            return Mappy()
        
        
        
        

    