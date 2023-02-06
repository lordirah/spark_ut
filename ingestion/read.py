from dataclasses import dataclass
from utils import DataClassUnpack

@dataclass
class read_args:
    arg1: str
    arg2: str
    
    def validate(self):
        ret = True
        for field_name, field_def in self.__dataclass_fields__.items():
            actual_type = type(getattr(self, field_name))
            if actual_type != field_def.type:
                print(f"\t{field_name}: '{actual_type}' instead of '{field_def.type}'")
                ret = False
        return ret

    def __post_init__(self):
        if not self.validate():
            raise ValueError('Wrong types')

class ReadData:
    def __init__(self, params):
        self.params = DataClassUnpack.instantiate(read_args, params)
    def read(self):
        df = "test"
        print('Inside the ReadData')
        print(self.params.arg1)
        return df