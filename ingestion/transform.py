from dataclasses import dataclass
from utils import DataClassUnpack

@dataclass
class transform_args:
    arg1: str
    arg2: str

class Transform:
    def __init__(self, params):
        self.params = DataClassUnpack.instantiate(transform_args, params)
    def flatten(self, df):
        print("inside transform flatten")
        print(self.params.arg1)
        return df