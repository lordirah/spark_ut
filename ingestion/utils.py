from dataclasses import fields
import sys

class DataClassUnpack:
    class_field_cache = {}
    @staticmethod
    def str_to_class(classname):
        return getattr(sys.modules[__name__], classname)

    @classmethod
    def instantiate(cls, class_to_instantiate, params):
        if class_to_instantiate not in cls.class_field_cache:
            cls.class_field_cache[class_to_instantiate] = {f.name for f in fields(class_to_instantiate) if f.init}
        field_set = cls.class_field_cache[class_to_instantiate]
        filtered_param_dict = {k : v for k, v in params.items() if k in field_set}
        return class_to_instantiate(**filtered_param_dict)