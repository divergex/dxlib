from enum import Enum

class FieldFormat(Enum):
    JSON = "json"
    SERIALIZABLE = "serial"
    DATAFRAME = "dataframe"
    BINARY = "binary"
    ANY = "any"

class StoredField:
    def __init__(self, field_format: FieldFormat, field_type: type):
        self.field_format = field_format
        self.field_type = field_type
