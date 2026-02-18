from enum import Enum

class AttributeFormat(Enum):
    JSON = "json"
    DATAFRAME = "dataframe"
    BINARY = "binary"
    ANY = "any"

class StoredAttribute:
    def __init__(self, attribute: AttributeFormat):
        self.attribute = attribute
        self.name = None
