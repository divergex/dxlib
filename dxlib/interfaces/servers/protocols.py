from enum import Enum


class Protocols(Enum):
    HTTP = "http"
    RPC = "rpc"
    PRODUCER = "producer"
    CONSUMER = "consumer"
    DATABASE = "database"
