from enum import Enum


class Protocols(Enum):
    HTTP = "http"
    HTTPS = "https"
    RPC = "rpc"
    WEBSOCKET = "websocket"
    PRODUCER = "producer"
    CONSUMER = "consumer"
    DATABASE = "database"
