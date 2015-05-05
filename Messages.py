import time
from typing import Dict, Any

__author__ = 'michel'


class ServerInfo:
    def __init__(self, ip: str, port: int):
        self.ip = ip
        self.port = port

    def __eq__(self, other):
        return \
            isinstance(other, ServerInfo) and \
            self.ip == other.ip and \
            self.port == other.port


class Message:
    def __init__(self, sender: ServerInfo):
        # initialize stuff common to all messages
        self.sender = sender
        self.timestamp = time.gmtime()


class Inform(Message):
    def __init__(self, info: str, sender: ServerInfo):
        super().__init__(sender)
        self.info = info


class SendData(Message):
    def __init__(self, data: Dict[Any, Any], sender: ServerInfo):
        super().__init__(sender)
        self.data = data
