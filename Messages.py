from functools import reduce
import time
from typing import Dict, Any, List

__author__ = 'michel'


class ServerInfo:
    def __init__(self, ip: str, port: int):
        self.ip = ip
        self.port = port

    def __eq__(self, other) -> bool:
        return (isinstance(other, ServerInfo)
                and self.ip == other.ip
                and self.port == other.port)

    def __hash__(self) -> int:
        return reduce(lambda acc, x: acc * 17 + x.__hash__(),
                      [self.ip, self.port],
                      71)

    def __str__(self):
        return self.ip + ':' + str(self.port)


class Message:
    def __init__(self, sender: ServerInfo):
        # initialize stuff common to all messages
        self.sender = sender
        self.timestamp = time.gmtime()


class Inform(Message):
    def __init__(self, info: str, sender: ServerInfo):
        super().__init__(sender)
        self.info = info


class DataMessage(Message):
    def __init__(self, data: Dict[Any, Any], sender: ServerInfo):
        super().__init__(sender)
        self.data = data


class ServerListMessage(Message):
    def __init__(self, servers: List[ServerInfo], sender: ServerInfo):
        super().__init__(sender)
        self.servers = servers
