"""
This File contains 'model/container' classes such as
messages, data files and server information
"""
__author__ = 'michel'
from functools import reduce
import time


class Data:
    def __init__(self, content: str, version_number: int):
        self.version_number = version_number
        self.content = content

    def __hash__(self) -> int:
        return reduce(lambda acc, x: acc * 17 + x.__hash__(),
                      [self.content, self.version_number],
                      71)

    def __str__(self) -> str:
        return "(data.%d): %s" % \
               (self.version_number, self.content)


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


class TESTMSG:
    pass


class Message:
    def __init__(self, sender: ServerInfo):
        self.sender = sender
        self.timestamp = time.gmtime()


class ClientDataMessage(Message):
    def __init__(self, sender, data: Data):
        super().__init__(sender)
        self.data = data


class QuorumRequest(Message):
    def __init__(self, sender: ServerInfo, data: Data, level):
        super().__init__(sender)
        self.level = level
        self.data = data


class QuorumResponse(Message):
    def __init__(self, sender: ServerInfo, accept_changes: bool, level):
        super().__init__(sender)
        self.level = level
        self.accept_changes = accept_changes


class WriteDataRequest(Message):
    def __init__(self, sender, version_number: int, level):
        super().__init__(sender)
        self.level = level
        self.version_number = version_number
