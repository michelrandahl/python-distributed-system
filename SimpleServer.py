__author__ = 'michel'
from Messages import *

# -- python core libs -- #
# https://docs.python.org/3/library/asyncio.html
import asyncio

# http://www.diveintopython3.net/serializing.html
# decode and encode objects to and from bytes
import pickle

# -- python community libs -- #
# pip install mypy-lang
# http://mypy.readthedocs.org/en/latest/introduction.html
# library that adds optional types which helps on readability and intellisense autocompletion
from typing import Dict, List

# pip install toolz
# http://toolz.readthedocs.org/en/latest/
# library that adds common functions, primarely for list and dictionary manipulation
from toolz.curried import pipe, filter, map


class SimpleServer:
    def __init__(self, info: ServerInfo, servers: List[ServerInfo],
                 loop, data: Dict[str, str]):
        self.servers = servers
        self.loop = loop
        self.info = info
        self.data = data
        self.server = pipe(
            asyncio.start_server(self.handle_msg,
                                 info.ip,
                                 info.port,
                                 loop=loop),
            asyncio.async)

        # the async function enqueues a coroutine to be run in the event loop,
        # and continues current flow immediatly without waiting
        asyncio.async(self.sync_data())

    # functions marked as coroutine can be scheduled for run in the event loop
    @asyncio.coroutine
    def sync_data(self):
        pipe(self.servers,
             filter(lambda s: s != self.info),
             map(self.send_data_to),
             map(asyncio.async),
             list)

    @asyncio.coroutine
    def send_data_to(self, server: ServerInfo):
        # asyncio.sleep syspends the function and allows the event loop to continue processing on the next scheduled
        # coroutine in the queue, until this one finishes its sleep
        yield from asyncio.sleep(3)
        reader, writer = yield from asyncio.open_connection(server.ip,
                                                            server.port,
                                                            loop=loop)
        pipe(SendData(self.data, self.info),
             pickle.dumps,
             writer.write)
        writer.close()

    @asyncio.coroutine
    def handle_msg(self, reader, writer):
        # reading the received bytes
        msg = yield from reader.read()

        # decodes the object from bytes
        data = pickle.loads(msg)

        # match message type and perform appropiate actions
        if isinstance(data, Inform):
            print(data.info)
        elif isinstance(data, SendData):
            # the difference between received dataset and local
            new_data_keys = set(data.data.keys()) - set(self.data.keys())

            # syncing one piece at the time just to create more action
            for k in new_data_keys:
                self.data[k] = data.data[k]
                break

            # if the other guys dataset is missing some of our data, then we schedule a sync
            if len(set(self.data.keys()) - set(data.data.keys())) > 0:
                asyncio.async(self.send_data_to(data.sender))

            print('synced %r' % self.info.port)
            print(self.data)
        else:
            print('unkown message type!')
            print(data)

    def kill(self):
        self.server.close()
        self.loop.run_until_complete(self.server.wait_closed())


if __name__ == "__main__":
    # the backbone of async is its event loop which is responsible for running the tasks you give it
    loop = asyncio.get_event_loop()

    # initial language databases spread on the servers
    data1 = {'dk': 'goddag', 'en': 'hello', 'fr': 'bonjour'}
    data2 = {'jp': 'konnichiwa', 'ch': 'ni hao'}
    data3 = {'hw': 'aloha'}

    # information for three servers
    servers = [ServerInfo('127.0.0.1', 7777), ServerInfo('127.0.0.1', 8888), ServerInfo('127.0.0.1', 9999)]

    started_servers = [
        SimpleServer(servers[0], servers, loop, data1),
        SimpleServer(servers[1], servers, loop, data2),
        SimpleServer(servers[2], servers, loop, data3),
    ]

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        for s in started_servers:
            s.kill()
        loop.close()
        print('over and out')
