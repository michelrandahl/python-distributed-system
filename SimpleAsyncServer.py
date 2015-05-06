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
from toolz import curry


class SimpleServer:
    def __init__(self, info: ServerInfo, servers: List[ServerInfo],
                 loop, data: Dict[str, str]):
        self.servers = servers
        self.servers.append(info)
        self.loop = loop
        self.info = info
        self.data = data
        self.server = pipe(
            asyncio.start_server(self.handle_msg,
                                 info.ip,
                                 info.port,
                                 loop=loop),
            loop.run_until_complete)

        # the async function enqueues a coroutine to be run in the event loop,
        # and continues current flow immediatly without waiting
        asyncio.async(self.sync_data())

    # functions marked as coroutine can be scheduled for run in the event loop
    @asyncio.coroutine
    def sync_data(self):
        pipe(self.servers,
             filter(lambda s: s != self.info),
             map(self.send_data_to),
             list  # calling list will force the lazy sequence to be evaluated
             )

    def send_data_to(self, recipient):
        pipe(
            DataMessage(self.data, self.info),
            curry(self.send_message_to, recipient),
            asyncio.async
        )

    @asyncio.coroutine
    def send_message_to(self, recipient: ServerInfo, msg: Message):
        # asyncio.sleep syspends the function and allows the event loop to continue processing on the next scheduled
        # coroutine in the queue, until this one finishes its sleep
        yield from asyncio.sleep(3)
        reader, writer = yield from asyncio.open_connection(recipient.ip,
                                                            recipient.port,
                                                            loop=loop)
        pipe(msg,
             pickle.dumps,
             writer.write)
        writer.close()

    @asyncio.coroutine
    def sync_servers_list(self):
        pipe(self.servers,
             filter(lambda s: s != self.info),
             map(self.send_servers_list_to),
             list  # calling list will force the lazy sequence to be evaluated
             )

    def send_servers_list_to(self, recipient):
        pipe(
            ServerListMessage(self.servers, self.info),
            curry(self.send_message_to, recipient),
            asyncio.async
        )

    @asyncio.coroutine
    def handle_msg(self, reader, writer):
        # reading the received bytes
        msg = yield from reader.read()

        # decodes the object from bytes
        data = pickle.loads(msg)

        # if we see a new server then we update out own list and send the updated list to all other servers
        if isinstance(data, Message):
            if not(data.sender in self.servers):
                self.servers.append(data.sender)
                asyncio.async(self.sync_servers_list())

        # match message type and perform appropiate actions
        if isinstance(data, Inform):
            print(data.info)
        elif isinstance(data, ServerListMessage):
            print('%s got new serverlist %d' % (self.info, len(self.servers)))
            self.servers = list(set(data.servers) | set(self.servers))  # union of the two server lists
        elif isinstance(data, DataMessage):
            # the difference between received dataset and local
            new_data_keys = set(data.data.keys()) - set(self.data.keys())
            new_data_keys_other = set(self.data.keys()) - set(data.data.keys())

            for k in new_data_keys:
                self.data[k] = data.data[k]

            # if the other guys dataset is missing some of our data, then we schedule a sync
            if len(new_data_keys_other) > 0:
                self.send_data_to(data.sender)

            print('synced %s, now has data: %s' % (self.info, self.data))
        else:
            print('unkown message type!')
            print(data)

    def kill(self):
        self.server.close()
        self.loop.run_until_complete(self.server.wait_closed())


if __name__ == "__main__":
    # the backbone of async is its event loop which is responsible for running the tasks you give it
    loop = asyncio.get_event_loop()

    # information for three servers
    servers = [
        ServerInfo('127.0.0.1', 5001),
        ServerInfo('127.0.0.1', 5002),
        ServerInfo('127.0.0.1', 5003),
        ServerInfo('127.0.0.1', 5004),
        ServerInfo('127.0.0.1', 5005),
    ]

    # the servers only know themselves and the main server from the beginning
    started_servers = [
        SimpleServer(servers[0], [], loop, {'dk': 'goddag'}),
        SimpleServer(servers[1], [servers[0]], loop, {'en': 'hello'}),
        SimpleServer(servers[2], [servers[0]], loop, {'fr': 'bonjour'}),
        SimpleServer(servers[3], [servers[0]], loop, {'jp': 'konnichiwa'}),
        SimpleServer(servers[4], [servers[0]], loop, {'ch': 'ni hao'}),
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
