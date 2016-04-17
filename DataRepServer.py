"""
This file contains a data replication class which defines behavior for a given data replicaiton node in the system
A data replication node, which is part of a network structure.
- for example like the structure below
              ______________________________0____________________________                <- entry node (can be any node in the system)
             |                              |                            |
      _______0_________            _________9_________          ________18_________      <- virtual group leader first level
     |       |         |          |         |         |        |         |         |
   __0__   __4__     __6__      __9__     _12__     _15__    _18__     _21__     _24__   <- virtual group leader second level
  |  |  | |  |  |   |  |  |    |  |  |   |  |  |   |  |  |  |  |  |   |  |  |   |  |  |
  0  1  2 3  4  5   6  7  8    9 10 11  12 13 14  15 16 17 18 19 20  21 22 23  24 25 26  <- actual data replication nodes
"""
# TODO: Timeout, reject and read
__author__ = 'michel'
from DataRepMessages import *

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
from typing import Dict, List, Union

# pip install toolz
# http://toolz.readthedocs.org/en/latest/
# library that adds common functions, primarely for list and dictionary manipulation
from toolz.curried import pipe, filter, map
from toolz import curry, partition_all, count


class DataRepNode:
    def __init__(self, info: ServerInfo,
                 network_structure:  List[List[ServerInfo]],
                 loop, data: Data):
        super().__init__()
        self.client_request_info = None
        self.quorum_requester_info = None  # the guy who requested quorum
        self.is_top_node = False
        self.temp_data = None  # field to contain data until it can be written
        self.network_structure = network_structure
        """
        a network structure contains information about which groups the given node belongs to
        and is of the form
        [ [a,d,e], [a,b,c] ]
        where last element of the outer list is its actual bottom level group
        and the first elements of the outer list are the virtual groupings.
                    ___________________a____ ...
                   |                   |
            _______a_________         ...
           |       |         |
         __a__   __d__     __e__
        |  |  | |  |  |   |  |  |
        a  b  c .  .  .   .  .  .
        """
        self.level_quorum_count = self.create_empty_quorum_count(network_structure)
        self.loop = loop  # reference to the asyncio eventloop
        self.info = info  # address information for current node
        self.data = data  # replicated data file
        server_task = asyncio.start_server(
            self.handle_msg, info.ip, info.port, loop=loop)
        self.server = loop.run_until_complete(server_task)

    @staticmethod
    def create_empty_quorum_count(network_structure):
        return [dict() for i in range(len(network_structure))]

    def send_message_to_many(self, servers: List[ServerInfo], msg: Message, ignores: List[ServerInfo]):
        for server in filter(lambda s: s not in ignores, servers):
            asyncio.async(self.send_message_to(server, msg))

    @asyncio.coroutine
    def send_message_to(self, recipient: ServerInfo, msg: Message):
        # asyncio.sleep suspends the function and allows the event loop to continue processing on the next scheduled
        # coroutine in the queue, until this one finishes its sleep
        yield from asyncio.sleep(2)  # simulating send delay of 2 seconds
        reader, writer = \
            yield from asyncio.open_connection(
                recipient.ip, recipient.port, loop=loop)
        pipe(
            msg,
            pickle.dumps,
            writer.write
        )
        # yield from asyncio.sleep(x)  # simulate slow transfer (eg. huge file or very low bandwidth)
        writer.close()

    def save_data(self):
        print("%s saving data" % self.info)
        self.data = self.temp_data

    # The 'main' method of the server
    # Each time a message is received, this method is responsible for processing it
    @asyncio.coroutine
    def handle_msg(self, reader, writer):
        # reading the received bytes
        msg_raw = yield from reader.read()

        try:
            # decodes the object from bytes
            msg = pickle.loads(msg_raw)

            if isinstance(msg, TESTMSG):
                print("%s got TESTMSG" % self.info)

                # writing back response
                writer.write(pickle.dumps(TESTMSG))
                yield from writer.drain()
                writer.close()

            elif isinstance(msg, ClientDataMessage):
                """
                client writes to some node..
                this node is now responsible for assembling a quorum
                therefore it writes to the level 1 group leaders
                and assign itself as level 1 group leader for its own group
                             ______________________________0____________________________
                            |                              |                            |
                     _______0_________            _________9_________          ________18_________
                    |       |         |          |         |         |        |         |         |
                   ...     ...       ...        ...       ...       ...      ...       ...       ...
                """
                self.temp_data = msg.data
                self.client_request_info = msg.sender
                self.is_top_node = True

                self.send_message_to_many(
                    servers=self.network_structure[0],
                    msg=QuorumRequest(self.info, msg.data, 0),
                    ignores=[self.info])
                self.level_quorum_count = self.create_empty_quorum_count(self.network_structure)
                # the top node is also in some group at the lowest lvl
                self.level_quorum_count[-1][self.info] = True

                # send immediately to its own bottom group
                # and act as leader of that group by setting level to second highest value
                self.send_message_to_many(
                    servers=self.network_structure[-1],
                    msg=QuorumRequest(
                        sender=self.info,
                        data=msg.data,
                        level=len(self.network_structure) - 1
                    ),
                    ignores=[self.info]
                )

            elif isinstance(msg, QuorumRequest):
                """
                Quorum request received at one of the virtual layers
                If the node is in the lowest level then it should reply with a Quorum replay
                else if the node is in the second lowest level then it should count one vote for itself
                    and send quorum request to the other nodes in its group
                    and wait for at least one of them to reply.
                    When reply is received then it should send quorum response back to its parent.
                else it should send the quorum request to the responsible node in the next level group
                    and wait for enough quorum replies such that it has at least two.
                    When a consensus of at least two has been reached, it sends quorum reply to its parent.
                             ______________________________0____________________________
                            |                              |                            |
                     _______0_________            _________9_________          ________18_________      <- a node in a virtual level has received message
                    |       |         |          |         |         |        |         |         |
                   ...     ...       ...        ...       ...       ...      ...       ...       ...    <- or a node in the bottom layer has received message
               """
                print("%s got quorum request from %s (LVL: %d)" % (self.info, msg.sender, msg.level))
                current_lvl = msg.level + 1
                if current_lvl < len(self.network_structure):
                    self.quorum_requester_info = msg.sender

                    self.send_message_to_many(
                        servers=self.network_structure[current_lvl],
                        msg=QuorumRequest(
                            sender=self.info,
                            data=msg.data,
                            level=current_lvl),
                        ignores=[self.info])

                    if current_lvl == len(self.network_structure) - 1:
                        # lowest virtual lvl has been reached.
                        # we count one vote representing ourselves
                        self.level_quorum_count[current_lvl][self.info] = True
                    else:
                        # we are in one of the other virtual levels
                        # we count 0 votes so far and awaits responses
                        self.level_quorum_count[current_lvl][msg.sender] = False
                else:
                    # lowest bottom level has been reached
                    # we send quorum reply back
                    asyncio.async(
                        self.send_message_to(
                            recipient=msg.sender,
                            msg=QuorumResponse(
                                sender=self.info,
                                accept_changes=True,
                                level=current_lvl)))

            elif isinstance(msg, QuorumResponse):
                """
                Quorum response has been received at one of the virtual levels
                             ______________________________0____________________________             <- top node has received response
                            |                              |                            |
                     _______0_________            _________9_________          ________18_________   <- or a node in a virtual level has received message
                    |       |         |          |         |         |        |         |         |
                   ...     ...       ...        ...       ...       ...      ...       ...       ...
               """
                current_lvl = msg.level - 1
                if msg.sender in self.network_structure[-1]:
                    self.level_quorum_count[current_lvl][msg.sender] = True
                    if self.count_quorum(current_lvl) >= 2:
                        self.level_quorum_count[current_lvl - 1][self.info] = True
                else:
                    self.level_quorum_count[current_lvl][msg.sender] = True

                print("%s - lvl: %d, count: %d" % (self.info, current_lvl, self.count_quorum(current_lvl)))

                if self.count_quorum(current_lvl) >= 2:

                    if current_lvl == 0:
                        self.save_data()

                        self.send_message_to_many(
                            servers=self.network_structure[0],
                            msg=WriteDataRequest(
                                sender=self.info,
                                version_number=0,
                                level=0),
                            ignores=[self.info])
                    elif not self.is_top_node:

                        pipe(
                            QuorumResponse(
                                sender=self.info,
                                accept_changes=True,
                                level=current_lvl),
                            curry(self.send_message_to,
                                  self.quorum_requester_info),
                            asyncio.async
                        )

            elif isinstance(msg, WriteDataRequest):
                current_lvl = msg.level + 1
                print("%s got write request" % self.info)

                self.save_data()

                if current_lvl < len(self.level_quorum_count):
                    self.send_message_to_many(
                        servers=self.network_structure[current_lvl],
                        msg=WriteDataRequest(self.info, 0, current_lvl),
                        ignores=[self.info])

            else:
                print('%s received not supported message type!' % self.info)
                print(msg)

        except Exception as e:
            print("exception: %r" % e)

    def count_quorum(self, lvl: int) -> int:
        return count([q for q in self.level_quorum_count[lvl].values() if q])

    def kill(self):
        self.server.close()
        self.loop.run_until_complete(self.server.wait_closed())

    @asyncio.coroutine
    def send_message_to_client(self, message):
        if self.client_request_info is not None:
            print("%s sending reply to client")
            reader, writer = yield from asyncio.open_connection(
                host=self.client_request_info.ip,
                port=self.client_request_info.port,
                loop=self.loop)
            writer.write(pickle.dumps(message))
            writer.close()
        else:
            raise Exception("%s has no client info" % self.info)


if __name__ == "__main__":
    # the backbone of async is its event loop which is responsible for running the tasks you give it
    loop = asyncio.get_event_loop()

    servers = []
    for i in range(1, 10):
        servers.append(ServerInfo('127.0.0.1', 5000 + i))

    initial_data = Data(content="lorem", version_number=1)

    # [[0,3,6],[0,1,2]]
    # ________0________
    # |        |        |
    #          __0__    __3__    __6__
    #         |  |  |  |  |  |  |  |  |
    #         0  1  2  3  4  5  6  7  8

    #       [[4,9,18],[4,0,6],[4,3,5]]
    #              ______________________________0____________________________
    #             |                              |                            |
    #      _______0_________            _________9_________          ________18_________
    #     |       |         |          |         |         |        |         |         |
    #   __0__   __4__     __6__      __9__     _12__     _15__    _18__     _21__     _24__
    #  |  |  | |  |  |   |  |  |    |  |  |   |  |  |   |  |  |  |  |  |   |  |  |   |  |  |
    #  0  1  2 3  4  5   6  7  8    9 10 11  12 13 14  15 16 17 18 19 20  21 22 23  24 25 26

    # use index as virtual group identifier?
    started_servers = []
    netgroups = partition_all(3, servers)
    first_level = [g[0] for g in netgroups]

    for i in range(9):
        if i < 3:
            net_group = [first_level, servers[:3]]
        elif i < 6:
            net_group = [first_level, servers[3:6]]
        else:
            net_group = [first_level, servers[6:]]

        node = DataRepNode(servers[i], net_group, loop, initial_data)
        started_servers.append(node)

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        for s in started_servers:
            s.kill()
        loop.close()
        print('over and out')

