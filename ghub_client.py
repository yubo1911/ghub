#!/usr/bin/python
# -*- coding: utf-8 -*-
from concurrent import futures
import grpc
import ghub_pb2
import cPickle
import zlib
import threading
import time
import logging
from docopt import docopt

entities = {}

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

shutdown_event = threading.Event()


class GHubClient(ghub_pb2.GHubClientServicer):
    def ForwardCall(self, request, context):
        args = cPickle.loads(zlib.decompress(request.args))
        method = request.method
        if request.typ == 1:
            # global methods
            func = globals().get(method, None)
            try:
                func(*args)
                return ghub_pb2.ReturnState(ret=0)
            except:
                return ghub_pb2.ReturnState(ret=-1)
        elif request.typ == 2:
            # entity methods
            entity = entities.get(request.entity, None)
            try:
                func = getattr(entity, method)
                func(*args)
                return ghub_pb2.ReturnState(ret=0)
            except:
                return ghub_pb2.ReturnState(ret=-1)

        return ghub_pb2.ReturnState(ret=-1)


def serve(ip, port):
    client = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    ghub_client = GHubClient()
    ghub_pb2.add_GHubClientServicer_to_server(ghub_client, client)
    client.add_insecure_port('{}:{}'.format(ip, port))
    logger.info('client listening on {}:{}'.format(ip, port))
    client.start()
    return client


class GHubProxy(object):
    def __init__(self, hub_ip, hub_port, name, port):
        channel = grpc.insecure_channel('{}:{}'.format(hub_ip, hub_port))
        self.stub = ghub_pb2.GHubServerStub(channel)
        self.port = port
        self.name = name

    def CallMethod(self, remote, typ, entity, method, args):
        byte_args = zlib.compress(cPickle.dumps(args, -1))
        call_info = ghub_pb2.CallInfo(
                dst=remote, typ=typ,
                entity=entity, method=method, args=byte_args)
        ret = self.stub.RemoteCall(call_info)
        return ret.ret

    def Register(self):
        self.stub.Register(ghub_pb2.ClientInfo(
            ip='localhost',
            port=self.port,
            name=self.name))


def HeartBeat(proxy):
    while not shutdown_event.is_set():
        proxy.Register()
        time.sleep(10)


def TestMethod(a, b):
    logger.info('TestMethod called with: {}, {}'.format(a, b))


class Entity(object):
    def __init__(self, name):
        self.name = name

    def TestMethod(self, a, b):
        logger.info('{}.TestMethod called with: {} {}'.format(self.name, a, b))


if __name__ == "__main__":
    doc = """Usage:
        ghub_client.py -p <port> -s <hub_port> -n <name>
        ghub_client.py (-h | --help)

    Options:
        -h --help       Show this screen
        -p              Listening port
        -s              Hub port
        -n              Client name
    """
    args = docopt(doc, version="ghub_client ver1.0")
    port = int(args['<port>'])
    hub_port = int(args['<hub_port>'])
    client_name = args['<name>']

    client_stub = serve('[::]', port)
    proxy = GHubProxy('localhost', hub_port, client_name, port)
    t2 = threading.Thread(target=HeartBeat, args=(proxy,))
    t2.deamon = True
    t2.start()

    user = Entity('user')
    entities[user.name] = user
    account = Entity('account')
    entities[account.name] = account

    time.sleep(2)
    for a, b in zip(range(1, 3), range(11, 13)):
        proxy.CallMethod(client_name, 1, '', 'TestMethod', (a, b))
        proxy.CallMethod(client_name, 2, 'user', 'TestMethod', (a, b))
        proxy.CallMethod(client_name, 2, 'account', 'TestMethod', (a, b))
        time.sleep(1)

    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        logger.error('KeyboardInterrupt')
        shutdown_event.set()
        client_stub.stop(0)
