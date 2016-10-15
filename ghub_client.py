# -*- coding: utf-8 -*-
from concurrent import futures
import grpc
import ghub_pb2
import cPickle
import zlib
import threading
import time
import logging

entities = {}


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
    logging.error('{}:{}'.format(ip, port))
    client.start()
    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        client.stop(0)


class GHubProxy(object):
    def __init__(self, hub_ip, hub_port):
        channel = grpc.insecure_channel('{}:{}'.format(hub_ip, hub_port))
        self.stub = ghub_pb2.GHubServerStub(channel)

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
            port=50012,
            name='client1'))


def HeartBeat(proxy):
    while True:
        proxy.Register()
        time.sleep(10)


def TestMethod(a, b):
    print 'TestMethod called with: ', a, b


class Entity(object):
    def __init__(self, name):
        self.name = name

    def TestMethod(self, a, b):
        print '{}.TestMethod called with: {} {}'.format(self.name, a, b)

if __name__ == "__main__":
    import sys
    logging.basicConfig(stream=sys.stdout)
    t1 = threading.Thread(target=serve, args=('[::]', 50012))
    t1.start()
    proxy = GHubProxy('localhost', 50011)
    t2 = threading.Thread(target=HeartBeat, args=(proxy,))
    t2.start()

    user = Entity('user')
    entities[user.name] = user
    account = Entity('account')
    entities[account.name] = account

    time.sleep(1)
    for a, b in zip(range(1, 10), range(11, 20)):
        proxy.CallMethod('client1', 1, '', 'TestMethod', (a, b))
        proxy.CallMethod('client1', 2, 'user', 'TestMethod', (a, b))
        proxy.CallMethod('client1', 2, 'account', 'TestMethod', (a, b))
        time.sleep(10)
