# -*- coding: utf-8 -*-
from concurrent import futures
import grpc
import ghub_pb2
import time
import logging

HUB_PORT = 50011


class Channel(object):
    def __init__(self, channel, timestamp):
        self.channel = channel
        self.timestamp = timestamp


class GHubServer(ghub_pb2.GHubServerServicer):
    def __int__(self):
        self.clients = {}

    def Register(self, request, context):
        now = time.time()
        if request.name not in self.clients:
            addr = '{}:{}'.format(request.ip, request.port)
            channel = grpc.insecure_channel(addr)
            self.clients[request.name] = Channel(channel, now)
            logging.info('client {} from {} registered.'.format(
                request.name, addr))
        else:
            self.clients[request.name].timestamp = now
        return ghub_pb2.ReturnState(ret=0)

    def RemoteCall(self, request, context):
        dst_name = request.dst
        if dst_name not in self.clients:
            return ghub_pb2.ReturnState(ret=-1)
        channel = self.clients[dst_name]
        stub = ghub_pb2.GHubClientStub(channel)
        return stub.ForwardCall(request)

    def CheckChannels(self):
        now = time.time()
        rm_channels = []
        for name, channel in self.clients.iteritems():
            if now - channel.timestamp >= 60:
                rm_channels.append(name)

        for name in rm_channels:
            self.clients.pop(name, None)
            logging.info('client {} disconnected.'.format(name))


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    ghub_server = GHubServer()
    ghub_pb2.add_GHubServerServicer_to_server(ghub_server, server)
    server.add_insecure_port('[::]:{}'.format(HUB_PORT))
    server.start()
    try:
        while True:
            time.sleep(10)
            ghub_server.CheckChannels()
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == "__main__":
    serve()
