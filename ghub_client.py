# -*- coding: utf-8 -*-
from concurrent import futures
import grpc
import ghub_pb2
import cPickle
import zlib


class GHubClient(ghub_pb2.GHubClientServicer):
    def ForwardCall(self, request, context):
        pass


def serve(ip, port):
    client = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    ghub_client = GHubClient()
    ghub_pb2.add_GHubClientServicer_to_server(ghub_client, client)
    client.add_insecure_port('{}:{}'.format(ip, port))
    client.start()


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

if __name__ == "__main__":
    serve('localhost', 50012)
