import json
from thrift import Thrift
from kazoo.client import KazooClient
from concord_cli.generated.concord.internal.thrift.ttypes import *
from concord_cli.generated.concord.internal.thrift import (
    BoltTraceAggregatorService,
    BoltSchedulerService
)
from thrift.protocol import (
    TJSONProtocol, TBinaryProtocol
)
from thrift.transport import (
    TSocket,TTransport
)

def bytes_to_thrift(bytes, thrift_struct):
    transportIn = TTransport.TMemoryBuffer(bytes)
    protocolIn = TBinaryProtocol.TBinaryProtocol(transportIn)
    thrift_struct.read(protocolIn)

def thrift_to_json(thrift_struct):
    return json.dumps(thrift_struct, default=lambda o: o.__dict__,
            sort_keys=True, indent=4)

def get_zookeeper_master_ip(zkurl, zkpath):
    print "Connecting to:", zkurl
    zk = KazooClient(hosts=zkurl)
    ip = ""
    try:
        print "Starting zk connection"
        zk.start()
        print "Serializing TopologyMetadata() from /bolt"
        data, stat = zk.get(zkpath + "/masterip")
        print "Stattus of 'getting' " + zkpath + "/masterip: ", stat
        ip = str(data)
    except Exception as e:
        print "Error: ", e
    finally:
        print "Closing zk connection"
        zk.stop()
    return ip

def tproto(ip, port):
    socket = TSocket.TSocket(ip, port)
    transport = TTransport.TFramedTransport(socket)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    return (protocol, transport)

def get_sched_service_client(ip, port):
    (protocol, transport) = tproto(ip, port)
    client = BoltSchedulerService.Client(protocol)
    transport.open()
    return client

def get_trace_service_client(ip, port):
    (protocol, transport) = tproto(ip, port)
    client = BoltTraceAggregatorService.Client(protocol)
    transport.open()
    return client
