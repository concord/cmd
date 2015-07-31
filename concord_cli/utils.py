import os
import json
import logging
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

logging.basicConfig()
logger = logging.getLogger('cmd.utils')
logger.setLevel(logging.INFO)

class ContextDirMgr:
    def __init__(self, path):
        self.new_dir = os.path.dirname(os.path.abspath(path))

    def __enter__(self):
        self.old_dir = os.getcwd()
        os.chdir(self.new_dir)

    def __exit__(self, value, type, traceback):
        os.chdir(self.old_dir)

def bytes_to_thrift(bytes, thrift_struct):
    transportIn = TTransport.TMemoryBuffer(bytes)
    protocolIn = TBinaryProtocol.TBinaryProtocol(transportIn)
    thrift_struct.read(protocolIn)

def thrift_to_json(thrift_struct):
    return json.dumps(thrift_struct, default=lambda o: o.__dict__,
            sort_keys=True, indent=4)

def get_zookeeper_master_ip(zkurl, zkpath):
    logger.info("Connecting to:%s" % zkurl)
    zk = KazooClient(hosts=zkurl)
    ip = ""
    try:
        logger.debug("Starting zk connection")
        zk.start()
        logger.debug("Serializing TopologyMetadata() from /bolt")
        data, stat = zk.get(zkpath + "/masterip")
        logger.debug("Status of 'getting' %s/masterip: %s" % (zkpath, str(stat)))
        ip = str(data)
    except Exception as e:
        logger.exception(e)
    finally:
        logger.debug("Closing zk connection")
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
