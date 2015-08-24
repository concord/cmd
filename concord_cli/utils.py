import os
import json
import logging
from thrift import Thrift
from kazoo.client import KazooClient
from concord_cli.config import find_config
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
        if not zk.exists(zkpath):
            logger.error('Path on zk doesn\'t exist')
            return ip
        logger.debug("Serializing TopologyMetadata() from %s" % zkpath)
        data, stat = zk.get(zkpath + "/masterip")
        logger.debug("Status of 'getting' %s/masterip: %s" % (zkpath, str(stat)))
        ip = str(data)
    except Exception as e:
        logger.exception(e)
    finally:
        logger.debug("Closing zk connection")
        zk.stop()
    return ip

def get_zookeeper_metadata(zkurl, zkpath):
    logger.info("Connecting to: %s" % zkurl)
    zk = KazooClient(hosts=zkurl)
    meta = TopologyMetadata()
    try:
        logger.debug("Starting zk connection")
        zk.start()
        if not zk.exists(zkpath):
            logger.error('Path on zk doesn\'t exist')
            return None
        logger.debug("Serializing TopologyMetadata() from %s" % zkpath)
        data, stat = zk.get(zkpath)
        logger.debug("Status of 'getting' %s: %s" % (zkpath, str(stat)))
        bytes_to_thrift(data, meta)
    except Exception as e:
        logger.exception(e)
        return None
    finally:
        logger.debug("Closing zk connection")
        zk.stop()

    return meta

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

def flatten(xs):
    return reduce(lambda m, x: m + x, xs,[])

def default_options(opts):
    location = find_config(os.getcwd())
    if location is None:
        return
    with open(location, 'r') as data_file:
        config_data = json.load(data_file)
    opts_methods = dir(opts)
    if 'zookeeper' in opts_methods:
        opts.zookeepers = config_data['zookeeper_hosts']
    if 'zk_path' in opts_methods:
        opts.zk_path = config_data['zookeeper_path']
    if 'scheduler' in opts_methods:
        opts.scheduler = config_data['scheduler_address']
