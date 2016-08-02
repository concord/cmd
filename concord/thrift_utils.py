import json
import logging
from kazoo.client import KazooClient
from concord.internal.thrift.ttypes import *
from concord.internal.thrift import BoltSchedulerService
from concord.utils import build_logger
from thrift import Thrift
from thrift.protocol import (
    TJSONProtocol, TBinaryProtocol
)
from thrift.transport import (
    TSocket,TTransport
)

logging_format_string='%(levelname)s:%(asctime)s %(filename)s:%(lineno)d] %(message)s'
logger = build_logger('cmd.thrift_utils', logging_format_string)

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
            logger.error('Path on zk doesn\'t exist: ' + zkpath)
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


