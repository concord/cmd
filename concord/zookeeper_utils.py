import requests
from kazoo.client import KazooClient
from concord.internal.thrift.ttypes import TopologyMetadata
from concord.utils import build_logger
from thrift import TSerialization

logger = build_logger('cmd.zookeeper_utils')

def _zookeeper_data_request(zkurl, zkpath, callback):
    logger.info("Connecting to zookeeper at: %s" % zkurl)
    zk = KazooClient(hosts=zkurl)
    data = None
    try:
        logger.debug("Starting zk connection")
        zk.start()
        if not zk.exists(zkpath):
            logger.error('Path on zk doesn\'t exist: ' + zkpath)
            return None
        data = callback(zk)
    except Exception as e:
        logger.exception(e)
    finally:
        logger.debug("Closing zk connection")
        zk.stop()
    return data

def get_topology_metadata(zkurl, zkpath):
    def concord_topology_meta():
        meta = TopologyMetadata()
        logger.debug("Serializing TopologyMetadata() from %s" % zkpath)
        data, stat = zk.get(zkpath)
        logger.debug("Status of 'getting' %s: %s" % (zkpath, str(stat)))
        return deserialize(meta, data)
    return zookeeper_data_request(zkurl, zkpath, concord_topology_meta)

def get_scheduler_master_url(zkurl, zkpath):
    def get_master_ip(zk):
        logger.debug("Serializing TopologyMetadata() from %s" % zkpath)
        data, stat = zk.get(zkpath + "/masterurl")
        logger.debug("Status of 'getting' %s/masterurl: %s" % (zkpath, str(stat)))
        return str(data) # ipaddr
    return zookeeper_data_request(zkurl, zkpath, get_master_ip)
