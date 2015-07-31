#!/usr/bin/env python
import thrift
import json
import logging
from optparse import OptionParser
from concord_cli.utils import *

logging.basicConfig()
logger = logging.getLogger('cmd.kill_task')
logger.setLevel(logging.INFO)

def generate_options():
    usage = "usage: %prog [options] arg"
    parser = OptionParser(usage)
    parser.add_option("--zookeeper-hosts", dest="zookeeper",
                      action="store", help="i.e: 1.2.3.4:2181,2.3.4.5:2181")
    parser.add_option("--task-id", dest="task_id", action="store")
    parser.add_option("--zookeeper-path", action="store", dest="zk_path")
    return parser

def validate_options(options, parser):
    if not options.task_id: parser.error("please specify --task-id")
    if not options.zk_path: parser.error("please specify --zookeeper-path")
    if not options.zookeeper: parser.error("please specify --zookeeper-hosts")


def kill(options):
    logger.info("Getting master ip from zookeeper")
    ip = get_zookeeper_master_ip(options.zookeeper, options.zk_path)
    logger.info("Found leader at: %s" % ip)
    (addr, port) = ip.split(":")
    logger.info("Initiating connection to scheduler")
    cli = get_sched_service_client(addr,int(port))
    logger.info("Sending request to scheduler")
    try: cli.killTask(options.task_id)
    except thrift.Thrift.TApplicationException as e:
        logger.error("Error killing task:%s" % options.task_id)
        logger.exception(e)
    logger.info("Done sending request to server")


def main():
    parser = generate_options()
    (options, args) = parser.parse_args()
    validate_options(options,parser)
    kill(options)

if __name__ == "__main__":
    main()
