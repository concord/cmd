#!/usr/bin/env python
from optparse import OptionParser
from graphviz import Digraph
from kazoo.client import KazooClient
from concord.internal.thrift.ttypes import *
from operator import attrgetter

import json
import logging

from thrift.protocol import TJSONProtocol, TBinaryProtocol
from thrift.transport import TTransport

from concord.utils import *
from concord.thrift_utils import *

logger = build_logger('cmd.print_graph')

def generate_options():
    usage = "usage: %prog [options] arg"
    parser = OptionParser(usage)
    parser.add_option("-z", "--zookeeper", dest="zookeeper",
                      action="store", help="i.e: 1.2.3.4:2181,2.3.4.5:2181")
    parser.add_option("-p", "--zookeeper_path", dest="zk_path",
                      help="zookeeper path, i.e.: /concord", action="store")
    parser.add_option("-v", "--verbose",
                      action="store_true", dest="verbose")
    parser.add_option("-q", "--quiet",
                      action="store_false", dest="verbose")
    parser.add_option("-f", "--file", help="output file",
                      default="dependency_graph",
                      action="store", dest="filename")
    return parser


def print_endpoint(e):
    return "{0}:{1}".format(e.ip, e.port)

def print_physical(node, name):
    return "Name:\t\t{0}\lMem:\t\t{1}MB\lCPUs:\t\t{2}\lDisk:\t\t{3}MB\l" \
        "Principal:\t{4}\lProxy:\t\t{5}\lRouter:\t\t{6}\lSlave:\t\t{7}\lTask:\t\t{8}\l" \
        "Exec:\t\t{9} {10}\lUser ENVs:\t{11}\lDocker:\t\t{12}\l" \
        "Reconciling:\t{13}\l".format(
            name, node.mem, str(node.cpus), str(node.disk),
            print_endpoint(node.taskHelper.client),
            print_endpoint(node.taskHelper.proxy),
            print_endpoint(node.taskHelper.router),
            node.slaveId, node.taskId,
            node.taskHelper.execName,
            ", ".join(node.taskHelper.clientArguments),
            ", ".join(node.taskHelper.environmentExtra),
            node.taskHelper.dockerContainer, node.needsReconciliation)

def print_single_stream(s):
    return json.dumps(s, default=lambda o: o.__dict__, indent=4)

def print_edge(comp1, comp2, dot):
    for streamMetadata in comp1.istreams:
        if streamMetadata.name in comp2.ostreams:
            for node1 in comp1.nodes:
                for node2 in comp2.nodes:
                    dot.node(node1.taskId, print_physical(node1, comp1.name))
                    dot.node(node2.taskId, print_physical(node2, comp2.name))
                    dot.edge(node2.taskId, node1.taskId,
                             label=print_single_stream(streamMetadata))

def print_dot(meta, filename):
    if meta == None or meta.computations == None:
        logger.error('No graph to trace')
        return

    dot = Digraph(comment='Concord Systems',
                  node_attr={"shape":"rectangle",
                             "align":"left",
                             "fontname":"Arial",
                             "fontsize":"12"})

    for key, comp1 in meta.computations.iteritems():
        for node1 in comp1.nodes:
            dot.node(node1.taskId, print_physical(node1, comp1.name))

    for key, comp1 in meta.computations.iteritems():
        for key2, comp2 in meta.computations.iteritems():
            if key == key2: continue
            print_edge(comp1, comp2, dot)

    logger.info("Graph generated, rendering now")
    dot.render(filename, view=True, cleanup=True)


def main():
    parser = generate_options()
    (options, args) = parser.parse_args()
    default_options(options)

    print_dot(get_zookeeper_metadata(options.zookeeper, options.zk_path),
              options.filename)

if __name__ == "__main__":
    main()
