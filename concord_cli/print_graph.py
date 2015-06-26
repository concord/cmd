#!/usr/bin/env python
from optparse import OptionParser
from graphviz import Digraph
from kazoo.client import KazooClient
from concord_cli.generated.concord.internal.thrift.ttypes import *
from operator import attrgetter;

import json
import logging

from thrift.protocol import TJSONProtocol, TBinaryProtocol
from thrift.transport import TTransport

from concord_cli.utils import *

def generate_options():
    usage = "usage: %prog [options] arg"
    parser = OptionParser(usage)
    parser.add_option("-z", "--zookeeper", dest="zookeeper",
                      action="store", help="i.e: 1.2.3.4:2181,2.3.4.5:2181")
    parser.add_option("-v", "--verbose",
                      action="store_true", dest="verbose")
    parser.add_option("-q", "--quiet",
                      action="store_false", dest="verbose")
    parser.add_option("-f", "--file", help="output file",
                      default="dependency_graph",
                      action="store", dest="filename")
    return parser


def getmeta(zkurl):
    print "Connecting to:", zkurl
    zk = KazooClient(hosts=zkurl)
    meta = TopologyMetadata()
    try:
        print "Starting zk connection"
        zk.start()
        print "Serializing TopologyMetadata() from /bolt"
        data, stat = zk.get("/bolt")
        print "Stattus of 'getting' /bolt: ", stat
        bytes_to_thrift(data, meta)
    except Exception as e:
        print "Error: ", e
    finally:
        print "Closing zk connection"
        zk.stop()

    return meta


def print_physical(node):
    return json.dumps(node, default=lambda o: o.__dict__, indent=4)

def print_single_stream(s):
    return json.dumps(s, default=lambda o: o.__dict__, indent=4)

def print_streams(comp):
    return json.dumps(comp, default=lambda o: o.__dict__, indent=4)

def print_edge(comp1, comp2, dot):
    for streamMetadata in comp1.istreams:
        if streamMetadata.name in map(attrgetter('name'), comp2.ostreams):
            for node1 in comp1.nodes:
                for node2 in comp2.nodes:
                    print "EDGE!: ", node2.taskId, " -> ", node1.taskId
                    dot.node(node1.taskId, print_streams(node1))
                    dot.node(node2.taskId, print_physical(node2))
                    dot.edge(node2.taskId, node1.taskId,
                             label=print_single_stream(streamMetadata))

def printdot(options):
    dot = Digraph(comment='Concord Systems',
                  node_attr={"shape":"rectangle",
                             "align":"left",
                             "fontname":"Arial",
                             "fontsize":"16"})
    meta = getmeta(options.zookeeper)
    for key in meta.computations:
        comp1 = meta.computations[key]
        for key2 in meta.computations:
            if key == key2: continue
            comp2 = meta.computations[key2]
            print_edge(comp1, comp2, dot)

    print "Graph generated, rendering now"
    dot.render(options.filename, view=True, cleanup=True)


def main():
    logging.basicConfig()
    parser = generate_options()
    (options, args) = parser.parse_args()
    if not options.zookeeper:
        parse.error("need to specify zookeeper addr")

    printdot(options)

if __name__ == "__main__":
    main()
