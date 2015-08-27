#!/usr/bin/env python
from optparse import OptionParser
from graphviz import Digraph
from kazoo.client import KazooClient
from concord_cli.generated.concord.internal.thrift.ttypes import *

import json
import logging

from thrift.protocol import TJSONProtocol, TBinaryProtocol
from thrift.transport import TTransport

from concord_cli.utils import *

logging.basicConfig()
logger = logging.getLogger('cmd.tracer')
logger.setLevel(logging.INFO)

def generate_options():
    usage = "usage: %prog [options] arg"
    parser = OptionParser(usage)
    parser.add_option("--zookeeper", dest="zookeeper", default="localhost:2181",
                      action="store", help="i.e: 1.2.3.4:2181,2.3.4.5:2181")
    parser.add_option("--file", help="output file",
                      default="trace_graph", action="store", dest="filename")
    parser.add_option("--scheduler-path", help="zookeeper path, i.e.: /concord",
                      default="/concord", action="store", dest="zk_path")
    parser.add_option("--trace-id", help="any trace id, i.e.: 1121345",
                      action="store", dest="trace_id")
    parser.add_option("--scheduler-address", help="i.e.: 1.2.3.4:11219",
                      action="store", dest="scheduler")
    return parser

def validate_options(options, parser):
    default_options(options)
    if not options.trace_id:
        parser.error("need to specify trace id")
    if not options.scheduler:
        parser.error("need to specify scheduler_address")

def gen_trace(options):
    logger.info("what is my ip? %s" % options.scheduler)
    (addr, port) = options.scheduler.split(":")
    trace_cli = get_trace_service_client(addr, 11219)
    logger.info("about to get trace id: %s" % options.trace_id)
    logger.debug(dir(trace_cli))
    spans = trace_cli.getTrace(long(options.trace_id))
    dot = Digraph(comment='Concord Systems',
                  format='svg',
                  engine='dot',
                  graph_attr={
                      "overlap":"false"
                  },
                  edge_attr={
                      "overlap":"false",
                      "splines":"true",
                  },
                  node_attr={"shape":"rectangle",
                             "overlap":"false",
                             "fontname":"Arial",
                             "fontsize":"16"})

    times = {}
    for span in spans:
        for ann in span.annotations:
            key = str(span.id) + "_" + str(ann.type)
            times[key] = ann.timestamp

    for span in spans:
        for ann in span.annotations:
            ant = str(
                AnnotationType. _VALUES_TO_NAMES[ann.type] +
                "\n" + ann.host.ip + ":" + str(ann.host.port)
            )
            parent = str(span.id)
            child = str(span.parentId)
            dot.node(parent, span.name)
            dot.node(child)
            if ann.type == AnnotationType.SERVER_SEND:
                key = str(span.id) + "_" + str(AnnotationType.SERVER_RECV)
                if times.has_key(key):
                    t = times[key]
                    ant += ", " + str(t - ann.timestamp) + "ms"
                dot.edge(parent, child, label=ant)
            elif ann.type == AnnotationType.SERVER_RECV:
                key = str(span.id) + "_" + str(AnnotationType.SERVER_SEND)
                if times.has_key(key):
                    t = times[key]
                    ant += ", " + str(ann.timestamp - t) + "ms"
                dot.edge(child, parent, label=ant)
            elif ann.type == AnnotationType.CLIENT_RECV:
                key = str(span.id) + "_" + str(AnnotationType.CLIENT_SEND)
                if times.has_key(key):
                    t = times[key]
                    ant += ", " + str(ann.timestamp - t) + "ms"
                dot.edge(parent, child, label=ant)
            elif ann.type == AnnotationType.CLIENT_SEND:
                key = str(span.id) + "_" + str(AnnotationType.CLIENT_RECV)
                if times.has_key(key):
                    t = times[key]
                    ant += ", " + str(t - ann.timestamp) + "ms"
                dot.edge(child, parent, label=ant)
            else:
                dot.edge(parent,child,label='unknown')

    logger.info("Graph generated, rendering now")
    dot.render(options.filename, view=True, cleanup=True)


def main():
    parser = generate_options()
    (options, args) = parser.parse_args()
    validate_options(options,parser)
    gen_trace(options)

if __name__ == "__main__":
    main()
