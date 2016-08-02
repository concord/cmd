#!/usr/bin/env python
import sys
import thrift
import logging
import argparse
from concord.utils import *
from concord.thrift_utils import *
from concord.functional_utils import *
from terminaltables import AsciiTable

logger = build_logger('cmd.kill_task')

def generate_options():
    parser = argparse.ArgumentParser()
    parser.add_argument("-p"
                        ,"--zk_path"
                        ,metavar="zookeeper-path"
                        ,help="Path of concord topology on zk"
                        ,action="store")
    parser.add_argument("-z"
                        ,"--zookeeper"
                        ,metavar="zookeeper-hosts"
                        ,help="i.e: 1.2.3.4:2181,2.3.4.5:2181"
                        ,action="store")
    parser.add_argument("-t"
                        ,"--task_id"
                        ,help="The id of task to kill"
                        ,action="store")
    parser.add_argument("-a"
                        ,"--all"
                        ,help="Kill all computations"
                        ,action="store_true")
    return parser

def validate_options(options, parser):
    if options.all and options.task_id:
        parser.error('You are using task_id and passing the all flag')
    default_options(options)

def kill(zookeeper, zk_path, task_ids):
    if len(task_ids) == 0:
        logger.info('Kill received an empty list of task_ids')
        return
    logger.info("Getting master ip from zookeeper")
    ip = get_zookeeper_master_ip(zookeeper, zk_path)
    logger.info("Found leader at: %s" % ip)
    (addr, port) = ip.split(":")
    logger.info("Initiating connection to scheduler")
    cli = get_sched_service_client(addr,int(port))
    logger.info("Sending request to scheduler")
    try:
        for task_id in task_ids:
            logger.info('Sending request to kill task: %s', task_id)
            cli.killTask(task_id)
    except thrift.Thrift.TApplicationException as e:
        leftover = task_ids[task_ids.index(task_id):]
        logger.error("Error killing task: %s" % task_id)
        logger.error("Tasks that could not be killed... %s" % ", ".join(leftover))
        logger.exception(e)
    logger.info("Done sending request to server")

def collect_taskids(zookeeper, zkpath):
    """ Querys zk for topology metadata and returns a list of task_ids"""
    meta = get_zookeeper_metadata(zookeeper, zkpath)
    if meta is None or len(meta.computations) == 0:
        return []

    # Obtain taskids from list of nodes from list of computations then flatten
    return flatten([list(d.taskId for d in c.nodes) \
                    for c in meta.computations.values()])

def parse_input(iterable, user_input):
    """ Parses user input and either will abort or return the data at
    a given index or indicies or range"""
    def token_expand(token):
        if token.isdigit():
            return [int(token)]
        # Otherwise, we are dealing with a range
        inclusive_range = token.split('..')
        if len(inclusive_range) == 2:
            return range(int(inclusive_range[0]), int(inclusive_range[1]) + 1)
        raise Exception('Unexpected token detected')

    tokens = user_input.split(',')
    if len(tokens) == 1:
        if tokens[0] == 'quit' or tokens[0] == 'q':
            sys.exit(1)
        elif tokens[0] == 'all' or tokens[0] == 'a':
            return iterable

    # Expand ranges, flatten results, then insert into set to remove dups
    indicies = set(flatten(map(token_expand, tokens)))

    # Returns desired choices performing bounds check at the same time
    def fetch_index(index):
        if index < 1 or index > len(iterable):
            raise Exception('Index out of bounds')
        return iterable[index-1]
    return map(fetch_index, indicies)

def prompt_selection(iterable, heading, printer):
    """ Takes 'iterable' and displays it in an ascii table using printable
    method to determine how to transform the data. Will also prompt user for
    a selection and return the blob to the caller"""
    def preprinter(value_pair):
        idx, data = value_pair
        return [str(idx)] + printer(data)

    # Create table
    heading.insert(0, 'Idx')
    table_data = zip(xrange(1, len(iterable) + 1), iterable)
    table_data = map(preprinter, table_data)
    table_data.insert(0, heading)

    # Print table in nice format, wait for user selection
    print AsciiTable(table_data).table
    inpt = ""
    while inpt == "":
        inpt = raw_input('Selection: [1..%d, 1,x,y,..., quit(q), all(a)]:'\
                         ' ' % len(iterable))

    return parse_input(iterable, inpt)

def prompt_nodes(pcomp_metadata):
    def pcomp_printable(pcomp_metadata):
        return [pcomp_metadata.taskId,
                str(pcomp_metadata.cpus),
                str(pcomp_metadata.mem)]

    print '\nSelect a node to terminate:'
    nodedetail_header = ['Task id', 'cpus', 'mem']
    selection = prompt_selection(pcomp_metadata, nodedetail_header, pcomp_printable)
    return map(lambda c: c.taskId, selection)

def prompt_computations(pcomp_layout):
    def layout_printable(pcomp_layout):
        def pretty_md(stream_metadata):
            return "\n".join(map(lambda c: '(%s <-> %s)' % (c.name, c.grouping),
                                 stream_metadata))
        return [pcomp_layout.name,
                pretty_md(pcomp_layout.istreams),
                "\n".join(pcomp_layout.ostreams)]

    print '\nSelect a computation to inspect:'
    header = ['Computation name', 'istreams/grouping', 'ostreams/grouping']
    selection = prompt_selection(pcomp_layout, header, layout_printable)
    return flatten(map(lambda c: c.nodes, selection))

def interactive_mode(zookeeper, zk_path):
    """ presents to the user an easy to use prompt so that they may selectively
    search through computations and eventually choose a concord node to kill"""
    print 'Querying zookeeper for cluster topology...'
    meta = get_zookeeper_metadata(zookeeper, zk_path)

    # Exit on failure, or if no computations exist
    if meta == None:
        logger.critical('Could not connect to zk')
        return
    elif meta.computations == None or len(meta.computations) == 0:
        logger.info('No computations to report')
        return

    selection = meta.computations.values()
    user_path = [prompt_computations, prompt_nodes]
    for current_action in user_path:
        selection = current_action(selection)

    # Terminate selected node
    kill(zookeeper, zk_path, selection)

def main():
    parser = generate_options()
    options = parser.parse_args()
    validate_options(options, parser)

    if options.all:
        kill(options.zookeeper, options.zk_path,
             collect_taskids(options.zookeeper, options.zk_path))
    elif options.task_id:
        kill(options.zookeeper, options.zk_path, [options.task_id])
    else:
        interactive_mode(options.zookeeper, options.zk_path)

if __name__ == "__main__":
    main()
