#!/usr/bin/env python
"""
Example of configuration file
{
    "executable_arguments": [
        "--hello=world",
        "--zookeeper=2",
        "logtostderr=2"
    ],
    "compress_files": [
        "folder",
        "file1.py",
        "file2.cpp"
    ],
    "docker_container": "concord/concord",
    "fetch_url": "",
    "mem": 2048,
    "disk":"10240",
    "zookeeper_path": "/concord",
    "cpus": 4,
    "framework_v_module": "",
    "instances": 1,
    "framework_logging_level": 1,
    "environment_variables": [
        "LD_LIBRARY_PATH=./lib:/usr/local/lib",
        "MY_OPT=2"
    ],
    "zookeeper_hosts": "127.0.0.1:2181,10.1.2.3:2181",
    "executable_name": "my_binary",
    "exclude_compress_files": [
        ".git"
    ],
    "computation_name": "word-count-source",
    "update_binary": true,
    "execute_as_user": "agallego",
    "docker_container": "concord/client_devbox"
}
"""

import os
import re
import json
import tarfile
import logging
import argparse
from kazoo.client import KazooClient
from concord_cli.generated.concord.internal.thrift.ttypes import *
from concord_cli.utils import *

logging.basicConfig()
logger = logging.getLogger('cmd.deploy')
logger.setLevel(logging.INFO)

DEFAULTS = dict(
    mem = 256,
    update_binary = True,
    disk = 10240,
    cpus = 1,
    instances = 1,
    execute_as_user = "",
    environment_variables = [],
    executable_arguments = [],
    framework_v_module = "",
    framework_logging_level = 0,
    exclude_compress_files = [],
    docker_container = "",
)

def validate_json_raw_config(dictionary, parser):
    valid_keys = ["executable_arguments", "docker_container",
                  "fetch_url", "mem", "disk", "cpus",
                  "framework_v_module", "instances",
                  "framework_logging_level", "environment_variables",
                  "zookeeper_hosts", "exclude_compress_files",
                  "update_binary", "execute_as_user",
                  "docker_container"]
    reqs = ['compress_files', 'executable_name', 'computation_name',
            'zookeeper_hosts', 'zookeeper_path']

    all_keys = list(valid_keys)
    all_keys.extend(reqs)

    for k in dictionary:
        if k not in all_keys:
            parser.error("Key is not a valid concord request key: " + str(k))

    for k in reqs:
        if not dictionary.has_key(k):
            contents = json.dumps(dictionary, indent=4, separators=(',', ': '))
            parser.error("Please specify: " + k + ", parsed file: " + contents)


def parseFile(filename, parser):
    if os.path.splitext(filename)[1] != '.json':
        raise Exception('config file must end in .json')

    data = {}
    with open(filename) as data_file:
        data = json.load(data_file)

    validate_json_raw_config(data, parser)

    conf = DEFAULTS.copy()
    conf.update(data)

    if not conf["executable_name"] in conf["compress_files"]:
        logger.debug("Adding %s to compress_files" % conf["executable_name"])
        conf["compress_files"].append(conf["executable_name"])

    return conf

def generate_options():
    parser = argparse.ArgumentParser()
    parser.add_argument("config", metavar="config-file", action="store",
                        help="i.e: ./src/config.json")
    return parser

def validate_options(options, parser):
    if not options.config:
        parser.error("need to specify config file")
    default_options(options)

def tar_file_list(white_list, black_list):
    """
    Add files/dirs in the white_list. When you encounter something in the
    blacklist, exclude it. This is useful when adding a subdirectory containing
    many files, some of which you wish to exclude. The black_list should be
    formatted as regular expressions.
    """

    # dedupe
    white_list = set(white_list)
    black_list = set(black_list)
    black_list = map(re.compile, black_list)

    # check a file against the blacklist
    def not_on_black_list(path):
        return reduce(lambda memo, x: memo and x.match(path) == None,
                black_list,
                True)

    # iterate through a set of subdirectories, filtering by blacklist
    def iterate_files(path):
        if os.path.isfile(path):
            if not_on_black_list(path):
                return [path]
            else:
                return []
        else:
            files = map(lambda x: iterate_files(path + '/' + x), os.listdir(path))
            files = reduce(lambda x, y: x + y, files, [])
            return filter(not_on_black_list, files)

    # flat map `iterate_files` over the list of files/dirs to include
    return reduce(lambda memo, x: memo + iterate_files(x),
            white_list,
            [])

def build_thrift_request(request):
    logger.debug("JSON Request: %s" % json.dumps(request, indent=4, separators=(',', ': ')))
    tar_files = tar_file_list(request["compress_files"],
                              request["exclude_compress_files"])

    tar_name = os.path.abspath("concord_slug.tar.gz")
    if os.path.exists(tar_name): logger.debug("%s exists! overriding" % tar_name)
    if len(tar_files) == 0: raise Exception("Nothing to tar.gz :'(")

    with tarfile.open(tar_name, "w:gz") as tar:
        for name in tar_files:
            logger.debug("Adding tarfile: %s" % name)
            tar.add(name)

    logger.debug("Reading slug into memory")
    slug = open(tar_name, "rb").read()
    if os.path.exists(tar_name):
        logger.debug("Removing slug %s", tar_name)
        os.remove(tar_name)

    logger.debug("Making request object")
    req = BoltComputationRequest()
    req.name = request["computation_name"]
    req.instances = request["instances"]
    req.cpus = request["cpus"]
    req.mem = request["mem"]
    req.disk = request["disk"]
    req.forceUpdateBinary = request["update_binary"]
    req.taskHelper = ExecutorTaskInfoHelper()
    req.taskHelper.execName = request["executable_name"]
    req.taskHelper.clientArguments = request["executable_arguments"]
    req.taskHelper.environmentExtra = request["environment_variables"]
    req.taskHelper.frameworkLoggingLevel = request["framework_logging_level"]
    req.taskHelper.frameworkVModule = request["framework_v_module"]
    req.taskHelper.folder = os.path.dirname(
        os.path.relpath(request["executable_name"]))
    req.taskHelper.user = request["execute_as_user"]
    req.taskHelper.dockerContainer = request["docker_container"]
    logger.debug("Thrift Request: %s" % req)
    req.slug = slug
    return req

def register(request, config):
    with ContextDirMgr(config):
        req = build_thrift_request(request)
        logger.debug("Getting master ip from zookeeper")
        ip = get_zookeeper_master_ip(
            request["zookeeper_hosts"], request["zookeeper_path"])
        (addr, port) = ip.split(":")

        logger.info("Sending computation to: %s" % ip)

        cli = get_sched_service_client(addr,int(port))
        logger.debug("Sending request to scheduler")
        cli.deployComputation(req)
        logger.debug("Done sending request to server")
        logger.info("Verify with the mesos host: %s that the service is running" % addr)

def main():
    parser = generate_options()
    options = parser.parse_args()
    validate_options(options,parser)
    register(parseFile(options.config, parser), options.config)

if __name__ == "__main__":
    main()
