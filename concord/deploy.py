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
    "fetch_url": "",
    "mem": 2048,
    "disk":2048,
    "executor_args" : ["--kafka_consumer_debug=cgrp"],
    "retries" : 3,
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
    "docker_container": "concord/client_devbox",
    "force_pull_container": "true"
}
"""

import os
import re
import json
import tarfile
import logging
import argparse
import uuid
from kazoo.client import KazooClient
from concord.internal.thrift.ttypes import *
from concord.utils import *
from concord.thrift_utils import *
from concord.functional_utils import *

logger = build_logger('cmd.deploy')

DEFAULTS = dict(
    mem = 256,
    update_binary = True,
    disk = 2048,
    cpus = 1,
    instances = 1,
    execute_as_user = "",
    environment_variables = [],
    executable_arguments = [],
    framework_v_module = "",
    framework_logging_level = 0,
    exclude_compress_files = [],
    docker_container = "",
    force_pull_container = True,
    retries = 0,
    executor_args = []
)

def validate_json_raw_config(dictionary, parser):
    valid_keys = ["executable_arguments", "docker_container",
                  "fetch_url", "mem", "disk", "cpus", "retries",
                  "framework_v_module", "instances",
                  "framework_logging_level", "environment_variables",
                  "zookeeper_hosts", "exclude_compress_files",
                  "update_binary", "execute_as_user",
                  "docker_container", "force_pull_container",
                  "executor_args"]
    reqs = ['compress_files', 'executable_name', 'computation_name',
            'zookeeper_hosts', 'zookeeper_path']

    # Add additional options from local concord.cfg file if necessary
    default_manifest_options(dictionary, valid_keys + reqs)

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
        parser.error("The executable name '" + conf["executable_name"] +
                     "' must exist in the 'compress_files' list")

    return conf

def generate_options():
    parser = argparse.ArgumentParser()
    parser.add_argument("config", metavar="config-file", action="store",
                        help="i.e: ./src/config.json")
    return parser

def validate_options(options, parser):
    if not options.config:
        parser.error("need to specify config file")

def tar_file_list(white_list, black_list):
    """
    Add files/dirs in the white_list. When you encounter something in the
    blacklist, exclude it. This is useful when adding a subdirectory containing
    many files, some of which you wish to exclude. The black_list should be
    formatted as regular expressions.
    """

    def expand_white_list(path):
        if os.path.isfile(path):
            return [path]
        files, directories = split_predicate(lambda x: os.path.isfile(x), os.listdir(path))
        return files + flat_map(lambda x: expand_white_list(path + '/' + x), directories)

    # set is useful for deduping
    white_list = flat_map(lambda x: expand_white_list(x), set(white_list))
    black_list = map(re.compile, set(black_list))

    # Return paths that don't contain any matches against any pieces of the path
    def exclude_matches(path):
        def on_black_list(component):
            return any(map(lambda x: x.match(component) != None, black_list))

        path_components = path.split('/')
        # We cannot guarantee that in mesos we'll have even read access to
        # parent dirs. So we cannot support compressing files wihtout flattening
        # the structure. Which means for simplicity instead of providing tuples
        # (../relative../../path -> desired/destination/path) PER file added
        # we only support files starting from this directory down
        # Same restriction as the one imposed by docker.
        #
        if len(path_components) > 0 and path_components[0] == '..':
            raise Exception("\n\n==> Relative paths that do not start from this %s" %
                         "directory are not supported, bad path: %s \n" % path)

        # Truncate by one if relative dir is prepended with a '.'
        if len(path_components) > 0 and path_components[0] == '.':
            path_components = path_components[1:-1]
        return not any(map(on_black_list, path_components))

    return filter(exclude_matches, white_list)

def build_thrift_request(request):
    logger.debug("JSON Request: %s" % json.dumps(request, indent=4, separators=(',', ': ')))
    tar_files = tar_file_list(request["compress_files"],
                              request["exclude_compress_files"])

    if len(tar_files) == 0: raise Exception("Nothing to tar.gz :'(")
    tar_name = os.path.abspath("concord_slug_" + str(uuid.uuid4()) + ".tar.gz")
    if os.path.exists(tar_name): logger.debug("%s exists! overriding" % tar_name)
    logger.debug("Created tar file: %s", tar_name)
    with tarfile.open(tar_name, "w:gz") as tar:
        for name in tar_files:
            logger.info("Adding tarfile: %s" % name)
            tar.add(name)

    logger.debug("Reading slug into memory")
    slug = open(tar_name, "rb").read()
    logger.info("Size of tar file is: %s", human_readable_units(len(slug)))
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
    req.executorArgs = request["executor_args"]
    req.forceUpdateBinary = request["update_binary"]
    req.forcePullContainer = request["force_pull_container"]
    req.taskHelper = ExecutorTaskInfoHelper()
    req.taskHelper.retries = request["retries"]
    req.taskHelper.execName = request["executable_name"]
    req.taskHelper.clientArguments = request["executable_arguments"]
    req.taskHelper.environmentExtra = request["environment_variables"]
    req.taskHelper.frameworkLoggingLevel = request["framework_logging_level"]
    req.taskHelper.frameworkVModule = request["framework_v_module"]
    req.taskHelper.folder = os.path.dirname(
        os.path.relpath(request["executable_name"]))
    req.taskHelper.user = request["execute_as_user"]
    req.taskHelper.dockerContainer = request["docker_container"]
    logger.info("Thrift Request: %s" % thrift_to_json(req))
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
