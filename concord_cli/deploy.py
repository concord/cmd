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
    "disk":"10240",
    "zookeeper_path": "/bolt",
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

}
"""

import os
import json
import tarfile
from optparse import OptionParser
from kazoo.client import KazooClient
from concord_cli.generated.concord.internal.thrift.ttypes import *
from concord_cli.utils import *



def parseFile(filename, parser):
    data = {}
    with open(filename) as data_file:
        data = json.load(data_file)

    reqs = ['compress_files', 'executable_name', 'computation_name',
            'zookeeper_hosts', 'zookeeper_path']

    contents = json.dumps(data, indent=4, separators=(',', ': '))
    for k in reqs:
        if not data.has_key(k):
            parser.error("Please specify: " + k + ", parsed file: " + contents)

    print "Contents parsed: ", contents
    if not data.has_key("mem"): data["mem"] = 2048
    if not data.has_key("update_binary"): data["update_binary"] = True
    if not data.has_key("disk"): data["disk"] = 10240
    if not data.has_key("cpus"): data["cpus"] = 1
    if not data.has_key("instances"): data["instances"] = 1
    if not data.has_key("execute_as_user"): data["execute_as_user"] = ""
    if not data.has_key("environment_variables"):
        data["environment_variables"] = []
    if not data.has_key("executable_arguments"):
        data["executable_arguments"] = []
    if not data.has_key("framework_v_module"):
        data["framework_v_module"] = ""
    if not data.has_key("framework_logging_level"):
        data["framework_logging_level"] = 0
    if not data.has_key("exclude_compress_files"):
        data["exclude_compress_files"] = []

    if not request["executable_name"] in request["compress_files"]:
        print "Adding ", request["executable_name"], " to compress_files"
        request["compress_files"].append(request["executable_name"])

    return data

def generate_options():
    usage = "usage: %prog [options] arg"
    parser = OptionParser(usage)
    parser.add_option("--zookeeper", dest="zookeeper",
                      action="store", help="i.e: 1.2.3.4:2181,2.3.4.5:2181")
    parser.add_option("--config-file", dest="config",
                      action="store", help="i.e: ./src/config.json")
    parser.add_option("--zookeeper-path", help="zookeeper path, i.e.: /bolt",
                      default="/bolt", action="store", dest="zk_path")
    return parser

def validate_options(options, parser):
    if not options.config:
        parser.error("need to specify config file")

def tar_file_list(dir_list, skip_list):
    if dir_list is None: raise Exception("Invalid dir_list for taring")
    if skip_list is None: raise Exception("Invalid skip_list for taring")

    retval = []
    real_tar_dirs = []
    skip_directories = []
    skip_files = []
    for d in skip_list:
        if not os.path.exists(d): continue

        f = os.path.abspath(d)
        if os.path.isdir(f):
            skip_directories.append(f)
        elif os.path.isfile(f):
            skip_files.append(f)

    for d in dir_list:
        if not os.path.exists(d):
            print "File for comrpessing: ", d, " does not exist"
            continue

        f = os.path.abspath(d)
        if os.path.dirname(f) in skip_directories: continue
        if f in skip_files: continue

        if os.path.isfile(f):
            retval.append(f)
        elif os.path.isdir(f):
            real_tar_dirs.append(f)

    for traverse_dir in real_tar_dirs:
        if traverse_dir in skip_directories: continue
        for root, dirs, files in os.walk(traverse_dir):
            checked_dir = False
            for file in files:
                file = os.path.abspath(file)
                if not checked_dir:
                    checked_dir = True
                    if os.path.dirname(file) in skip_directories: break;

                print "Examining: ", file
                if not file in skip_files: retval.append(file)

    return retval

def build_thrift_request(request):
    tar_files = tar_file_list(request["compress_files"],
                              request["exclude_compress_files"])

    tar_name = os.path.abspath("concord_slug.tar.gz")
    if os.path.exists(tar_name): print tar_name, " exists! overriding"
    if len(tar_files) == 0: raise Exception("Nothing to tar.gz :'(")

    with tarfile.open(tar_name, "w:gz") as tar:
        for name in tar_files:
            print "Adding tarfile: ", name
            tar.add(name)

    print "Reading slug into memory"
    slug = open(tar_name, "rb").read()
    if os.path.exists(tar_name):
        print "Removing slug", tar_name
        os.remove(tar_name)

    print "Making request object"
    req = BoltComputationRequest()
    req.name = request["computation_name"]
    req.instances = request["instances"]
    req.cpus = request["cpus"]
    req.mem = request["mem"]
    req.disk = request["disk"]
    req.slug = slug
    req.forceUpdateBinary = request["update_binary"]
    req.taskHelper = ExecutorTaskInfoHelper()
    req.taskHelper.execName = request["executable_name"]
    req.taskHelper.clientArguments = request["executable_arguments"]
    req.taskHelper.environmentExtra = request["environment_variables"]
    req.taskHelper.frameworkLoggingLevel = request["framework_logging_level"]
    req.taskHelper.frameworkVModule = request["framework_v_module"]
    req.taskHelper.folder = os.path.dirname(
        os.path.abspath(request["executable_name"]))
    req.taskHelper.user = request["execute_as_user"]
    return req

def register(request):
    req = build_thrift_request(request)
    print "Getting master ip from zookeeper"
    (addr, port) = get_zookeeper_master_ip(
        request["zookeeper_hosts"], request["zookeeper_path"]).split(":")
    print "Initiating connection to scheduler"
    cli = get_sched_service_client(addr,int(port))
    print "Sending request to scheduler"
    cli.deployComputation(req)
    print "Done sending request to server"
    print "Verify with the mesos host: ", addr, " that the service is running"


def main():
    parser = generate_options()
    (options, args) = parser.parse_args()
    validate_options(options,parser)
    os.chdir(os.path.dirname(os.path.abspath(options.config)))
    register(parseFile(options.config, parser))

if __name__ == "__main__":
    main()
