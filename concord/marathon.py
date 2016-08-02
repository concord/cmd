import os
import time
import json
from argparse import ArgumentParser
from terminaltables import AsciiTable
from concord.utils import docker_metadata

DEFAULT_MARATHON = {
    "constraints": [ [ "hostname", "UNIQUE" ] ],
    "ports":[0],
    "upgradeStrategy": {
        "minimunHealthCapacity": 0.5,
        "maximumOverCapacity":0.5
    },
    "container": {
        "type": "DOCKER",
        "docker": {
            "image": "concord/scheduler:0.3.16.4",
            "network": "HOST"
        }
    }
}

DEFAULT_MARATHON_FILENAME = "concord_marathon_{0}.json".format(time.strftime("%H:%M:%S"))
DEFAULT_SERVICE_DISCOVERY = "$(/sbin/ifconfig eth0 | grep 'inet addr:' | cut -d: -f2 | awk '{ print $1}')"
OPENDNS_SERVICE_DISCOVERY = "$(dig +short myip.opendns.com @resolver1.opendns.com)"
DOCKERHUB_CONCORD_TAG_URL = "https://hub.docker.com/v2/repositories/concord/scheduler/tags/"

def generate_options():
    parser = ArgumentParser()
    parser.add_argument("-C", "--concord_zookeeper", default="zk://localhost:2181/concord"
                        ,help="i.e. zk://1.2.3.4:2181,2.2.2.2:2181/concord", action="store")
    parser.add_argument("-M", "--mesos_zookeeper", default="zk://localhost:2181/mesos"
                        ,help="i.e. zk://1.2.3.4:2181,2.2.2.2:2181/mesos", action="store")
    parser.add_argument("-n", "--framework_name"
                        ,help="Name to give to Concord Scheduler framework"
                        ,default="concord-scheduler", action="store")
    parser.add_argument("-l", "--locate_publicip", dest='locate', action='store_true',
                        help="""Use openDNS to automatically resolve public ip.
                        If this is not set then ifconfig will be used to
                        query for a public ip. This may not work if the selected
                        machine is behind a router that uses NAT.""")
    parser.add_argument("-c", "--cpu_shares"
                        ,default=1, type=int, action="store")
    parser.add_argument("-m", "--mem_allocated"
                        ,default=1024, type=int, action="store")
    parser.add_argument("-i", "--instances", help="Instances of Concord Scheduler"
                        ,default=1, type=int, action="store")
    parser.add_argument("-o", "--output_destination"
                        ,default="./", action="store")
    parser.set_defaults(locate=False)
    return parser

def build_cmd(options):
    service_discovery = DEFAULT_SERVICE_DISCOVERY if options.locate is False \
                        else OPENDNS_SERVICE_DISCOVERY
    return ("java "
            "-XX:+UseConcMarkSweepGC "
            "-XX:+CMSParallelRemarkEnabled "
            "-XX:+HeapDumpOnOutOfMemoryError "
            "-XX:HeapDumpPath=/tmp/concord_heap_dump_{5}.hprof "
            "-Dsun.net.inetaddr.ttl=60 "
            "-Djava.library.path=/usr/lib "
            "-DMESOS_NATIVE_JAVA_LIBRARY=/usr/lib/libmesos.so "
            "$JAVA_OPTS -cp /usr/local/share/concord/concord.jar"
            " com.concord.scheduler.Scheduler "
            "--listen 0.0.0.0:$PORT0 "
            "--advertise-external {3} "
            "--advertise-internal {4} "
            "--concord-master {0} "
            "--mesos-master {1} "
            "--framework-name {2}").format(options.concord_zookeeper,
                                           options.mesos_zookeeper,
                                           options.framework_name.lower(),
                                           service_discovery,
                                           DEFAULT_SERVICE_DISCOVERY,
                                           time.strftime("%s"))

def build_marathon(options):
    # Assemble JSON file
    def choose_version():
        dockertag_data = docker_metadata(DOCKERHUB_CONCORD_TAG_URL)
        all_versions = map(lambda x: x['name'], dockertag_data)
        assert(len(all_versions) > 0)
        data_rows = zip(xrange(1, len(all_versions) + 1), all_versions)
        data_rows = map(lambda x: [str(x[0]), x[1]], data_rows)
        data_rows.insert(0, ['Idx', 'Version'])
        return data_rows

    versions = choose_version()
    print "Choose a scheduler runtime version: "
    print AsciiTable(versions).table
    sel = int(raw_input("Selection: "))

    marathon_opts = DEFAULT_MARATHON.copy()
    marathon_opts["id"] = options.framework_name.lower()
    marathon_opts["cmd"] = build_cmd(options)
    marathon_opts["cpus"] = options.cpu_shares
    marathon_opts["mem"] = options.mem_allocated
    marathon_opts["instances"] = options.instances
    marathon_opts["container"]["docker"]["image"] = "concord/scheduler:{}".format(versions[sel][1])
    return json.dumps(marathon_opts, indent=2)

def write_marathon(marathon_json, dest_path, parser):
    basename = os.path.basename(dest_path)
    if '.' in basename:
        filename, ext = basename.split('.')
        if ext != 'json':
            parser.error("Output filename must end in '.json'")
    else:
        dest_path += DEFAULT_MARATHON_FILENAME

    with open(dest_path, 'w') as data_file:
        data_file.write(marathon_json)
        print "Successfully wrote Marathon file to: " + dest_path

def main():
    parser = generate_options()
    options = parser.parse_args()
    marathon_json = build_marathon(options)
    write_marathon(marathon_json, options.output_destination, parser)

if __name__ == "__main__":
    main()
