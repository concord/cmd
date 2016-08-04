#!/usr/bin/env python
"""
Concord Runway
For use with source here: https://github.com/concord/runway
Official build artifacts can be found here: https://hub.docker.com/u/concord/
"""

import os
import sys
import uuid
import argparse
import urllib2
import json
import tempfile
from urlparse import (urlparse, urljoin)
from datetime import datetime
from terminaltables import AsciiTable
from concord.deploy import (parseFile, register)
from concord.functional_utils import (find_first_of, flat_map)
from concord.utils import (build_logger, default_options, docker_metadata)
from concord.utils import CONCORD_DEFAULTS

logger = build_logger('cmd.runway')

MAIN_METADATA = (['Index', 'Connector', 'Description', 'Last Updated', 'Pull Count', 'Star Count'],
                ['operator_name', 'description', 'last_updated', 'pull_count', 'star_count'])
TAG_METADATA = (['Index', 'Version', 'Size(GB)', 'Last Updated'],
                ['name', 'full_size', 'last_updated'])

DOCKERHUB_ENDPOINT = "https://hub.docker.com/v2/repositories/"
NON_ALLOWED_KEYS = ['docker_container', 'executable_name', 'zookeeper_hosts', 'zookeeper_path']

HELP_TXT = """Deploys preconfigured Concord connectors. Either pass a standard deploy
manifest file; pass necessary zookeeper_hosts/zookeeper_path  arguments through command line flags
; or 'concord config' stored defaults will be used"""

def generate_options():
    parser = argparse.ArgumentParser(description=HELP_TXT)
    parser.add_argument("-c", "--config", metavar="config-file", action="store",
                        help="i.e: ./src/config.json")
    parser.add_argument("-p", "--zk_path", metavar="zookeeper-path",
                        help="Path of concord topology on zk", action="store")
    parser.add_argument("-z", "--zookeeper", metavar="zookeeper-hosts",
                        help="i.e: 1.2.3.4:2181,2.3.4.5:2181", action="store")
    parser.add_argument("-r", "--repository", metavar="repo-url", action="store",
                        help="URL to a runway repository")
    return parser

def validate_enrich_metadata(metadata):
    """Using metadata stored in the runway repo, fetch additional metadata from each
    projects respective dockerhub repository. Discard packages that don't match"""
    unique_repos = set(map(lambda x: x['docker_repository'], metadata))
    docker_meta = { x:docker_metadata(DOCKERHUB_ENDPOINT + x) for x in unique_repos }

    # TODO: Remove duplicate code
    def container_name(data):
        return "{}/{}".format(data['user'], data['name'])

    def assemble_cname(elem):
        return "{}/{}".format(elem['docker_repository'], elem['docker_container'])

    def enrich_metadata(elem):
        docker_repo = docker_meta[elem['docker_repository']]
        container_info = find_first_of(lambda x: container_name(x) == assemble_cname(elem), docker_repo)
        assert(container_info != None)
        elem['last_updated'] = container_info['last_updated']
        elem['pull_count'] = container_info['pull_count']
        elem['star_count'] = container_info['star_count']
        elem['description'] = container_info['description']
        return elem

    def filter_metadata(elem):
        repo = docker_meta[elem['docker_repository']]
        tf = map(lambda x: assemble_cname(elem) == container_name(x), repo)
        return any(tf)

    runway_meta = filter(filter_metadata, metadata)
    runway_meta = map(enrich_metadata, runway_meta)
    return runway_meta

def validate_tag_metadata(connector):
    """Returns a new copy of runway_metadata, after vetting the supported versions
    array to ensure that only valid options are presented to the user"""
    dockertag_url = '{}{}/{}/tags/'.format(DOCKERHUB_ENDPOINT,
                                           connector['docker_repository'],
                                           connector['docker_container'])
    def convert_size(elem):
        new_size = '%.5f'%(elem['full_size'] / float(1 << 30))
        elem['full_size'] = new_size
        return elem

    dockertag_data = map(convert_size, docker_metadata(dockertag_url))
    return filter(lambda x: x['name'] in connector['supported_versions'], dockertag_data)

def create_ascii_table(row_data, heading, runway_data):
    """Pure method, doesn't mutate arguments, returns AsciiTable from args"""
    def format_iso8601(time):
        # eg: 2016-07-08T20:16:59.149731Z
        date_object = datetime.strptime(time, "%Y-%m-%dT%H:%M:%S.%fZ")
        return date_object.strftime("%Y/%m/%d")

    def into_row(elem_tuple):
        idx, elem = elem_tuple
        row = map(lambda x: str(elem[x]) if x != 'last_updated' else format_iso8601(elem[x]),
                  row_data)
        row.insert(0, str(idx))
        return row

    indexed_data = zip(xrange(1, len(runway_data) + 1), runway_data)
    data_rows = map(into_row, indexed_data)
    data_rows.insert(0, heading)
    return AsciiTable(data_rows).table

def present_and_select(message, table_metadata, raw_metadata):
    # Prefer print to logger for ascii table
    print message
    print create_ascii_table(table_metadata[1], table_metadata[0], raw_metadata)
    user_selection = int(raw_input("Selection: "))
    if user_selection <= 0 or user_selection > len(raw_metadata):
        logger.critical("Exiting Program: Incorrect selection parameter")
        sys.exit(1)
    return user_selection

def simple_prompt(obj):
    default_str = "None" if 'default' not in obj else obj['default']
    return raw_input("Argument:{} (default: {}) ".format(obj['flag_name'], default_str))

# Second paramters useful for unit testing this function
def collect_argument(obj, collection_fn=simple_prompt):
    """Recusively prompts until valid argument is accepted"""
    user_input = collection_fn(obj)
    if user_input == "":
        if obj['required'] is False:
            if obj['default'] is None:
                raise Exception("If required key is false default should exits")
            return obj['default']
        else:
            return collect_argument(obj)
    else:
        try:
            if obj['type'] == "number":
                user_input = float(user_input)
            elif obj['type'] == "boolean":
                user_input = bool(user_input)
        except Exception as e:
            logger.error("Could not convert value to expected type... try again")
            return collect_argument(obj)
    return user_input

def collect_args(prompt_args):
    """Method collects arguments defined in 'prompt_args' section of repo metadata"""
    def construct_flag(obj, user_input):
        dash = "--" if obj['double_dash'] is True else "-"
        return dash + obj['flag_name'] + obj['join_char'] + str(user_input)

    logger.info("""Your operator requries that you input values for command line
    arguments defined in 'propmt_args'.""")
    logger.info("Hit enter to keep the default, otherwise enter an argument.")
    new_prompt_args = []
    for obj in prompt_args:
        user_input = collect_argument(obj)
        new_prompt_args.append(construct_flag(obj, user_input))
    return new_prompt_args

def runway_deploy(connector, ver, parser, options):
    def autogen_name(conn):
        prefix = conn['operator_name'].replace(' ', '_')
        rand = '-'.join(str(uuid.uuid1()).split('-')[1:3])
        return '{}-{}'.format(prefix, rand)

    runway_manifest = {
        'cpus' : connector['default_cpus'],
        'mem' : connector['default_mem'],
        'executable_name' : '', # update below
        'computation_name' : autogen_name(connector),
        'zookeeper_hosts' : options.zookeeper,
        'zookeeper_path' : options.zk_path,
        'compress_files' : [], # Must append ./runner.bash after call to update()
        'executable_arguments' : [], # Will optionally append, depends if config file is included
        'docker_container' : '{}/{}:{}'.format(connector['docker_repository'],
                                               connector['docker_container'],
                                               ver['name'])
    }

    # If manifest given, copy options into 'runway_manifest'
    tmp_manifest_path = os.getcwd()
    if options.config is not None:
        user_manifest = {}
        with open(options.config) as data_file:
            user_manifest = json.load(data_file)
        non_valid = map(lambda x: x in NON_ALLOWED_KEYS, user_manifest.keys())
        if any(non_valid):
            parser.error("User manifest cannot contain these keys: " + str(NON_ALLOWED_KEYS))
        runway_manifest.update(user_manifest)
        tmp_manifest_path = os.path.dirname(options.config)
    elif 'prompt_args' in connector:
        runway_manifest['executable_arguments'] += collect_args(connector['prompt_args'])

    # Create runner script and use that as executable_name
    handle, runner_file = tempfile.mkstemp(dir=tmp_manifest_path, suffix='.bash')
    runner_relpath = os.path.relpath(runner_file, start=tmp_manifest_path)
    with open(runner_file, 'w') as data_file:
        data_file.write('#!/bin/bash\n')
        data_file.write('echo "Directory: $(pwd)"\n')
        data_file.write('set -ex\n')
        data_file.write('md5=$(which md5sum)\n')
        data_file.write('if [[ $md5 != "" ]]; then $md5 "$(which {})"; fi\n'.format(connector['executable_name']))
        data_file.write('exec {} "$@"\n'.format(connector['executable_name']))
    runway_manifest['executable_name'] = runner_relpath
    runway_manifest['compress_files'].append(runner_relpath)

    # Write runway manifest json data and use concord.deploy to run
    handle, tmp_manifest = tempfile.mkstemp(dir=tmp_manifest_path, suffix='.json')
    with open(tmp_manifest, 'w') as data_file:
        json.dump(runway_manifest, data_file)

    logger.debug("Writing temporary file at: " + tmp_manifest)
    logger.debug("Runway Manifest options: ")
    logger.debug(str(runway_manifest))
    try:
        register(parseFile(tmp_manifest, parser), tmp_manifest)
    except Exception as e:
        logger.critical("Error when attempting to deploy operator: " + str(e))
    finally:
        os.remove(tmp_manifest)
        os.remove(runner_file)

def construct_runway_url(repo_url):
    # TODO: Ensure url ends in / for split path to work
    def remove_tree(path):
        return '/'.join(filter(lambda x: x != 'tree', path.split('/')))
    # https://raw.githubusercontent.com/concord/runway/test/meta/repo_metadata.json
    repo_resource = urlparse(repo_url if repo_url is not None else \
                             CONCORD_DEFAULTS['runway_repository'])
    repo_resource = repo_resource._replace(netloc="raw.githubusercontent.com")
    repo_resource = repo_resource._replace(path=remove_tree(repo_resource.path))
    return urljoin(repo_resource.geturl(), 'meta/repo_metadata.json')

def main():
    parser = generate_options()
    options = parser.parse_args()
    default_options(options)
    runway_url = construct_runway_url(options.repository)

    # 1. Parse runway metadata, ensure it is matches with hub.docker.com, present list to user
    logger.info("Fetching concord runway metadata at: " + runway_url)
    repo_metadata = json.loads(urllib2.urlopen(runway_url).read())['packages']
    repo_metadata = validate_enrich_metadata(repo_metadata)
    user_selection = present_and_select("Select an operator to deploy: ",
                                        MAIN_METADATA,
                                        repo_metadata)
    selection = repo_metadata[user_selection - 1]

    # 2. Do the same for tag/version information, present a list if more then one version is up
    versions = validate_tag_metadata(selection)
    num_versions = len(versions)
    assert(num_versions > 0)
    selected_version = None
    if num_versions == 1:
        selected_version = versions[0]
        logger.info("Deploying sole version: {}".format(selected_version['name']))
    else:
        logger.info("Fetching connector container versions...")
        user_selection = present_and_select("Select a version: ",
                                            TAG_METADATA,
                                            versions)
        selected_version = versions[user_selection - 1]

    # 3. Present documentation link
    if 'documentation_link' in selection:
        logger.info("Offical Connector documentaion: " + selection['documentation_link'])
        #print urllib2.urlopen(selection['documentation_link']).read()

    # 4. Deploy operator if user gives the OK
    should_deploy = str(raw_input("Deploy operator? [Y/n] "))
    if should_deploy == 'Y':
        runway_deploy(selection, selected_version, parser, options)


if __name__ == '__main__':
    main()
