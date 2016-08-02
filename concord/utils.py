import os
import json
import logging
import urllib2
from concord.dcos_utils import *

CONCORD_FILENAME = '.concord.cfg'

CONCORD_DEFAULTS = { 'zookeeper_path' : '/concord',
                     'zookeeper_hosts' : 'localhost:2181',
                     'runway_repository' : 'https://github.com/concord/runway/master/' }

def build_logger(module_name, fmt_string=None):
    if fmt_string is not None:
        logging.basicConfig(format=fmt_string)
    else:
        logging.basicConfig()
    logger = logging.getLogger(module_name)
    level = "INFO" if 'CONCORD_LOG_LEVEL' not in os.environ else \
            os.environ['CONCORD_LOG_LEVEL']
    if level == 'DEBUG':
        logger.setLevel(logging.DEBUG)
    elif level == 'WARNING':
        logger.setLevel(logging.WARNING)
    elif level == 'ERROR':
        logger.setLevel(logging.ERROR)
    elif level == 'CRITICAL':
        logger.setLevel(logging.CRITICAL)
    else:
        logger.setLevel(logging.INFO)
    return logger

logger = build_logger('cmd.utils')

def docker_metadata(url):
    """Recursivly builds array of 'results' from responses from dockerhub HTTP requests"""
    def raw_impl(url):
        try:
            data = urllib2.urlopen(url).read()
        except Exception as e:
            logger.critical("Failed to connect to dockerhub, cannot fetch verison metadata " + url)
            raise e
        data = json.loads(data)
        next_url = data['next']
        return data['results'] if next_url is None else data['results'] + raw_impl(next_url)
    return raw_impl(url)

def find_config(src, config_file):
    """ recursively searches .. until it finds a file named config_file
    will return None in the case of no matches or the abspath if found"""
    filepath = os.path.join(src, config_file)
    if os.path.isfile(filepath):
        return filepath
    elif src == '/':
        return None
    else:
        return find_config(os.path.dirname(src), config_file)

def fetch_config_opts(src, config_file):
    if ON_DCOS is True:
        return dcos_config_data()

    location = find_config(os.getcwd(), CONCORD_FILENAME)
    config_data = {}
    if location is None:
        config_data = CONCORD_DEFAULTS
    else:
        with open(location, 'r') as data_file:
            config_data = json.load(data_file)
    return config_data

def default_options(opts):
    config_data = fetch_config_opts(os.getcwd(), CONCORD_FILENAME)
    opts_methods = dir(opts)
    if 'zookeeper' in opts_methods:
        opts.zookeeper = config_data['zookeeper_hosts'] if opts.zookeeper is None \
                         else opts.zookeeper
    if 'zk_path' in opts_methods:
        opts.zk_path = config_data['zookeeper_path'] if opts.zk_path is None \
                        else opts.zk_path
    if 'repository' in opts_methods:
        opts.repository = config_data['runway_repository'] if opts.repository is None \
                          else opts.repository

def default_manifest_options(manifest, all_options):
    config_data = fetch_config_opts(os.getcwd(), CONCORD_FILENAME)
    for option, value in config_data.iteritems():
        if option in all_options and option not in manifest:
            manifest[option] = value

def human_readable_units(num, suffix='B'):
    for unit in ['','K','M','G','T','P','E','Z']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Y', suffix)
