import os
import sys
import argparse
from dcos import util

ON_DCOS = True if 'DCOS_SSL_VERIFY' in os.environ else False

BASE_USAGE = """concord [-h] [--info] [--version] subcommand [suboptions [suboptions ...]]"""

POSITIONAL = """
\npositional arguments: (one of)
  deploy\tDeploy concord operators
  runway\tDeploy prebuilt operators/connectors from runway repository
  kill\t\tLaunch interactive session to browse and kill operators
  graph\t\tCreate a graphical representation of the current topology
  marathon\tCreate a marathon application from given parameters
  config\tSet global CLI defaults
"""

USAGE = BASE_USAGE + POSITIONAL

def dcos_options(parser):
    """Adds dcos specific options to the initial argument parser"""
    if ON_DCOS is not True:
        return
    parser.add_argument('--config-schema', action='store_true', help='Schema Validator')

def dcos_config_data():
    """Fetches config data from dcos schema"""
    config_data = {}
    config_data['zookeeper_hosts'] = util.get_config().get('concord.zookeeper_hosts')
    config_data['zookeeper_path'] = util.get_config().get('concord.zk_path')
    return config_data
