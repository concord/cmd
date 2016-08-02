#!/usr/bin/env python
import os #environ
import sys
import concord.kill_task, concord.print_graph, concord.runway
import concord.marathon, concord.deploy, concord.config
from concord import constants
from argparse import ArgumentParser
from pkg_resources import resource_string
from dcos_utils import *

def config_err():
    print "Cannot use 'concord config' command in DC/OS mode"

SUBPROGRAMS = {
    'config': concord.config.main if ON_DCOS is False else config_err,
    'runway': concord.runway.main,
    'kill': concord.kill_task.main,
    'graph': concord.print_graph.main,
    'marathon': concord.marathon.main,
    'deploy': concord.deploy.main    
}

def generate_options():
    parser = ArgumentParser(prog='concord', add_help=False, usage=USAGE)
    parser.add_argument('-h', '--help', action='store_true', help='Show this message')
    parser.add_argument('--info', action='store_true', help='Information about this cli')
    parser.add_argument('--version', action='store_true', help='Version info')
    dcos_options(parser)
    return parser

def run(options, program_options):
    # Pass along -h if detected but use after subcommand
    if options.help is True:
        program_options.append('-h')
        options.help = False

    # Attempt to run subcommand, passing along options
    program = program_options[0]
    if program in SUBPROGRAMS.keys():
        sys.argv = program_options
        SUBPROGRAMS[program]()
    else:
        print "'%s' is not a recognized command" % program

def main():
    parser = generate_options()
    options, program_options = parser.parse_known_args()

    # Remove 'concord' string if on DC/OS
    if ON_DCOS is True:
        program_options = program_options[1:]

    if options.info is True:
        print 'Deploy and manage Concord operators'
    elif options.version is True:
        print 'Concord CLI version %s' % constants.version
    elif ON_DCOS is True and options.config_schema is True:
        print resource_string(__name__, '/resources/config_schema.json')
    elif len(program_options) > 0:
        run(options, program_options)
    elif options.help is True:
        parser.print_help()

if __name__ == '__main__':
    main()
