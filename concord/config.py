import os
import sys
import json
import argparse
from functools import partial
from concord.utils import *
from concord.functional_utils import *

def generate_options():
    parser = argparse.ArgumentParser()
    subparser = parser.add_subparsers()

    # Init command takes two optional arguments 'location' and 'parameters'
    init_parser = subparser.add_parser('init', help='Create concord cli defaults')
    init_parser.add_argument('-c', '--config', default="~",
                             help="location of settings file, defaults to ~")
    init_parser.add_argument('-p', '--parameters', nargs='+', type=str,
                             help='Custom cli defaults, i.e. k1=v1 k2=v2')
    init_parser.set_defaults(which='init')

    # Set command takes one positional argument 'parameters'
    set_parser = subparser.add_parser('set', help='Add/Modify concord cli defaults')
    set_parser.add_argument('parameters', nargs='+', type=str,
                            help='Custom cli defaults, i.e. k1=v1 k2=v2')
    set_parser.set_defaults(which='set')

    show_parser = subparser.add_parser('show', help='Show concord cli defaults')
    show_parser.set_defaults(which='show')

    return parser

def fetch_config(location, callback, default_path=None):
    """ Attempts to find file starting at location. Upon success the file
    will be opened and contents passed into callback"""
    filepath = find_config(location, CONCORD_FILENAME)
    if filepath is None:
        print 'While searching recursively upwards (starting from %s), ' \
            'a configuration file could not be found...' % location
        return default_path

    with open(filepath, 'r') as datafile:
        config_data = json.load(datafile)

    return callback(config_data, filepath)

def write_defaults(new_defaults, current, writepath):
    """ Writes 'new_defaults' to disk at 'writepath' using 'current' for
    default valies that 'new_defaults' may be omitting"""
    def validate_config_params(config_params):
        valid_config_keys = CONCORD_DEFAULTS.keys()
        def validate(key):
            return key in valid_config_keys
        return all(map(validate, config_params.keys()))

    if new_defaults is not None:
        if not validate_config_params(new_defaults):
            raise Exception("Attempting to set unrecognized parameter")
        current.update(new_defaults)

    with open(writepath , 'w') as outfile:
        json.dump(current, outfile)

def init(location, new_defaults=None):
    """ Creates config_file in location, asking user to overwrite if already
    exists. Then prompts user for default values and writes to disk if -p
    option has not been used"""
    def duplicate_prompt(config_data, fpath):
        print 'config_file already exits in', fpath
        inpt = raw_input('Overwrite? [y|n] ')
        return fpath if inpt == "y" else sys.exit(1)

    default_path = os.path.join(os.path.expanduser(location), CONCORD_FILENAME)
    filepath = fetch_config(os.getcwd(), duplicate_prompt, default_path)

    if new_defaults is None:
        def prompt_default(k, v):
            inpt = raw_input('-- for key "%s" value will be ["%s"]: ' % (k,v))
            return v if inpt == "" else inpt

        print '\nCreating %s, select or overwrite default values: ' % filepath
        print 'When prompted, hit enter to take the default or enter a new value'
        new_defaults = { k:prompt_default(k,v) for k,v in CONCORD_DEFAULTS.iteritems() }

    write_defaults(new_defaults, CONCORD_DEFAULTS.copy(), filepath)
    print '\nWrite successful, these are the new settings: '
    fetch_config(filepath, show)

def show(config_data, filepath):
    """ Prints the contents of the config_file to stdout"""
    def pretty_print(kvpair):
        print '%s=%s' % kvpair
    print 'Concord configuration data: ', filepath
    map(pretty_print, config_data.iteritems())

def main():
    parser = generate_options()
    options = parser.parse_args()
    if options.which == 'show':
        fetch_config(os.getcwd(), show)
    else:
        argsdict = pairs_todict(options.parameters)
        if options.which == 'init':
            init(options.config, argsdict)
        elif options.which == 'set':
            fetch_config(os.getcwd(), partial(write_defaults, argsdict))

if __name__ == '__main__':
    main()
