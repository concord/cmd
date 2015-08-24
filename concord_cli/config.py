import os
import json
import argparse
from functools import partial

CONFIG_FILENAME = '.concord.cfg'

CONCORD_DEFAULTS = { 'zookeeper_path' : '/concord',
                     'zookeeper_hosts' : 'localhost:2181',
                     'scheduler_address' : 'localhost:11219' }

def generate_options():
    parser = argparse.ArgumentParser()
    subparser = parser.add_subparsers()

    # Init command takes one optional argument 'parameters'
    init_parser = subparser.add_parser('init', help='Create concord cli defaults')
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

def find_config(src):
    """ recursively searches .. until it finds a file named CONFIG_FILENAME
    will return None in the case of no matches or the abspath if found"""
    filepath = os.path.join(src, CONFIG_FILENAME)
    if os.path.isfile(filepath):
        return filepath
    elif src == '/':
        return None
    else:
        return find_config(os.path.dirname(src))

def fetch_config(location, callback):
    filepath = find_config(location)
    if filepath is None:
        print 'Could not find a configuration file'
        return

    with open(filepath, 'r') as datafile:
        config_data = json.load(datafile)

    callback(config_data, filepath)

def validate_config_params(config_params):
    """ verifys that any user given keys are valid defaults"""
    valid_config_keys = CONCORD_DEFAULTS.keys()
    def validate(key):
        return key in valid_config_keys

    return all(map(validate, config_params.keys()))

def pairs_todict(kvpair_list):
    """ Transform list -> dict i.e. [a=b, c=d] -> {'a':'b', 'c':'d'}"""
    return { k:v for k, v in map(lambda c: c.split('='), kvpair_list) }

def write_defaults(new_defaults, current, writepath):
    if new_defaults is not None:
        if not validate_config_params(new_defaults):
            raise Exception("Attempting to set unrecognized parameter")
        current.update(new_defaults)

    with open(writepath , 'w') as outfile:
        json.dump(current, outfile)

def init(new_defaults):
    filepath = find_config(os.getcwd())
    if filepath is not None:
        print '%s already exists in %s' \
            % (CONFIG_FILENAME, os.path.realpath(filepath))
        inpt = raw_input('Overwrite? [y|n] ')
        if inpt != "y":
            print 'Exiting'
            return
    else:
        filepath = os.path.join(os.getcwd(), CONFIG_FILENAME)

    # Append defaults with user params, user params take precedence
    print 'Creating %s with defaults...' % CONFIG_FILENAME
    write_defaults(new_defaults, CONCORD_DEFAULTS.copy(), filepath)

def show(config_data, filepath):
    def pretty_print(kvpair):
        print '%s=%s' % kvpair
    print 'Concord configuration data: '
    map(pretty_print, config_data.iteritems())

def main():
    parser = generate_options()
    options = parser.parse_args()
    if options.which == 'show':
        fetch_config(os.getcwd(), show)
    else:
        argsdict = None if options.parameters is None else \
                   pairs_todict(options.parameters)
        if options.which == 'init':
            init(argsdict)
        elif options.which == 'set':
            fetch_config(os.getcwd(), partial(write_defaults, argsdict))

if __name__ == '__main__':
    main()
