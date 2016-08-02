# cmd

**Concord Command Line Interface**

## Building

This repository is organized to work with the pip python package manager.
In order to create the package you will need to generate thrift definitions 
located in the main concord repo. Run the build_thrift script from the
root of the concord project:
```
$ cd ~/workspace/concord/ && ./configure.py --thrift
```

To run any tests associated with this package:
```
$ python -m unittest discover tests
```

## Installing the CLI

Install this CLI via pip like so:
```
$ pip install concord
```

How to use the CLI:
```
$ concord -h
usage: concord [-h] [--info] [--version] subcommand [suboptions [suboptions ...]]

positional arguments: (one of)
  deploy        Deploy concord operators
  runway        Deploy prebuilt operators/connectors from runway repository
  kill          Launch interactive session to browse and kill operators
  graph         Create a graphical representation of the current topology
  marathon      Create a marathon application from given parameters
  config        Set global CLI defaults

optional arguments:
  -h, --help  Show this message
  --info      Information about this cli
  --version   Version info
```

For a more detailed look on this CLI and the supported commands check out our
official [CLI docs here](http://concord.io/docs/tutorials/cli.html).


## DC/OS CLI

This CLI is meant to also be used through the DC/OS CLI, so certain design 
decisions were made to keep up with this spec. At the time of this writing
the concord DC/OS CLI is **not** supported, even though a published version 
may exist on Pypi. If you are using our DC/OS install and are wondering how to
manage your concord topology, check out the official [DC/OS install 
information here](http://concord.io/docs/tutorials/dcos_install.html).
