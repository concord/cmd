#!/usr/bin/env python
import os
import pip
from setuptools import setup, find_packages
from pip.req import parse_requirements
from subprocess import call



# e.g. ['django==1.5.1', 'mezzanine==1.4.6']
reqs = [
    'graphviz==0.4.3',
    'kazoo==2.0',
    'thrift==0.9.2',
    'trace2html==0.2.1'
]


setup(version='0.1.1',
      name='concord',
      description='python concord command line tools',
      scripts=['concord'],
      author='concord systems',
      author_email='hello@concord.io',
      packages=find_packages('.'),
      url='http://concord.io',
      install_requires=reqs,
      test_suite="tests",
)
