#!/usr/bin/env python
import os
import pip
from setuptools import setup, find_packages
from pip.req import parse_requirements
from subprocess import call

reqs = parse_requirements("requirements.txt",
                          session=pip.download.PipSession())

install_reqs = parse_requirements(req, session=pip.download.PipSession())

setup(version='0.1.4',
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
