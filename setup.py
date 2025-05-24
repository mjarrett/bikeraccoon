#!/usr/bin/env python

from distutils.core import setup
from setuptools import find_packages
setup(name='bikeraccoon',
      version='1.0',
      description='Python utilities for accessing the raccoon.bike bikeshare API',
      author='Mike Jarrett',
      author_email='msjarrett@gmail.com',
      url='raccoon.bike',
      packages = find_packages(include=['bikeraccoon', 'bikeraccoon.*']),
      install_requires=['pandas','requires','duckdb','pyarrow','dash','flask'],
     )
