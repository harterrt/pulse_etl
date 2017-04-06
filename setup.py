#!/usr/bin/env python

from setuptools import setup, find_packages
from distutils.core import setup

setup(name='pulse_etl',
      version='0.1',
      description='Testpilot ETL for Pulse',
      author='Ryan Harter (:harter)',
      author_email='harterrt@mozilla.com',
      url='https://github.com/harterrt/pulse_etl.git',
      packages=find_packages(exclude=['tests']),
)
