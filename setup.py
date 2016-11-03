#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from setuptools import find_packages
from setuptools import setup

setup(
    name='replication_handler',
    version='0.1.0',
    description='',
    author='BAM',
    author_email='bam@yelp.com',
    url='https://github.com/Yelp/mysql_streamer',
    packages=find_packages(exclude=['tests']),
    setup_requires=['setuptools'],
    install_requires=[
        'mysql-replication',
        'MySQL-python',
        'PyMySQL',
        'sqlparse',
        'vmprof',
    ],
    license='Copyright Yelp 2015, All Rights Reserved'
)
