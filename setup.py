#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import find_packages
from setuptools import setup

setup(
    name='replication_handler',
    version='0.1.0',
    description='',
    author='BAM',
    author_email='bam@yelp.com',
    url='https://gitweb.yelpcorp.com/?p=replication_handler.git',
    packages=find_packages(exclude=['tests']),
    setup_requires=['setuptools'],
    install_requires=[
        'mysql-replication',
        'MySQL-python',
        'PyMySQL'
    ],
    license='Copyright Yelp 2015, All Rights Reserved'
)
