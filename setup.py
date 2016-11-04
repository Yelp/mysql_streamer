#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright 2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
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
