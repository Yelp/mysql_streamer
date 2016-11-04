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
# This script comes from the base image. It starts mysqld in the background
bash /code/startup.sh

# Create our database and users
cat setup.sql | mysql

# Create tables
cat tables/business.sql | mysql yelp
cat tables/heartbeat.sql | mysql yelp_heartbeat

mysqladmin shutdown
