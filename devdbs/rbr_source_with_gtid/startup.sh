#!/bin/bash
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

set -e

/usr/bin/mysqld_safe &

# Wait for mysqld to start
delay=1
timeout=5
while ! mysqladmin ping >/dev/null 2>&1; do
    timeout=$(expr $timeout - $delay)

    if [ $timeout -eq 0 ]; then
        echo "Timeout error occurred trying to start MySQL Daemon."
        exit 1
    fi
    sleep $delay
done

echo "GRANT ALL ON *.* TO mysql@'169.254.%.%' WITH GRANT OPTION; FLUSH PRIVILEGES" | mysql
echo "GRANT ALL ON *.* TO mysql@'localhost' WITH GRANT OPTION; FLUSH PRIVILEGES" | mysql
