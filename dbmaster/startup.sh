#!/bin/bash

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
