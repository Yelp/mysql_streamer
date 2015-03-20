#
# Run mysqld on the default port, with my.cnf settings
#

FROM    docker-dev.yelpcorp.com/lucid_yelp

ENV     DEBIAN_FRONTEND noninteractive

RUN     apt-get update -q
RUN     apt-get install -y percona-server-server-5.6

ADD     ./startup.sh /opt/startup.sh
ADD     ./my.cnf     /etc/my.cnf
RUN     chown mysql /etc/my.cnf
RUN     bash /opt/startup.sh

USER    mysql
# Convert 48bit hostname hash to a 32bit int
CMD     mysqld --server-id=$((0x$(hostname) >> 16))

USER    root
RUN     apt-get update; apt-get install -y python-mysqldb

# Setup mysqld
ADD     . /code/
RUN     cd /code; bash setup.sh

USER    mysql
WORKDIR /code
CMD     bash start.sh
EXPOSE  3306
