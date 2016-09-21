#
# Run mysqld on the default port, with my.cnf settings
#

FROM    centos:latest
RUN     rpm -Uhv https://www.percona.com/redir/downloads/percona-release/redhat/0.0-1/percona-release-0.0-1.x86_64.rpm

RUN     yum install -y Percona-Server-server-56 Percona-Server-client-56 Percona-Server-shared-56

ADD     ./startup.sh /opt/startup.sh
ADD     ./my.cnf     /etc/my.cnf
RUN     chown mysql /etc/my.cnf

# Starts mysqld_safe, grants mysql privilidges
RUN     bash /opt/startup.sh

USER    mysql
# Convert 48bit hostname hash to a 32bit int
CMD     mysqld --server-id=$((0x$(hostname) >> 16))

USER    root
RUN     apt-get update; apt-get install -y python-mysqldb

ADD     . /code/

# Create our database and user
RUN     cd /code; bash setup.sh

USER    mysql
WORKDIR /code
CMD     bash start.sh
EXPOSE  3306
