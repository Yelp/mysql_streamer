FROM	centos:latest

RUN     rpm -Uhv https://www.percona.com/redir/downloads/percona-release/redhat/0.0-1/percona-release-0.0-1.x86_64.rpm

RUN		yum install -y Percona-Server-server-56 Percona-Server-client-56 Percona-Server-shared-56


ADD     ./startup.sh /code/startup.sh
ADD     ./my.cnf     /etc/my.cnf
RUN     chown mysql /etc/my.cnf
RUN     bash /code/startup.sh

USER    mysql
# Convert 48bit hostname hash to a 32bit int
CMD     mysqld --server-id=$((0x$(hostname) >> 16))

USER    root
RUN     yum update; yum install -y MySQL-python

# Setup mysqld
ADD     . /code/
RUN     cd /code; bash setup.sh

USER    mysql
WORKDIR /code
CMD     bash start.sh
EXPOSE  3306
