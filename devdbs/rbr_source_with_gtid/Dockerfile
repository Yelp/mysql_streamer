FROM    centos:latest
RUN     rpm -Uhv https://www.percona.com/redir/downloads/percona-release/redhat/0.0-1/percona-release-0.0-1.x86_64.rpm

RUN     yum install -y Percona-Server-server-56 Percona-Server-client-56 Percona-Server-shared-56

ADD     . /code/
ADD     my.cnf /etc/my.cnf
RUN     chown mysql /etc/my.cnf
RUN     bash /code/startup.sh
RUN     cd code; bash setup.sh

USER    mysql
# Convert 48bit hostname hash to a 32bit int
CMD     [ \
    "bash", "-c", \
    "exec mysqld --server-id=$((0x$(hostname) >> 16))" \
]
