FROM ubuntu:precise
# Upgrade
RUN apt-get update && apt-get upgrade -y && apt-get dist-upgrade -y
ENV DEBIAN_FRONTEND noninteractive
RUN echo "deb http://repo.percona.com/apt precise main" >> /etc/apt/sources.list.d/percona.list
RUN echo "deb-src http://repo.percona.com/apt precise main" >> /etc/apt/sources.list.d/percona.list
RUN apt-key adv --keyserver keys.gnupg.net --recv-keys 1C4CBDCDCD2EFD2A
RUN apt-get update
RUN apt-get install -y percona-xtradb-cluster-56 qpress xtrabackup

ADD     ./startup.sh /code/startup.sh
ADD     ./my.cnf     /etc/my.cnf
RUN     chown mysql /etc/my.cnf
RUN     bash /code/startup.sh

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
