FROM ubuntu:14.04.1

ENV DEBIAN_FRONTEND noninteractive

run apt-get update && apt-get upgrade -y && apt-get install -y \
   build-essential \
   python-dev \
   libmysqlclient-dev \
   python-pkg-resources \
   python-setuptools \
   python-virtualenv \
   python-pip \
   libpq5 \
   libpq-dev \
   wget \
   language-pack-en-base \
   uuid-dev \
   git-core

run locale-gen en_US en_US.UTF-8 && dpkg-reconfigure locales

# Setup pypy
run mkdir /src
workdir /src
run wget https://bitbucket.org/pypy/pypy/downloads/pypy-2.5.0-linux64.tar.bz2 --no-check-certificate
run bunzip2 pypy-2.5.0-linux64.tar.bz2
run tar xvf pypy-2.5.0-linux64.tar
ENV PATH $PATH:/src/pypy-2.5.0-linux64/bin/
run wget https://bootstrap.pypa.io/get-pip.py --no-check-certificate
run pypy get-pip.py

run ln -s /usr/bin/gcc /usr/local/bin/cc

# Add the service code
WORKDIR /code
ADD     requirements.txt /code/requirements.txt
ADD     setup.py /code/setup.py
RUN     virtualenv -p pypy /code/virtualenv_run
RUN     /code/virtualenv_run/bin/pip install \
            -i https://pypi-dev.yelpcorp.com/simple \
            -r /code/requirements.txt

ADD     . /code

RUN useradd batch
RUN chown -R batch /code
USER batch

# Share the logging directory as a volume
RUN     mkdir /tmp/logs
VOLUME  /tmp/logs

WORKDIR /code
ENV     BASEPATH /code
CMD /code/virtualenv_run/bin/pypy /code/replication_handler/batch/parse_replication_stream.py -v --no-notification
