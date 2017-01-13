# MySQL Streamer


What is it?
-----------
MySQLStreamer is a database change data capture and publish system.
It’s responsible for capturing each individual database change,
enveloping them into messages and publishing to Kafka.


[Read More](https://engineeringblog.yelp.com/2016/08/streaming-mysql-tables-in-real-time-to-kafka.html)


How to download
---------------
```
git clone git@github.com:Yelp/mysql_streamer.git
```


Requirements
------------
For Ubuntu 14.04
```
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
   git-core \
   mysql-client-5.5 \
   tox
```


Tests
-----
Running unit tests
```
make -f Makefile-opensource test
```


Running integration tests
```
make -f Makefile-opensource itest
```


Demo
----
```
make -f Makefile-opensource interactive-streamer
```
![MySQL Streamer Demo](MySQLStreamerWorking.gif)

In the bottom pane, you can execute any mysql statement on a barebones structure we’ve set up. Try creating a table and inserting into it to see it be sent (seen in the replication handler logs in the top right), and then received messages in the top left (seen in the kafka tailer’s logs). The received messages will probably look a little funky - this is because they’re avro-encoded.


Disclaimer
-------
We're still in the process of setting up this service as a stand-alone. There may be additional work required to run MySQL Streamer instances and integrate with other applications.


License
-------
MySQL Streamer is licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0


Contributing
------------
Everyone is encouraged to contribute to MySQL Streamer by forking the Github repository and making a pull request or opening an issue.
