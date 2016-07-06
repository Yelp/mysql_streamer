docker build -t replication-handler-dev-bowu .
Sending build context to Docker daemon 557.1 kBSending build context to Docker daemon 1.114 MBSending build context to Docker daemon 1.671 MBSending build context to Docker daemon 2.228 MBSending build context to Docker daemon 2.785 MBSending build context to Docker daemon 3.342 MBSending build context to Docker daemon 3.899 MBSending build context to Docker daemon 4.456 MBSending build context to Docker daemon 5.014 MBSending build context to Docker daemon 5.014 MB
Step 1 : FROM ubuntu:14.04.1
 ---> ab1bd63e0321
Step 2 : ENV DEBIAN_FRONTEND noninteractive
 ---> Using cache
 ---> 0e1cba0f23ca
Step 3 : RUN apt-get update && apt-get upgrade -y && apt-get install -y    build-essential    python-dev    libmysqlclient-dev    python-pkg-resources    python-setuptools    python-virtualenv    python-pip    libpq5    libpq-dev    wget    language-pack-en-base    uuid-dev    git-core
 ---> Using cache
 ---> 122a1a97b416
Step 4 : RUN locale-gen en_US en_US.UTF-8 && dpkg-reconfigure locales
 ---> Using cache
 ---> 503f0b52b331
Step 5 : RUN mkdir /src
 ---> Using cache
 ---> 18bed2bed760
Step 6 : WORKDIR /src
 ---> Using cache
 ---> 363dc9088109
Step 7 : RUN wget https://bitbucket.org/pypy/pypy/downloads/pypy-5.1.0-linux64.tar.bz2 --no-check-certificate
 ---> Using cache
 ---> ba56389bd9b1
Step 8 : RUN bunzip2 pypy-5.1.0-linux64.tar.bz2
 ---> Using cache
 ---> be29b0c55a34
Step 9 : RUN tar xvf pypy-5.1.0-linux64.tar
 ---> Using cache
 ---> 849d05e39ab8
Step 10 : ENV PATH $PATH:/src/pypy-5.1.0-linux64/bin/
 ---> Using cache
 ---> 152befa9b7eb
Step 11 : RUN wget https://bootstrap.pypa.io/get-pip.py --no-check-certificate
 ---> Using cache
 ---> b6042b1f9712
Step 12 : RUN pypy get-pip.py
 ---> Using cache
 ---> e3ac14c20f19
Step 13 : RUN ln -s /usr/bin/gcc /usr/local/bin/cc
 ---> Using cache
 ---> 7cc2c02167ef
Step 14 : RUN wget -O /usr/local/bin/dumb-init https://github.com/Yelp/dumb-init/releases/download/v1.0.1/dumb-init_1.0.1_amd64
 ---> Using cache
 ---> d57d9593ac80
Step 15 : RUN chmod +x /usr/local/bin/dumb-init
 ---> Using cache
 ---> 1a1a3d8ce0d3
Step 16 : WORKDIR /code
 ---> Using cache
 ---> 6caddf81d438
Step 17 : ADD requirements.txt /code/requirements.txt
 ---> Using cache
 ---> a64137d51342
Step 18 : ADD setup.py /code/setup.py
 ---> Using cache
 ---> 1b9977989caa
Step 19 : RUN virtualenv -p pypy /code/virtualenv_run
 ---> Using cache
 ---> 4c744b11ef71
Step 20 : RUN /code/virtualenv_run/bin/pip install             -i https://pypi.yelpcorp.com/simple/             -r /code/requirements.txt
 ---> Using cache
 ---> f95734faa848
Step 21 : ADD . /code
 ---> cd5654ce6097
Removing intermediate container 56d115c9eb78
Step 22 : RUN useradd batch
 ---> Running in 587df4ebd9c8
 ---> 7c12c598f0a3
Removing intermediate container 587df4ebd9c8
Step 23 : RUN chown -R batch /code
 ---> Running in 3bba2c57ddd1
 ---> 1353174d9783
Removing intermediate container 3bba2c57ddd1
Step 24 : USER batch
 ---> Running in 98e7482a5280
 ---> 68eafc6765fc
Removing intermediate container 98e7482a5280
Step 25 : RUN mkdir /tmp/logs
 ---> Running in dd9e975c28ce
 ---> f4f0183211c6
Removing intermediate container dd9e975c28ce
Step 26 : VOLUME /tmp/logs
 ---> Running in d73931bf0126
 ---> 402b66f93f97
Removing intermediate container d73931bf0126
Step 27 : WORKDIR /code
 ---> Running in 948ef8296e14
 ---> 084df6e88ace
Removing intermediate container 948ef8296e14
Step 28 : ENV BASEPATH /code
 ---> Running in 99431cb7eed9
 ---> 2e1b61cb718b
Removing intermediate container 99431cb7eed9
Step 29 : CMD /usr/local/bin/dumb-init /code/virtualenv_run/bin/pypy /code/replication_handler/batch/parse_replication_stream.py -v --no-notification
 ---> Running in a0b4bc8bcd1c
 ---> e949ff331372
Removing intermediate container a0b4bc8bcd1c
Successfully built e949ff331372
DOCKER_TAG=replication-handler-dev-bowu tox -e itest
GLOB sdist-make: /nail/home/bowu/pg/replication_handler/setup.py
itest inst-nodeps: /nail/home/bowu/pg/replication_handler/.tox/dist/replication_handler-0.1.0.zip
itest runtests: PYTHONHASHSEED='2277420951'
itest runtests: commands[0] | py.test -m itest --ignore=setup.py -vv
============================= test session starts ==============================
platform linux2 -- Python 2.7.6, pytest-2.9.2, py-1.4.26, pluggy-0.3.1 -- /nail/home/bowu/pg/replication_handler/.tox/itest/bin/python2.7
cachedir: .cache
rootdir: /nail/home/bowu/pg/replication_handler, inifile: tox.ini
collecting ... collected 478 items

tests/integration/end_to_end_test.py::TestEndToEnd::test_complex_table PASSED
tests/integration/end_to_end_test.py::TestEndToEnd::test_create_table FAILED
tests/integration/end_to_end_test.py::TestEndToEnd::test_basic_table PASSED
tests/models/data_event_checkpoint_test.py::TestDataEventCheckpoint::test_get_topic_to_kafka_offset_map PASSED
tests/models/data_event_checkpoint_test.py::TestDataEventCheckpoint::test_kafka_offset_bulk_update PASSED
tests/models/global_event_state_test.py::TestGlobalEventState::test_upsert_global_event_state PASSED
tests/models/schema_event_state_test.py::TestSchemaEventState::test_get_pending_schema_event_state PASSED
tests/models/schema_event_state_test.py::TestSchemaEventState::test_delete_schema_event_state_by_id PASSED
tests/models/schema_event_state_test.py::TestSchemaEventState::test_get_lastest_schema_event_state PASSED
tests/models/schema_event_state_test.py::TestSchemaEventState::test_update_schema_event_state_to_complete_by_id PASSED

=================================== FAILURES ===================================
________________________ TestEndToEnd.test_create_table ________________________

self = <end_to_end_test.TestEndToEnd object at 0x50e6690>
containers = <data_pipeline.testing_helpers.containers.Containers object at 0x3dd9c50>
create_table_query = 'CREATE TABLE {table_name}
        (
            `id` int(11) NOT NULL PRIMARY KEY,
            `name` varchar(64) DEFAULT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8
        '
avro_schema = {'fields': [{'name': 'id', 'type': 'int'}, {'default': None, 'maxlen': '64', 'name': 'name', 'type': ['null', 'string']}], 'name': 'biz', 'namespace': '', 'type': 'record'}
table_name = 'biz', namespace = 'dev.refresh_primary.yelp'
schematizer = <data_pipeline.schematizer_clientlib.schematizer.SchematizerClient object at 0x3ddd390>
rbr_source_session = <sqlalchemy.orm.session.Session object at 0x50e6e90>

    def test_create_table(
        self,
        containers,
        create_table_query,
        avro_schema,
        table_name,
        namespace,
        schematizer,
        rbr_source_session
    ):
        increment_heartbeat(containers)
        execute_query_get_one_row(
            containers,
            RBR_SOURCE,
            create_table_query.format(table_name=table_name)
        )
    
        # Need to poll for the creation of the table
        self._wait_for_table(containers, SCHEMA_TRACKER, table_name)
    
        # Check the schematracker db also has the table.
        verify_create_table_query = "SHOW CREATE TABLE {table_name}".format(
            table_name=table_name)
        verify_create_table_result = execute_query_get_one_row(containers, SCHEMA_TRACKER, verify_create_table_query)
        expected_create_table_result = execute_query_get_one_row(containers, RBR_SOURCE, verify_create_table_query)
        self.assert_expected_result(verify_create_table_result, expected_create_table_result)
    
        # It's necessary to insert data for the topic to actually be created.
        Biz = self._generate_basic_model(table_name)
        rbr_source_session.add(Biz(id=1, name='insert'))
        rbr_source_session.commit()
    
        self._wait_for_schematizer_topic(schematizer, namespace, table_name)
    
        # Check schematizer.
        self.check_schematizer_has_correct_source_info(
            table_name=table_name,
            avro_schema=avro_schema,
            namespace=namespace,
>           schematizer=schematizer
        )

tests/integration/end_to_end_test.py:169: 
_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ 

self = <end_to_end_test.TestEndToEnd object at 0x50e6690>, table_name = 'biz'
avro_schema = {'fields': [{'name': 'id', 'type': 'int'}, {'default': None, 'maxlen': '64', 'name': 'name', 'type': ['null', 'string']}], 'name': 'biz', 'namespace': '', 'type': 'record'}
namespace = 'dev.refresh_primary.yelp'
schematizer = <data_pipeline.schematizer_clientlib.schematizer.SchematizerClient object at 0x3ddd390>

    def check_schematizer_has_correct_source_info(
        self,
        table_name,
        avro_schema,
        namespace,
        schematizer
    ):
        sources = schematizer.get_sources_by_namespace(namespace)
        source = next(src for src in reversed(sources) if src.name == table_name)
        topic = schematizer.get_topics_by_source_id(source.source_id)[-1]
        schema = schematizer.get_latest_schema_by_topic_name(topic.name)
        assert schema.topic.source.name == table_name
        assert schema.topic.source.namespace.name == namespace
>       assert schema.schema_json == avro_schema
E       assert {'fields': [{...: ['id'], ...} == {'fields': [{'...pe': 'record'}
E         Common items:
E         {u'name': u'biz', u'namespace': u'', u'type': u'record'}
E         Differing items:
E         {'fields': [{'name': 'id', 'pkey': 1, 'type': 'int'}, {'default': None, 'maxlen': 64, 'name': 'name', 'type': ['null', 'string']}]} != {'fields': [{'name': 'id', 'type': 'int'}, {'default': None, 'maxlen': '64', 'name': 'name', 'type': ['null', 'string']}]}
E         Left contains more items:
E         {u'pkey': [u'id']}
E         Full diff:
E         - {u'fields': [{u'name': u'id', u'pkey': 1, u'type': u'int'},
E         ?                              ------------
E         + {u'fields': [{u'name': u'id', u'type': u'int'},
E         {u'default': None,
E         -               u'maxlen': 64,
E         +               u'maxlen': u'64',
E         ?                          ++  +
E         u'name': u'name',
E         u'type': [u'null', u'string']}],
E         u'name': u'biz',
E         u'namespace': u'',
E         -  u'pkey': [u'id'],
E         u'type': u'record'}

tests/integration/end_to_end_test.py:327: AssertionError
===================== 468 tests deselected by "-m 'itest'" =====================
============= 1 failed, 9 passed, 468 deselected in 375.26 seconds =============
ERROR: InvocationError: '/nail/home/bowu/pg/replication_handler/.tox/itest/bin/py.test -m itest --ignore=setup.py -vv'
___________________________________ summary ____________________________________
ERROR:   itest: commands failed
