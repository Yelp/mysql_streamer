import pytest

from models.schema_event_state import SchemaEventState
from testing import sandbox


class TestSchemaEventState(object):

    @pytest.fixture
    def gtid(self):
        return "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"

    @pytest.fixture
    def status(self):
        return "Pending"

    @pytest.fixture
    def query(self):
        return "alter table business add column category varchar(255)"

    @pytest.fixture
    def create_table_statement(self):
        return "CREATE TABLE `business` (\
                   `id` int(11) NOT NULL AUTO_INCREMENT,\
                   `name` VARCHAR(255) NOT NULL,\
                   `time_created` int(11) NOT NULL,\
                   `time_updated` int(11) NOT NULL,\
                   PRIMARY KEY (`id`)\
               ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci"

    @pytest.fixture
    def table_name(self):
        return "business"

    def test_example(self, gtid, status, query, create_table_statement, table_name):
        with sandbox.database_sandbox_master_connection_set() as sandbox_session:
            schema_event_state = SchemaEventState(
                gtid=gtid,
                status=status,
                query=query,
                create_table_statement=create_table_statement,
                table_name=table_name
            )
            sandbox_session.add(schema_event_state)
            sandbox_session.flush()
            result = sandbox_session.query(SchemaEventState).one()
            assert result == schema_event_state
