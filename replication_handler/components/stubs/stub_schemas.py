# -*- coding: utf-8 -*-


class Response(object):

    def __init__(self, topic, schema_id, schema):
        self.topic = topic
        self.schema_id = schema_id
        self.schema = schema


class Topic(object):

    def __init__(self, name, source):
        self.name = name
        self.source = source


class Source(object):

    def __init__(self, namespace, name):
        self.namespace = namespace
        self.name = name


def stub_business_schema():
    source = Source("yelp_main", "business")
    topic = Topic("services.datawarehouse.etl.business.0", source)
    schema= '{"type": "record", "namespace": "yelp", "name": "business", "fields": [{"pkey": true, "type": "int", "name": "id"}, {"default": null, "type": ["null", "int"], "name": "acxiom_id"}, {"default": null, "maxlen": 64, "type": ["null", "string"], "name": "name"}, {"default": null, "maxlen": 128, "type": ["null", "string"], "name": "address1"}, {"default": null, "maxlen": 128, "type": ["null", "string"], "name": "address2"}, {"default": null, "maxlen": 128, "type": ["null", "string"], "name": "address3"}, {"default": null, "maxlen": 64, "type": ["null", "string"], "name": "city"}, {"default": null, "maxlen": 64, "type": ["null", "string"], "name": "county"}, {"default": null, "maxlen": 3, "type": ["null", "string"], "name": "state"}, {"default": null, "maxlen": 2, "type": ["null", "string"], "name": "country"}, {"default": null, "maxlen": 12, "type": ["null", "string"], "name": "zip"}, {"default": null, "maxlen": 32, "type": ["null", "string"], "name": "phone"}, {"default": null, "maxlen": 32, "type": ["null", "string"], "name": "fax"}, {"default": null, "maxlen": 255, "type": ["null", "string"], "name": "url"}, {"default": null, "maxlen": 64, "type": ["null", "string"], "name": "email"}, {"default": 0, "type": "int", "name": "flags"}, {"default": null, "type": ["null", "double"], "name": "latitude"}, {"default": null, "type": ["null", "double"], "name": "longitude"}, {"default": null, "type": ["null", "double"], "name": "accuracy"}, {"default": 0, "type": "int", "name": "time_created"}, {"default": null, "type": ["null", "double"], "name": "score"}, {"default": null, "type": ["null", "double"], "name": "rating"}, {"default": 0, "type": "int", "name": "review_count"}, {"default": null, "type": ["null", "int"], "name": "photo_id"}, {"default": null, "maxlen": 96, "type": ["null", "string"], "name": "alias"}, {"default": null, "type": ["null", "int"], "unsigned": true, "name": "geoquad"}, {"default": null, "type": ["null", "int"], "unsigned": true, "name": "data_source_type"}]}'
    return Response(topic, 0, schema)

# removed geoquad just in order to create an altered schema
def stub_altered_business_schema():
    source = Source("yelp_main", "business")
    topic = Topic("services.datawarehouse.etl.business.0", source)
    schema = '{"type": "record", "namespace": "yelp", "name": "business", "fields": [{"pkey": true, "type": "int", "name": "id"}, {"default": null, "type": ["null", "int"], "name": "acxiom_id"}, {"default": null, "maxlen": 64, "type": ["null", "string"], "name": "name"}, {"default": null, "maxlen": 128, "type": ["null", "string"], "name": "address1"}, {"default": null, "maxlen": 128, "type": ["null", "string"], "name": "address2"}, {"default": null, "maxlen": 128, "type": ["null", "string"], "name": "address3"}, {"default": null, "maxlen": 64, "type": ["null", "string"], "name": "city"}, {"default": null, "maxlen": 64, "type": ["null", "string"], "name": "county"}, {"default": null, "maxlen": 3, "type": ["null", "string"], "name": "state"}, {"default": null, "maxlen": 2, "type": ["null", "string"], "name": "country"}, {"default": null, "maxlen": 12, "type": ["null", "string"], "name": "zip"}, {"default": null, "maxlen": 32, "type": ["null", "string"], "name": "phone"}, {"default": null, "maxlen": 32, "type": ["null", "string"], "name": "fax"}, {"default": null, "maxlen": 255, "type": ["null", "string"], "name": "url"}, {"default": null, "maxlen": 64, "type": ["null", "string"], "name": "email"}, {"default": 0, "type": "int", "name": "flags"}, {"default": null, "type": ["null", "double"], "name": "latitude"}, {"default": null, "type": ["null", "double"], "name": "longitude"}, {"default": null, "type": ["null", "double"], "name": "accuracy"}, {"default": 0, "type": "int", "name": "time_created"}, {"default": null, "type": ["null", "double"], "name": "score"}, {"default": null, "type": ["null", "double"], "name": "rating"}, {"default": 0, "type": "int", "name": "review_count"}, {"default": null, "type": ["null", "int"], "name": "photo_id"}, {"default": null, "maxlen": 96, "type": ["null", "string"], "name": "alias"}, {"default": null, "type": ["null", "int"], "unsigned": true, "name": "data_source_type"}]}'
    return Response(topic, 1, schema)


class StubSchemaClient(object):

    def register_avro_schema_from_mysql_statements(
        self,
        namespace,
        source,
        source_owner_email,
        mysql_statements
    ):
        """TODO(cheng|DATAPIPE-118): We should use the correct parameters specified
        the ticket.
        """
        if len(mysql_statements) == 1:
            return stub_business_schema()
        else:
            return stub_altered_business_schema()
