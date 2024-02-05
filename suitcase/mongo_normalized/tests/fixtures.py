# This separate fixtures module allows external libraries (e.g.
# intake-bluesky-mongo) to import and reuse this fixtures without importing
# *all* the fixtures used in conftest and the dependencies that they carry.
import mongomock
import pymongo
import pytest
import uuid


@pytest.fixture()
def db_factory(request):
    def inner():
        database_name = f'test-{str(uuid.uuid4())}'
        uri = 'mongodb://dbroker:secret@localhost:27017/test_database?authSource=admin'
        client = pymongo.MongoClient(uri, False)

        def drop():
            client.drop_database(database_name)

        request.addfinalizer(drop)
        return client[database_name]
    return inner
