from bluesky.tests.conftest import RE  # noqa
from ophyd.tests.conftest import hw  # noqa
from suitcase.utils.tests.conftest import (  # noqa
    example_data, generate_data, plan_type, detector_list, event_type)
import pymongo
import pytest
import uuid


@pytest.fixture()
def db_factory(request):
    def inner():
        database_name = f'test-{str(uuid.uuid4())}'
        uri = f'mongodb://localhost:27017/'
        client = pymongo.MongoClient(uri)

        def drop():
            client.drop_database(database_name)

        request.addfinalizer(drop)
        return client[database_name]
    return inner
