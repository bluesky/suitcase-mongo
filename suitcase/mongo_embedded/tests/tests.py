# Tests should generate (and then clean up) any files they need for testing. No
# binary files should be included in the repository.

from suitcase.mongo_embedded import Serializer


def test_export(db_factory, example_data):
    documents = example_data()
    volatile_db = db_factory()
    permanent_db = db_factory()
    serializer = Serializer(volatile_db, permanent_db)
    for item in documents:
        serializer(*item)


def test_multithread(db_factory, example_data):
    documents = example_data()
    volatile_db = db_factory()
    permanent_db = db_factory()
    serializer = Serializer(volatile_db, permanent_db, num_threads=5)
    for item in documents:
        serializer(*item)


def test_smallbuffer(db_factory, example_data):
    documents = example_data()
    volatile_db = db_factory()
    permanent_db = db_factory()
    serializer = Serializer(volatile_db, permanent_db, buffer_size=1000)
    for item in documents:
        serializer(*item)


def test_smallpage(db_factory, example_data):
    documents = example_data()
    volatile_db = db_factory()
    permanent_db = db_factory()
    serializer = Serializer(volatile_db, permanent_db, page_size=10000)
    for item in documents:
        serializer(*item)
