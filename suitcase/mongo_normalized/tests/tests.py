# Tests should generate (and then clean up) any files they need for testing. No
# binary files should be included in the repository.


from suitcase.mongo_normalized import Serializer


def test_export(db_factory, example_data):
    documents = example_data()
    metadatastore_db = db_factory()
    asset_registry_db = db_factory()
    serializer = Serializer(metadatastore_db, asset_registry_db)
    for item in documents:
        serializer(*item)
