# Tests should generate (and then clean up) any files they need for testing. No
# binary files should be included in the repository.


from suitcase.mongo_layout1 import Serializer


def test_export(db_factory, example_data):
    documents = example_data()
    metadatastore_client = db_factory()
    asset_registry_client = db_factory()
    serializer = Serializer(metadatastore_client, asset_registry_client)
    for item in documents:
        serializer(*item)
