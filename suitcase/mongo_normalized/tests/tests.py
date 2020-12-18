# Tests should generate (and then clean up) any files they need for testing. No
# binary files should be included in the repository.

import copy
import pytest
from event_model import sanitize_doc
from jsonschema import ValidationError
from suitcase.mongo_normalized import DuplicateUniqueID, Serializer


def test_export(db_factory, example_data):
    documents = example_data()
    metadatastore_db = db_factory()
    asset_registry_db = db_factory()
    serializer = Serializer(metadatastore_db, asset_registry_db)
    for item in documents:
        serializer(*item)


def test_duplicates(db_factory, example_data):
    # Duplicate should not cause exceptions, and should be deduped.
    documents = example_data()
    metadatastore_db = db_factory()
    asset_registry_db = db_factory()
    serializer = Serializer(metadatastore_db, asset_registry_db)
    for item in documents:
        serializer(*item)
    for item in documents:
        serializer(*item)

    # Modify a document, check that inserting a document with uid,
    # but different content raises.
    documents[0][1]['new_key'] = 'new_value'
    with pytest.raises(DuplicateUniqueID):
        for item in documents:
            serializer(*item)


def test_update(db_factory, example_data):
    documents = example_data()
    metadatastore_db = db_factory()
    asset_registry_db = db_factory()
    serializer = Serializer(metadatastore_db, asset_registry_db)
    for item in documents:
        serializer(*item)
    original = documents[0][1]
    start = copy.deepcopy(original)
    start['user'] = 'first updated temp user'
    serializer.update('start', start)
    real = metadatastore_db.get_collection('run_start').find_one({'uid': start['uid']})
    real.pop('_id')
    assert sanitize_doc(real) == sanitize_doc(start)
    revision = metadatastore_db.get_collection('run_start_revisions').find_one({'document.uid': start['uid']})
    assert revision['revision'] == 0
    revision.pop('revision')
    revision.pop('_id')
    assert sanitize_doc(revision['document']) == sanitize_doc(original)

    revision1 = copy.deepcopy(start)
    start['user'] = 'second updated temp user'
    serializer.update('start', start)
    real = metadatastore_db.get_collection('run_start').find_one({'uid': start['uid']})
    real.pop('_id')
    assert sanitize_doc(real) == sanitize_doc(start)
    revision = metadatastore_db.get_collection('run_start_revisions').find_one({'document.uid': start['uid'],
                                                                                'revision': 1})
    assert revision['revision'] == 1
    revision.pop('revision')
    revision.pop('_id')
    assert sanitize_doc(revision['document']) == sanitize_doc(revision1)


def test_notimplemented_error(db_factory, example_data):
    metadatastore_db = db_factory()
    asset_registry_db = db_factory()
    serializer = Serializer(metadatastore_db, asset_registry_db)
    with pytest.raises(NotImplementedError):
        assert serializer.update('not_start', {})


def test_validation_error(db_factory, example_data):
    metadatastore_db = db_factory()
    asset_registry_db = db_factory()
    serializer = Serializer(metadatastore_db, asset_registry_db)
    with pytest.raises(ValidationError):
        assert serializer.update('start', {})


def test_index_creation(db_factory):
    db = db_factory()
    print(type(db))
    metadatastore_db = db_factory()
    asset_registry_db = db_factory()
    Serializer(metadatastore_db, asset_registry_db)

    indexes = asset_registry_db.resource.index_information()
    assert len(indexes.keys()) == 3
    assert not indexes['uid_1'].get('unique')
    assert indexes['resource_id_1']

    indexes = asset_registry_db.datum.index_information()
    assert len(indexes.keys()) == 3
    assert indexes['datum_id_1']['unique']
    assert indexes['resource_1']

    indexes = metadatastore_db.run_start.index_information()
    assert len(indexes.keys()) == 6
    assert indexes['uid_1']['unique']
    assert indexes['time_-1_scan_id_-1']
    assert indexes['$**_text']
    assert indexes['data_session_1']
    assert indexes['data_groups_1']

    indexes = metadatastore_db.run_stop.index_information()
    assert len(indexes.keys()) == 5
    assert indexes['uid_1']['unique']
    assert indexes['run_start_1']['unique']
    assert indexes['time_-1']
    assert indexes['$**_text']

    indexes = metadatastore_db.event_descriptor.index_information()
    assert len(indexes.keys()) == 5
    assert indexes['uid_1']['unique']
    assert indexes['run_start_-1_time_-1']
    assert indexes['time_-1']
    assert indexes['$**_text']

    indexes = metadatastore_db.event.index_information()
    assert len(indexes.keys()) == 3
    assert indexes['uid_1']['unique']
    assert indexes['descriptor_-1_time_1']


def test_resource_uid_unique(db_factory):
    db = db_factory()
    print(type(db))
    metadatastore_db = db_factory()
    asset_registry_db = db_factory()
    Serializer(metadatastore_db, asset_registry_db, resource_uid_unique=True)

    indexes = asset_registry_db.resource.index_information()
    assert indexes['uid_1'].get('unique')
