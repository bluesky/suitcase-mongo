# Tests should generate (and then clean up) any files they need for testing. No
# binary files should be included in the repository.

import copy
import pytest
from event_model import sanitize_doc
from jsonschema import ValidationError
from suitcase.mongo_normalized import Serializer


def test_export(db_factory, example_data):
    documents = example_data()
    metadatastore_db = db_factory()
    asset_registry_db = db_factory()
    serializer = Serializer(metadatastore_db, asset_registry_db)
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
    revision = metadatastore_db.get_collection('run_start_revisions').find_one({'uid': start['uid']})
    assert revision['revision'] == 0
    revision.pop('revision')
    revision.pop('_id')
    assert sanitize_doc(revision) == sanitize_doc(original)

    revision1 = copy.deepcopy(start)
    start['user'] = 'second updated temp user'
    serializer.update('start', start)
    real = metadatastore_db.get_collection('run_start').find_one({'uid': start['uid']})
    real.pop('_id')
    assert sanitize_doc(real) == sanitize_doc(start)
    revision = metadatastore_db.get_collection('run_start_revisions').find_one({'uid': start['uid'],
                                                                                'revision': 1})
    assert revision['revision'] == 1
    revision.pop('revision')
    revision.pop('_id')
    assert sanitize_doc(revision) == sanitize_doc(revision1)


def test_notimplemented_error(db_factory, example_data):
    documents = example_data()
    metadatastore_db = db_factory()
    asset_registry_db = db_factory()
    serializer = Serializer(metadatastore_db, asset_registry_db)
    with pytest.raises(NotImplementedError) as e:
        assert serializer.update('not_start', {})
    assert str(e.value) == 'Only start could be updated'


def test_validation_error(db_factory, example_data):
    documents = example_data()
    metadatastore_db = db_factory()
    asset_registry_db = db_factory()
    serializer = Serializer(metadatastore_db, asset_registry_db)
    with pytest.raises(ValidationError) as e:
        assert serializer.update('start', {})
