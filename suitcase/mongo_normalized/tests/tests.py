# Tests should generate (and then clean up) any files they need for testing. No
# binary files should be included in the repository.

import copy
from suitcase.mongo_normalized import Serializer


def test_export(db_factory, example_data):
    documents = example_data()
    metadatastore_db = db_factory()
    asset_registry_db = db_factory()
    serializer = Serializer(metadatastore_db, asset_registry_db)
    for item in documents:
        serializer(*item)


def listit(t):
    return list(map(listit, t)) if isinstance(t, (list, tuple)) else t


def test_update(db_factory, example_data):
    documents = example_data()
    metadatastore_db = db_factory()
    asset_registry_db = db_factory()
    serializer = Serializer(metadatastore_db, asset_registry_db)
    for item in documents:
        serializer(*item)
    origin = documents[0][1]
    origin['hints']['dimensions'] = listit(origin['hints']['dimensions'])
    start = copy.deepcopy(origin)
    start['user'] = 'first updated temp user'
    serializer.update('start', start)
    real = metadatastore_db.get_collection('run_start').find_one({'uid': start['uid']})
    real.pop('_id')
    assert real == start
    revision = metadatastore_db.get_collection('run_start_revisions').find_one({'uid': start['uid']})
    assert revision['revision'] == 0
    revision.pop('revision')
    revision.pop('_id')
    assert revision == origin

    revision1 = copy.deepcopy(start)
    start['user'] = 'second updated temp user'
    serializer.update('start', start)
    real = metadatastore_db.get_collection('run_start').find_one({'uid': start['uid']})
    real.pop('_id')
    assert real == start
    revision = metadatastore_db.get_collection('run_start_revisions').find_one({'uid': start['uid'],
                                                                                'revision': 1})
    assert revision['revision'] == 1
    revision.pop('revision')
    revision.pop('_id')
    assert revision == revision1
