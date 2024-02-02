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
    documents[0][1]["new_key"] = "new_value"
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
    original_start = next(item[1] for item in documents if item[0] == "start")
    original_stop = next(item[1] for item in documents if item[0] == "stop")
    original_descriptor = next(item[1] for item in documents if item[0] == "descriptor")
    # (1) Make mutable copies
    start = copy.deepcopy(original_start)
    stop = copy.deepcopy(original_stop)
    descriptor = copy.deepcopy(original_descriptor)
    # (2) Update a property of the copies
    start["user"] = "first updated temp user"
    serializer.update("start", start)
    stop["reason"] = "Everything happens for a reason."
    serializer.update("stop", stop)
    descriptor["name"] = "secondary"
    serializer.update("descriptor", descriptor)
    # (3) Get the updated record from the database to confirm changes
    real_start = metadatastore_db.get_collection("run_start").find_one(
        {"uid": start["uid"]}
    )
    real_start.pop("_id")
    real_stop = metadatastore_db.get_collection("run_stop").find_one(
        {"uid": stop["uid"]}
    )
    real_stop.pop("_id")
    real_descriptor = metadatastore_db.get_collection("event_descriptor").find_one(
        {"uid": descriptor["uid"]}
    )
    real_descriptor.pop("_id")
    # (4) Test the data
    assert sanitize_doc(real_start) == sanitize_doc(start)
    assert sanitize_doc(real_stop) == sanitize_doc(stop)
    assert sanitize_doc(real_descriptor) == sanitize_doc(descriptor)
    # (5) Test the revisions
    revision_start = metadatastore_db.get_collection("run_start_revisions").find_one(
        {"document.uid": start["uid"]}
    )
    assert revision_start["revision"] == 0
    revision_start.pop("revision")
    revision_start.pop("_id")
    assert sanitize_doc(revision_start["document"]) == sanitize_doc(original_start)

    revision_stop = metadatastore_db.get_collection("run_stop_revisions").find_one(
        {"document.uid": stop["uid"]}
    )
    assert revision_stop["revision"] == 0
    revision_stop.pop("revision")
    revision_stop.pop("_id")
    assert sanitize_doc(revision_stop["document"]) == sanitize_doc(original_stop)

    revision_descriptor = metadatastore_db.get_collection(
        "event_descriptor_revisions"
    ).find_one({"document.uid": descriptor["uid"]})
    assert revision_descriptor["revision"] == 0
    revision_descriptor.pop("revision")
    revision_descriptor.pop("_id")
    assert sanitize_doc(revision_descriptor["document"]) == sanitize_doc(
        original_descriptor
    )

    # (6) Test another revision
    revision1_start = copy.deepcopy(start)
    revision1_stop = copy.deepcopy(stop)
    start["user"] = "second updated temp user"
    serializer.update("start", start)
    stop["reason"] = "Nothing happens for a reason."
    serializer.update("stop", stop)
    real_start = metadatastore_db.get_collection("run_start").find_one(
        {"uid": start["uid"]}
    )
    real_start.pop("_id")
    assert sanitize_doc(real_start) == sanitize_doc(start)
    real_stop = metadatastore_db.get_collection("run_stop").find_one(
        {"uid": stop["uid"]}
    )
    real_stop.pop("_id")
    assert sanitize_doc(real_stop) == sanitize_doc(stop)
    revision_start = metadatastore_db.get_collection("run_start_revisions").find_one(
        {"document.uid": start["uid"], "revision": 1}
    )
    assert revision_start["revision"] == 1
    revision_start.pop("revision")
    revision_start.pop("_id")
    revision_stop = metadatastore_db.get_collection("run_stop_revisions").find_one(
        {"document.uid": stop["uid"], "revision": 1}
    )
    assert revision_stop["revision"] == 1
    revision_stop.pop("revision")
    revision_stop.pop("_id")
    assert sanitize_doc(revision_start["document"]) == sanitize_doc(revision1_start)
    assert sanitize_doc(revision_stop["document"]) == sanitize_doc(revision1_stop)


def test_notimplemented_error(db_factory, example_data):
    metadatastore_db = db_factory()
    asset_registry_db = db_factory()
    serializer = Serializer(metadatastore_db, asset_registry_db)
    with pytest.raises(NotImplementedError):
        assert serializer.update("not_start", {})


def test_validation_error(db_factory, example_data):
    metadatastore_db = db_factory()
    asset_registry_db = db_factory()
    serializer = Serializer(metadatastore_db, asset_registry_db)
    with pytest.raises(ValidationError):
        assert serializer.update("start", {})


def test_index_creation(db_factory):
    db = db_factory()
    print(type(db))
    metadatastore_db = db_factory()
    asset_registry_db = db_factory()
    Serializer(metadatastore_db, asset_registry_db)

    indexes = asset_registry_db.resource.index_information()
    assert len(indexes.keys()) == 3
    assert not indexes["uid_1"].get("unique")
    assert indexes["resource_id_1"]

    indexes = asset_registry_db.datum.index_information()
    assert len(indexes.keys()) == 3
    assert indexes["datum_id_1"]["unique"]
    assert indexes["resource_1"]

    indexes = metadatastore_db.run_start.index_information()
    assert len(indexes.keys()) == 6
    assert indexes["uid_1"]["unique"]
    assert indexes["time_-1_scan_id_-1"]
    assert indexes["$**_text"]
    assert indexes["data_session_1"]
    assert indexes["data_groups_1"]

    indexes = metadatastore_db.run_stop.index_information()
    assert len(indexes.keys()) == 5
    assert indexes["uid_1"]["unique"]
    assert indexes["run_start_1"]["unique"]
    assert indexes["time_-1"]
    assert indexes["$**_text"]

    indexes = metadatastore_db.event_descriptor.index_information()
    assert len(indexes.keys()) == 5
    assert indexes["uid_1"]["unique"]
    assert indexes["run_start_-1_time_-1"]
    assert indexes["time_-1"]
    assert indexes["$**_text"]

    indexes = metadatastore_db.event.index_information()
    assert len(indexes.keys()) == 3
    assert indexes["uid_1"]["unique"]
    assert indexes["descriptor_-1_time_1"]


def test_resource_uid_unique(db_factory):
    db = db_factory()
    print(type(db))
    metadatastore_db = db_factory()
    asset_registry_db = db_factory()
    Serializer(metadatastore_db, asset_registry_db, resource_uid_unique=True)

    indexes = asset_registry_db.resource.index_information()
    assert indexes["uid_1"].get("unique")
