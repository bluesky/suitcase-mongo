import event_model
import pymongo
from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions


class Serializer(event_model.DocumentRouter):
    def __init__(
        self,
        metadatastore_db,
        asset_registry_db,
        ignore_duplicates=True,
        resource_uid_unique=False,
        tls=False,
    ):
        """
        Insert documents into MongoDB using layout v1.

        This layout uses a separate Mongo collection per document type and a
        separate Mongo document for each logical document.

        Note that this Seralizer does not share the standard Serializer
        name or signature common to suitcase packages because it can only write
        via pymongo, not to an arbitrary user-provided buffer.

        Parameters
        ----------
        metadatastore_db : pymongo.Database or URI
        asset_registry_db : pymongo.Database or URI
        ignore_duplicates : boolean, optional
            When receiving from a message bus, it is difficult to ensure that
            we will never receive the same document more than once. If this is
            set to True, when we receive a document with a uid that we already
            have in the database, we check that their contents are identical.
            If they are, we silently ignore the duplicate. If this is set to
            False, we always raise DuplicateUniqueID.
        resource_uid_unique : boolean, optional
            For historical reasons, we cannot create a *unique* index on
            Resource uid. Once databroker.v0 is retried and any old databases
            that violate uniqueness of Resource uid are migrated, this may be
            flipped to True. For now, it is False by default and generally
            should not be flipped to True until those conditions are met.
        tls : boolean, optional
            Set to True if database connection should use tls.
        """
        if isinstance(metadatastore_db, str):
            mds_db = _get_database(metadatastore_db, tls)
        else:
            mds_db = metadatastore_db
        if isinstance(asset_registry_db, str):
            assets_db = _get_database(asset_registry_db, tls)
        else:
            assets_db = asset_registry_db
        self._run_start_collection = mds_db.get_collection("run_start")
        self._run_start_collection_revisions = mds_db.get_collection(
            "run_start_revisions"
        )
        self._run_stop_collection = mds_db.get_collection("run_stop")
        self._run_stop_collection_revisions = mds_db.get_collection(
            "run_stop_revisions"
        )
        self._event_descriptor_collection_revisions = mds_db.get_collection(
            "event_descriptor_revisions"
        )
        self._event_descriptor_collection = mds_db.get_collection("event_descriptor")
        self._event_collection = mds_db.get_collection("event")

        self._resource_collection = assets_db.get_collection("resource")
        self._datum_collection = assets_db.get_collection("datum")

        self._collections = {
            "start": self._run_start_collection,
            "stop": self._run_stop_collection,
            "resource": self._resource_collection,
            "descriptor": self._event_descriptor_collection,
            "event": self._event_collection,
            "datum": self._datum_collection,
        }

        self._metadatastore_db = mds_db
        self._asset_registry_db = assets_db
        self._ignore_duplicates = ignore_duplicates
        self._resource_uid_unique = resource_uid_unique

    def create_indexes(self):
        """
        Create indexes on the various collections.

        If the index already exists, this has no effect.
        """
        self._resource_collection.create_index("uid", unique=self._resource_uid_unique)
        self._resource_collection.create_index("resource_id")  # legacy
        # TODO: Migrate all Resources to have a RunStart UID, and then make a
        # unique index on:
        # [('uid', pymongo.ASCENDING), ('run_start', pymongo.ASCENDING)]
        self._datum_collection.create_index("datum_id", unique=True)
        self._datum_collection.create_index("resource")
        self._run_start_collection.create_index("uid", unique=True)
        self._run_start_collection.create_index("scan_id", unique=False)
        self._run_start_collection.create_index(
            [("scan_id", pymongo.DESCENDING), ("_id", pymongo.DESCENDING)], unique=True
        )
        self._run_start_collection.create_index(
            [("time", pymongo.ASCENDING), ("_id", pymongo.DESCENDING)], unique=True
        )
        self._run_start_collection.create_index(
            [("time", pymongo.DESCENDING), ("_id", pymongo.DESCENDING)], unique=True
        )
        self._run_start_collection.create_index(
            [("time", pymongo.DESCENDING), ("scan_id", pymongo.DESCENDING)],
            unique=False,
            background=True,
        )
        self._run_start_collection.create_index([("$**", "text")])
        self._run_start_collection.create_index("data_session", unique=False)
        self._run_start_collection.create_index("data_groups", unique=False)
        self._run_stop_collection.create_index("run_start", unique=True)
        self._run_stop_collection.create_index("uid", unique=True)
        self._run_stop_collection.create_index(
            [("time", pymongo.DESCENDING)], unique=False, background=True
        )
        self._run_stop_collection.create_index([("$**", "text")])
        self._event_descriptor_collection.create_index("uid", unique=True)
        self._event_descriptor_collection.create_index(
            [("run_start", pymongo.DESCENDING), ("time", pymongo.DESCENDING)],
            unique=False,
            background=True,
        )
        self._event_descriptor_collection.create_index(
            [("time", pymongo.DESCENDING)], unique=False, background=True
        )
        self._event_descriptor_collection.create_index([("$**", "text")])
        self._event_collection.create_index("uid", unique=True)
        self._event_collection.create_index(
            [("descriptor", pymongo.DESCENDING), ("time", pymongo.ASCENDING)],
            unique=False,
            background=True,
        )

    def __call__(self, name, doc):
        # Before inserting into mongo, convert any numpy objects into built-in
        # Python types compatible with pymongo.
        sanitized_doc = event_model.sanitize_doc(doc)
        return super().__call__(name, sanitized_doc)

    def _insert(self, name, doc):
        try:
            self._collections[name].insert_one(doc)
        except pymongo.errors.DuplicateKeyError as err:
            if not self._ignore_duplicates:
                raise DuplicateUniqueID(
                    "A document with the same unique id as this one "
                    f"already exists in the database. Document:\n{doc}"
                ) from err
            else:
                doc.pop("_id")
                if name == "datum":
                    id_name = "datum_id"
                else:
                    id_name = "uid"
                existing = self._collections[name].find_one(
                    {id_name: doc[id_name]}, {"_id": False}
                )
                if existing != doc:
                    raise DuplicateUniqueID(
                        "A document with the same unique id as this one "
                        "already exists in the database, and it has different "
                        "contents.\n"
                        f"Existing document:\n{existing}\nNew document:\n{doc}"
                    ) from err

    def update(self, name, doc):
        """
        Update documents. Currently only 'start' documents are supported.

        The original copy is retained internally, but there is as of yet no
        *public* API for retrieving anything but the most recent update.

        Parameters
        ----------

        name: {'start', 'stop', 'descriptor'}
            The type of document being updated.
        doc: dict
            The new version of the document. Its uid will be used to match it
            to the current version, the one to be updated.
        """
        if name in {"start", "stop", "descriptor"}:
            event_model.schema_validators[
                getattr(event_model.DocumentNames, name)
            ].validate(doc)
            # Keys and collection names differ slightly between start, stop and descriptor
            key = "uid"
            name = f"_event_{name}" if name == "descriptor" else f"_run_{name}"
            current_col = getattr(self, f"{name}_collection")
            revisions_col = getattr(self, f"{name}_collection_revisions")
            old = current_col.find_one({key: doc[key]})
            if old is None and (name == "_run_stop" or name == "_event_descriptor"):
                # New stop or descriptor document : insert it
                current_col.insert_one(doc)
            else:
                old.pop("_id")
                # Field Saftey Enforcement : Prevent restricted fields from changing
                restricted_fields = ["run_start"]
                for field in restricted_fields:
                    if field in old and field in doc:
                        if old[field] != doc[field]:
                            raise ValueError(
                                f"Field '{field}' is restricted and cannot be changed."
                            )
                target_uid_docs = revisions_col.find({"document.uid": doc["uid"]})
                cur = target_uid_docs.sort([("revision", pymongo.DESCENDING)]).limit(1)
                wrapped = dict()
                try:
                    wrapped["revision"] = next(cur)["revision"] + 1
                except StopIteration:
                    wrapped["revision"] = 0
                wrapped["document"] = old
                revisions_col.insert_one(wrapped)
                current_col.find_one_and_replace({"uid": doc["uid"]}, doc)
        else:
            raise NotImplementedError(
                f"Updating a {name} document is not currently supported. "
            )

    def start(self, doc):
        self._insert("start", doc)

    def descriptor(self, doc):
        self._insert("descriptor", doc)

    def resource(self, doc):
        # In old databases, we know there are duplicates Resources. Until we
        # clean those up, we cannot create a *unique* index on Resource uid, so
        # cannot rely the database insertion process to alert us to key
        # collisions. We need to take a special approach for Resources.
        # Here we are "asking permission" rather than "begging forgiveness". It
        # is slow, but since there are never a large number of Resources per
        # Run, this is acceptable.
        if self._resource_uid_unique:
            self._insert("resource", doc)
        else:
            existing = self._collections["resource"].find_one(
                {"uid": doc["uid"]}, {"_id": False}
            )
            if existing is not None:
                if existing != doc:
                    raise DuplicateUniqueID(
                        "A document with the same unique id as this one "
                        "already exists in the database, and it has different "
                        "contents.\n"
                        f"Existing document:\n{existing}\nNew document:\n{doc}"
                    )
            self._collections["resource"].insert_one(doc)

    def event(self, doc):
        self._insert("event", doc)

    def event_page(self, doc):
        # Unpack an EventPage into Events and do the actual insert inside
        # the `event` method. (This is the oppose what DocumentRouter does by
        # default.)

        event_method = self.event  # Avoid attribute lookup in hot loop.
        filled_events = []

        for event_doc in event_model.unpack_event_page(doc):
            filled_events.append(event_method(event_doc))

    def datum(self, doc):
        self._insert("datum", doc)

    def datum_page(self, doc):
        # Unpack an DatumPage into Datum and do the actual insert inside
        # the `datum` method. (This is the oppose what DocumentRouter does by
        # default.)

        datum_method = self.datum  # Avoid attribute lookup in hot loop.
        filled_datums = []

        for datum_doc in event_model.unpack_datum_page(doc):
            filled_datums.append(datum_method(datum_doc))

    def stop(self, doc):
        self._insert("stop", doc)

    def __repr__(self):
        # Display connection info in eval-able repr.
        return (
            f"{type(self).__name__}("
            f"metadatastore_db={self._metadatastore_db!r}, "
            f"asset_registry_db={self._asset_registry_db!r})"
        )


def _get_database(uri, tls):
    if not pymongo.uri_parser.parse_uri(uri)["database"]:
        raise ValueError(
            f"Invalid URI: {uri} " f"Did you forget to include a database?"
        )
    else:
        client = pymongo.MongoClient(uri, tls=tls)
        return client.get_database()


class DuplicateUniqueID(Exception):
    ...
