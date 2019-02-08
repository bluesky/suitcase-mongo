import event_model
import pymongo
from ._version import get_versions


__version__ = get_versions()['version']
del get_versions


class Serializer(event_model.DocumentRouter):
    def __init__(self, metadatastore_client, asset_registry_client):
        """
        Insert documents into MongoDB using layout v1.

        This layout uses a separate Mongo collection per document type and a
        separate Mongo document for each logical document.

        Note that this Seralizer does not share the standard Serializer
        name or signature common to suitcase packages because it can only write
        via pymongo, not to an arbitrary user-provided buffer.
        """
        self._metadatastore_client = metadatastore_client
        self._asset_registry_client = asset_registry_client

        try:
            # Called with no args, get_database() returns the database
            # specified in the client's uri --- or raises if there was none.
            # There is no public method for checking this in advance, so we
            # just catch the error.
            mds_db = self._metadatastore_client.get_database()
        except pymongo.errors.ConfigurationError as err:
            raise ValueError(
                f"Invalid metadatastore_client: {metadatastore_client} "
                f"Did you forget to include a database?") from err
        try:
            assets_db = self._asset_registry_client.get_database()
        except pymongo.errors.ConfigurationError as err:
            raise ValueError(
                f"Invalid asset_registry_client: {asset_registry_client} "
                f"Did you forget to include a database?") from err

        self._run_start_collection = mds_db.get_collection('run_start')
        self._run_stop_collection = mds_db.get_collection('run_stop')
        self._event_descriptor_collection = mds_db.get_collection('event_descriptor')
        self._event_collection = mds_db.get_collection('event')

        self._resource_collection = assets_db.get_collection('resource')
        self._datum_collection = assets_db.get_collection('datum')

    def __call__(self, name, doc):
        # Before inserting into mongo, convert any numpy objects into built-in
        # Python types compatible with pymongo.
        sanitized_doc = event_model.sanitize_doc(doc)
        return super().__call__(name, sanitized_doc)

    def start(self, doc):
        self._run_start_collection.insert_one(doc)

    def descriptor(self, doc):
        self._event_descriptor_collection.insert_one(doc)

    def resource(self, doc):
        self._resource_collection.insert_one(doc)

    def event(self, doc):
        self._event_collection.insert_one(doc)

    def event_page(self, doc):
        # Unpack an EventPage into Events and do the actual insert inside
        # the `event` method. (This is the oppose what DocumentRouter does by
        # default.)

        event_method = self.event  # Avoid attribute lookup in hot loop.
        filled_events = []

        for event_doc in event_model.unpack_event_page(doc):
            filled_events.append(event_method(event_doc))

    def datum(self, doc):
        self._datum_collection.insert_one(doc)

    def datum_page(self, doc):
        # Unpack an DatumPage into Datum and do the actual insert inside
        # the `datum` method. (This is the oppose what DocumentRouter does by
        # default.)

        datum_method = self.datum  # Avoid attribute lookup in hot loop.
        filled_datums = []

        for datum_doc in event_model.unpack_datum_page(doc):
            filled_datums.append(datum_method(datum_doc))

    def stop(self, doc):
        self._run_stop_collection.insert_one(doc)

    def __repr__(self):
        # Display connection info in eval-able repr.
        return (f'{type(self).__name__}('
                f'metadatastore_client={self._metadatastore_client!r}, '
                f'asset_registry_client={self._asset_registry_client!r})')
