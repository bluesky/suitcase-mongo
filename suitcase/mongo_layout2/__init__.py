import event_model
from pathlib import Path
from ._version import get_versions


__version__ = get_versions()['version']
del get_versions


class Serializer(event_model.DocumentRouter):
    def __init__(self, uri):
        """
        Insert documents into MongoDB using layout v2.

        This layout uses ....
        
        Note that this Seralizer does not share the standard Serializer
        name or signature common to suitcase packages because it can only write
        via pymongo, not to an arbitrary user-provided buffer.
        """
        self.__client = pymongo.MongoClient(uri)
        try:
            # Called with no args, get_database() returns the database
            # specified in the uri --- or raises if there was none. There is no
            # public method for checking this in advance, so we just catch the
            # error.
            db = self._metadatastore_client.get_database()
        except pymongo.errors.ConfigurationError as err:
            raise ValueError(
                f"Invalid uri: {uri} "
                f"Did you forget to include a database?") from err

    def __call__(self, name, doc):
        # Before inserting into mongo, convert any numpy objects into built-in
        # Python types compatible with pymongo.
        sanitized_doc = doc.copy()
        _apply_to_dict_recursively(sanitized_doc, _sanitize_numpy)
        return super().__call__(name, sanitized_doc)

    def start(self, doc):
        ...

    def descriptor(self, doc):
        ...

    def resource(self, doc):
        ...

    def event_page(self, doc):
        ...

    def datum_page(self, doc):
        ...

    def stop(self, doc):
        self._run_stop_collection.insert_one(doc)

    def __repr__(self):
        # Display connection info in eval-able repr.
        return f'{type(self).__name__}(uri={self._uri!r})'
