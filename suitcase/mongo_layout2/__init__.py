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
        return f'{type(self).__name__}(uri={self._uri})'

class DocBuffer():
    from threading import Lock

    def __init__(self, doc_type):
        self.empty = True
        self.event_buffer = defaultdict(lambda: defaultdict(dict)))
        self.mutex = Lock()
        self.dump_block = threading.Condition(self.mutex)

        if doc_type == "event":
            self.array_keys = set(["seq_num","time","uid"])
            self.dataframe_keys = set(["data","timestamps","filled"])
            self.stream_id = "descriptor"
        elif doc_type == "datum":
            self.array_keys = set(["datum_id"])
            self.dataframe_keys = set(["datum_kwargs"])
            self.stream_id = "resource"

    def insert(self, doc):
        self.mutex.acquire()
        try:
            __buffer_insert(doc)
            self.empty = False
        finally:
            self.mutex.release()
            self.dump_block.notify()

    def dump(self):
        while self.empty:
             self.dump_block.wait()

        self.mutex.acquire()
        try:
            event_buffer_dump = self.event_buffer
            self.event_buffer = defaultdict(lambda: defaultdict(dict)))
            self.empty = True
        finally:
            self.mutex.release()
        return event_buffer_dump

    def __buffer_insert(self, doc):
        for key, value in doc.items():
            if key in self.array_keys:
                self.event_buffer[doc[self.stream_id][key].append(value)
            elif key in self.dataframe_keys:
                for inner_key, inner_value in doc[key].items():
                    self.event_buffer[doc[self.stream_id]][key][inner_key].append(value)
            else:
                self.event_buffer[doc[self.stream_id][key] = value
