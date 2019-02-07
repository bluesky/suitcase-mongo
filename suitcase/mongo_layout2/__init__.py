import event_model
from pathlib import Path
from ._version import get_versions
from mongobox import MongoBox
from pymongo import MongoClient
import concurrent.futures

__version__ = get_versions()['version']
del get_versions


class Serializer(event_model.DocumentRouter):

    def __init__(self, permanent_uri, volatile_uri="mongobox", **kwargs):
        """
        Insert documents into MongoDB using layout v2.

        This layout uses ....

        Note that this Seralizer does not share the standard Serializer
        name or signature common to suitcase packages because it can only write
        via pymongo, not to an arbitrary user-provided buffer.

        The uri's must specify the database name.
        """

        self._permanent_client = None
        self._volatile_client = None
        self._event_buffer = DocBuffer('event')
        self._datum_buffer = DocBuffer('datum')
        kwargs.setdefault('cls', NumpyEncoder)
        self._kwargs = kwargs
        self._start_found = False

        try:
            self._permanent_client = pymongo.MongoClient(permanent_uri)
        except pymongo.errors.ConfigurationError as err:
            raise ValueError(f"Invalid uri: {permanent_uri} ") from err

        if volatile_uri == 'mongobox':
            box = MongoBox()
            box.start()
            self._volatile_client = box.client()
        else
            try:
                self._volatile_client = pymongo.MongoClient(volatile_uri)
            except pymongo.errors.ConfigurationError as err:
                raise ValueError(f"Invalid uri: {volatile_uri} ") from err

        with ThreadPoolExecutor(max_workers=1) as executor:
            self._event_workers = executor.submit(_event_worker)
            self._datum_workers = executor.submit(_datum_worker)

    def __call__(self, name, doc):
        # Before inserting into mongo, convert any numpy objects into built-in
        # Python types compatible with pymongo.
        sanitized_doc = doc.copy()
        _apply_to_dict_recursively(sanitized_doc, _sanitize_numpy)
        return super().__call__(name, sanitized_doc)

    def _event_worker(self):
        #TODO should be updated with bulk write
        while True:
            event_dump = _event_buffer.dump()
            for descriptor, page in event_dump.items():
                event_page(page)

    def _datum_worker(self):
        #TODO should be updated with bulk write
        while True:
            datum_dump = _datum_buffer.dump()
            for descriptor, page in datum_dump.items():
                datum_page(page)

    def start(self, doc):
        self._check_start(doc)
        result = self._volatile_client.header.insert_one(doc)
        return doc

    def stop(self, doc):
        result = self._volatile_client.header.insert_one(doc)
        self.freeze()
        return doc

    def descriptor(self, doc):
        result = self._volatile_client.header.insert_one(doc)
        return doc

    def resource(self, doc):
        result = self._volatile_client.header.insert_one(doc)
        return doc

    def event_page(self, doc):
        return doc

    def datum_page(self, doc):
        return doc

    def event(self,doc):
        self._event_buffer.insert(doc)
        return doc

    def datum(self,doc):
        self._datum_buffer.insert(doc)
        return doc

    def freeze(self):
        if not self._event_buffer.empty:
            for descriptor, page in self._event_buffer.dump().items():
               self.event_page(page)
        if not self._datum_buffer.empty:
            for descriptor, page in self_datum_buffer.dump().items():
               self.datum_page(page)
        self.volatile_to_perminant_db()

    def volatile_to_permanent_db(self):
        ...


    def _check_start(self, doc):
        if self._start_found:
            raise RuntimeError(
                "The serializer in suitcase-mongo expects "
                "documents from one run only. Two `start` documents were "
                "sent to it")
        else:
            self._start_found = True

    def __repr__(self):
        # Display connection info in eval-able repr.
        return f'{type(self).__name__}(uri={self._uri})'


class DocBuffer():

    """
    DocBuffer is a thread-safe "embedding" buffer for bluesky event or datum
    documents.

    "embedding" refers to combining multiple documents from a stream of
    documents into a single document, where the values of matching keys are
    stored as a list, or dictionary of lists.

    DocBuffer has two public methods, insert, and dump. Insert, embedes an
    event or datum document in the buffer and blocks if the buffer is full.
    Dump returns a reference to the buffer and creates a new buffer for new
    inserts. Dump blocks if the buffer is empty.

    Events with different descriptors, or datum with different resources are
    stored in separate embedded documents in the buffer. The buffer uses a
    defaultdict so new embedded documents are automatically created when they
    are needed. The dump method totally clears the buffer. This mechanism
    automatically manages the lifetime of the embeded documents in the buffer.

    The doc_type argument which can be either 'event' or 'datum'.
    The the details of the embedding differ for event and datum documents.
    """

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
            _buffer_insert(doc)
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

    def _buffer_insert(self, doc):
        for key, value in doc.items():
            if key in self.array_keys:
                self.event_buffer[doc[self.stream_id][key].append(value)
            elif key in self.dataframe_keys:
                for inner_key, inner_value in doc[key].items():
                    self.event_buffer[doc[self.stream_id]][key][inner_key].append(value)
            else:
                self.event_buffer[doc[self.stream_id][key] = value
