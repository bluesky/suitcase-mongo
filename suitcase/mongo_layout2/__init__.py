import event_model
from pathlib import Path
from ._version import get_versions
from mongobox import MongoBox
from pymongo import MongoClient
import concurrent.futures
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError
import sys

__version__ = get_versions()['version']
del get_versions


class Serializer(event_model.DocumentRouter):
    # how to prevent the same run from being frozen two times?
    # How do we integrate with the run engine?

    def __init__(self, permanent_db, volatile_db, num_threads=1, **kwargs):
        """
        Insert documents into MongoDB using layout v2.

        Note that this Seralizer does not share the standard Serializer
        name or signature common to suitcase packages because it can only write
        via pymongo, not to an arbitrary user-provided buffer.
        """

        self._permanent_db = permanent_db
        self._volatile_db = volatile_db
        self._event_buffer = DocBuffer('event')
        self._datum_buffer = DocBuffer('datum')
        kwargs.setdefault('cls', NumpyEncoder)
        self._kwargs = kwargs
        self._start_found = False
        self._run_uid = None
        self._frozen = False
        self.MAX_DOC_SIZE = 5000  #Units are KB

        with ThreadPoolExecutor(max_workers = num_threads) as executor:
            self._event_workers = executor.submit(_event_worker)
            self._datum_workers = executor.submit(_datum_worker)

    def __call__(self, name, doc):
        if self._frozen:
            raise RuntimeError("Cannot insert documents into a "
                               "frozen Serializer")
        sanitized_doc = doc.copy()
        _apply_to_dict_recursively(sanitized_doc, _sanitize_numpy)
        return super().__call__(name, sanitized_doc)

    def _event_worker(self):
        while not self._frozen:
            event_dump = self._event_buffer.dump()
            self._update_events(event_dump)

    def _datum_worker(self):
        while not self._frozen:
            datum_dump = self._datum_buffer.dump()
            self._update_datum(datum_dump)

    def start(self, doc):
        self._check_start(doc)
        self._run_uid = doc['uid']
        self._update_header('start', doc)
        return doc

    def stop(self, doc):
        self._update_header('stop', doc)
        self.close()
        return doc

    def descriptor(self, doc):
        self._update_header('descriptor', doc)
        return doc

    def resource(self, doc):
        self._update_header('resource', doc)
        return doc

    def event(self,doc):
        self._event_buffer.insert(doc)
        return doc

    def datum(self,doc):
        self._datum_buffer.insert(doc)
        return doc

    def event_page(self, doc):
        self._update_events({doc['descriptor']:doc})
        return doc

    def datum_page(self, doc):
        self._update_datum({doc['descriptor']:doc})
        return doc

    def close(self):
        self.freeze()

    def freeze(self):
        self._frozen = True
        self._event_workers.shutdown(wait=True)
        self._datum_workers.shutdown(wait=True)
        if not self._event_buffer.empty:
            self._event_worker()
        if not self._datum_buffer.empty:
            self._datum_worker()
        self._freeze_db()

    def _freeze_db(self):
        run = self._get_run(self._volatile_db, self._run_uid)
        self._insert_run(self._permanent_db, run)
        self._volatile_db.header.drop()
        self._volatile_db.events.drop()
        self._volatile_db.datum.drop()

    def _get_run(self, db, run_uid):
        run = list()
        header = db.header.find_one({'uid': run_uid})
        run.append(('header',header))
        for descriptor in header['descriptors']:
            run += [('event',doc) for doc in
                        db.events.find_many({'descriptor': descriptor})]
        for resource in header{'resources']:
            run += [('datum', doc) for doc in
                        db.datum.find_many({'resource': resource})]
        return run

    def _insert_run(self, db, run):
        for collection, doc in run:
            result = db[collection].insert_one(doc)

    def _update_header(self, name,  doc):
        self._volatile_db.header.update_one(
                        {'run_id': self.run_id}, {'$push':
                        {name: doc}, {upsert: true}})

    def _update_events(self, event_pages):
        #should eventually update to bulk write
        for descriptor, event in event_pages:
            event_size = sys.getsizeof(event)
            db.events.update(
                { 'descriptor': descriptor, size { $lt : MAX_DOC_SIZE} },
                {
                    $push: {
                        { 'uid' : {'$each' : event['uid']},
                          'time' : {'$each' : event['time']},
                          'seq_num' : {'$each' : event['seq_num']}
                        }
                    },
                    $inc: { 'size' : event_size }
                },
                { upsert: true })

    def _update_datum(self, datum_pages):
        #should eventually update to bulk write
        for resource, datum in datum_pages_pages:
            datum_size = sys.getsizeof(event)
            db.events.update(
                { 'resource': resource, size { $lt : MAX_DOC_SIZE} },
                {
                    $push: {
                        { 'datum_id' : {'$each' : datum['datum_id']}}
                    },
                    $inc: { 'size' : datum_size }
                },
                { upsert: true })

    def _check_start(self, doc):
        if self._start_found:
            raise RuntimeError(
                "The serializer in suitcase-mongo expects "
                "documents from one run only. Two `start` documents were "
                "received.")
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
        # Need to add code to check if buffer is full
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
        # I think i need an if statement here to check if there is something in
        # the buffer. 2 workers could be  calling dump at the same time ans
        # sleeping. then if something inserts it will wake both of the workers.
        # This could cause a worker to get an empty dictionary.
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
