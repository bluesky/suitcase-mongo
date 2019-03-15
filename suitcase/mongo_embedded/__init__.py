import event_model
from ._version import get_versions
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from pymongo import UpdateOne
import threading
import sys
import time

__version__ = get_versions()['version']
del get_versions


class Serializer(event_model.DocumentRouter):
    """
    Insert bluesky documents into MongoDB using an embedded data model.

    This embedded data model has three collections: header, event,
    datum. The header collection includes start, stop, descriptor,
    and resource documents. The event_pages are stored in the event colleciton,
    and datum_pages are stored in the datum collection.

    To ensure data integrity two databases are required. A volatile database
    that allows the update of existing documents. And a permanent database that
    does not allow updates to documents. When the stop document is received
    from the RunEngine the volatile data is "frozen" by moving it to the
    permanent database.

    This Serializer ensures that when the stop document or close request is
    received all documents we be written to the volatile database. After
    everything has been successfully writen to volatile database, it will copy
    all of the runs documents to the permanent database, check that it has been
    correctly copied, and then delete the volatile data.

    This Serializer assumes that documents have been previously validated
    according to the bluesky event-model.

    Note that this Seralizer does not share the standard Serializer
    name or signature common to suitcase packages because it can only write
    via pymongo, not to an arbitrary user-provided buffer.

    Examples
    --------
    >>> from bluesky import RunEngine
    >>> from bluesky.plans import scan
    >>> from mongobox import MongoBox
    >>> from ophyd.sim import det, motor
    >>> from suitcase.mongo_embedded import Serializer

    >>> # Create two sandboxed mongo instances
    >>> volatile_box = MongoBox()
    >>> permanent_box = MongoBox()
    >>> volatile_box.start()
    >>> permanent_box.start()

    >>> # Get references to the mongo databases
    >>> volatile_db = volatile_box.client().db
    >>> permanent_db = permanent_box.client().db

    >>> # Create the Serializer
    >>> serializer = Serializer(volatile_db, permanent_db)

    >>> RE = RunEngine({})
    >>> RE.subscribe(serializer)

    >>> # Generate example data.
    >>> RE(scan([det], motor, 1, 10, 10))
    """

    def __init__(self, volatile_db, permanent_db, num_threads=1,
                 buffer_size=1000000, page_size=5000000, **kwargs):

        """
        Insert documents into MongoDB using an embedded data model.

        Parameters
        ----------
        volatile_db: pymongo database
            database for temporary storage
        permanent_db: pymongo database
            database for permanent storage
        num_theads: int, optional
            number of workers that read from the buffer and write to the
            database. Must be 5 or less. Default is 1.
        buffer_size: int, optional
            maximum size of the buffers in bytes. buffer_size + page_size must
            be less than 15000000. Default is 5000000
        page_size: int, optional
            The document size for event_page and datum_page documents. The
            maximum event/datum_page size is buffer_size + page_size.
            buffer_size + page_size must be less than 15000000.The
            default is 5000000.
        """

        # There is no performace improvment for more than 10 threads. Tests
        # validate for upto 10 threads.
        if num_threads > 10 or num_threads < 1:
            raise AttributeError(f"num_threads must be between 1 and 10"
                                 "inclusive.")

        if page_size < 1000:
            raise AttributeError(f"page_size must be >= 10000")

        # Maximum size of a document in mongo is 16MB. buffer_size + page_size
        # defines the biggest document that can be created.
        if buffer_size + page_size > 15000000:
            raise AttributeError(f"buffer_size: {buffer_size} + page_size: "
                                 "page_size} is greater then 15000000.")

        self._BUFFER_SIZE = buffer_size
        self._PAGE_SIZE = page_size
        self._permanent_db = permanent_db
        self._volatile_db = volatile_db
        self._event_buffer = DocBuffer('event', self._BUFFER_SIZE)
        self._datum_buffer = DocBuffer('datum', self._BUFFER_SIZE)
        self._kwargs = kwargs
        self._start_found = False
        self._run_uid = None
        self._frozen = False

        # _event_count and _datum_count are used for setting first/last_index
        # fields of event and datum pages
        self._event_count = defaultdict(lambda: 0)
        self._datum_count = defaultdict(lambda: 0)

        # These counters are used to track the total number of events and datum
        # that have been successfully inserted into the database.
        self._db_event_count = defaultdict(lambda: 0)
        self._db_datum_count = defaultdict(lambda: 0)

        # Start workers.
        self._event_executor = ThreadPoolExecutor(max_workers=1)
        self._datum_executor = ThreadPoolExecutor(max_workers=1)
        self._count_executor = ThreadPoolExecutor(max_workers=1)
        self._event_executor.submit(self._event_worker)
        self._datum_executor.submit(self._datum_worker)
        self._count_executor.submit(self._count_worker)

    def __call__(self, name, doc):
        # Before inserting into mongo, convert any numpy objects into built-in
        # Python types compatible with pymongo.
        sanitized_doc = event_model.sanitize_doc(doc)
        return super().__call__(name, sanitized_doc)

    def _event_worker(self):
        # Gets the event buffer and writes it to the volatile database.
        while not self._frozen:
            try:
                event_dump = self._event_buffer.dump()
                self._bulkwrite_event(event_dump)
                for descriptor_uid, event_page in event_dump.items():
                    self._db_event_count['count_' + descriptor_uid] += len(
                                          event_page['seq_num'])
            except BaseException as error:
                self._datum_buffer.worker_error = error
                self._event_buffer.worker_error = error

    def _datum_worker(self):
        # Gets the datum buffer and writes it to the volatile database.
        while not self._frozen:
            try:
                datum_dump = self._datum_buffer.dump()
                self._bulkwrite_datum(datum_dump)
                for resource_uid, datum_page in datum_dump.items():
                    self._db_datum_count['count_' + resource_uid] += len(
                                          datum_page['datum_id'])
            except BaseException as error:
                self._datum_buffer.worker_error = error
                self._event_buffer.worker_error = error

    def _count_worker(self):
        # Updates event_count and datum_count in the header document

        last_event_count = defaultdict(lambda: 0)
        last_datum_count = defaultdict(lambda: 0)

        while not self._frozen:
            try:
                time.sleep(5)

                # Only updates the header if the count has changed.
                if (
                        (sum(self._db_event_count.values()) >
                        sum(last_event_count.values()))
                        or (sum(self._db_datum_count.values()) >
                        sum(last_datum_count.values()))
                   ):
                    self._volatile_db.header.update_one(
                        {'run_id': self._run_uid},
                        {'$set': {**dict(self._db_event_count),
                                  **dict(self._db_datum_count)}})

                    last_event_count = self._db_event_count
                    last_datum_count = self._db_datum_count
            except BaseException as error:
                self._datum_buffer.worker_error = error
                self._event_buffer.worker_error = error

    def start(self, doc):
        self._check_start(doc)
        self._run_uid = doc['uid']
        self._insert_header('start', doc)
        self._insert_header('event_count', doc)
        self._insert_header('datum_count', doc)
        return doc

    def stop(self, doc):
        self._insert_header('stop', doc)
        self.close()
        return doc

    def descriptor(self, doc):
        self._insert_header('descriptors', doc)
        return doc

    def resource(self, doc):
        self._insert_header('resources', doc)
        return doc

    def event(self, doc):
        self._event_buffer.insert(doc)
        return doc

    def datum(self, doc):
        self._datum_buffer.insert(doc)
        return doc

    def event_page(self, doc):
        self._bulkwrite_event({doc['descriptor']: doc})
        return doc

    def datum_page(self, doc):
        self._bulkwrite_datum({doc['resource']: doc})
        return doc

    def close(self):
        self.freeze(self._run_uid)

    def freeze(self, run_uid):
        """
        Freeze the run by flushing the buffers and moving all of the run's
        documents to the permanent database. Can be used independantly to
        freeze a previous run.
        """
        # Freeze the serializer.
        self._frozen = True

        # Freeze the buffers.
        self._event_buffer.freeze()
        self._datum_buffer.freeze()

        # Wait for the workers to finish.
        self._count_executor.shutdown(wait=False)
        self._event_executor.shutdown(wait=True)
        self._datum_executor.shutdown(wait=True)

        # Flush the buffers if there is anything in them.
        if self._event_buffer.current_size > 0:
            event_dump = self._event_buffer.dump()
            self._bulkwrite_event(event_dump)
        if self._datum_buffer.current_size > 0:
            datum_dump = self._datum_buffer.dump()
            self._bulkwrite_datum(datum_dump)

        self._set_header('event_count', sum(self._event_count.values()))
        self._set_header('datum_count', sum(self._datum_count.values()))

        # Raise exception if buffers are not empty.
        assert self._event_buffer.current_size == 0
        assert self._datum_buffer.current_size == 0

        # Copy the run to the permanent database.
        volatile_run = self._get_run(self._volatile_db, run_uid)
        self._insert_run(self._permanent_db, volatile_run)
        permanent_run = self._get_run(self._permanent_db, run_uid)

        # Check that it has been copied correctly to permanent database, then
        # delete the run from the volatile database.
        if volatile_run != permanent_run:
            raise IOError("Failed to move data to permanent database.")
        else:
            self._volatile_db.header.drop()
            self._volatile_db.events.drop()
            self._volatile_db.datum.drop()

    def _get_run(self, db, run_uid):
        """
        Gets a run from a database. Returns a list of the run's documents.
        """
        run = list()

        # Get the header.
        header = db.header.find_one({'run_id': run_uid}, {'_id': False})
        if header is None:
            raise RuntimeError(f"Run not found {run_uid}")

        run.append(('header', header))

        # Get the events.
        if 'descriptors' in header.keys():
            for descriptor in header['descriptors']:
                run += [('event', doc) for doc in
                        db.event.find({'descriptor': descriptor['uid']},
                                      {'_id': False})]

        # Get the datum.
        if 'resources' in header.keys():
            for resource in header['resources']:
                run += [('datum', doc) for doc in
                        db.datum.find({'resource': resource['uid']},
                                      {'_id': False})]
        return run

    def _insert_run(self, db, run):
        """
        Inserts a run into a database. run is a list of the run's documents.
        """
        for collection, doc in run:
            db[collection].insert_one(doc)
            # del doc['_id'] is needed because insert_one mutates doc.
            del doc['_id']

    def _insert_header(self, name,  doc):
        """
        Inserts header document into the run's header document.
        """
        self._volatile_db.header.update_one({'run_id': self._run_uid},
                                            {'$push': {name: doc}},
                                            upsert=True)

    def _set_header(self, name,  doc):
        """
        Inserts header document into the run's header document.
        """
        self._volatile_db.header.update_one({'run_id': self._run_uid},
                                            {'$set': {name: doc}})

    def _bulkwrite_datum(self, datum_buffer):
        """
        Bulk writes datum_pages to Mongo datum collection.
        """
        operations = [self._updateone_datumpage(resource, datum_page)
                      for resource, datum_page in datum_buffer.items()]
        self._volatile_db.datum.bulk_write(operations, ordered=False)

    def _bulkwrite_event(self, event_buffer):
        """
        Bulk writes event_pages to Mongo event collection.
        """
        operations = [self._updateone_eventpage(descriptor, event_page)
                      for descriptor, event_page in event_buffer.items()]
        self._volatile_db.event.bulk_write(operations, ordered=False)

    def _updateone_eventpage(self, descriptor_id, event_page):
        """
        Creates the UpdateOne command that gets used with bulk_write.
        """
        event_size = sys.getsizeof(event_page)

        data_string = {'data.' + key: {'$each': value_array}
                       for key, value_array in event_page['data'].items()}

        timestamp_string = {'timestamps.' + key: {'$each': value_array}
                            for key, value_array
                            in event_page['timestamps'].items()}

        filled_string = {'filled.' + key: {'$each': value_array}
                         for key, value_array in event_page['filled'].items()}

        update_string = {**data_string, **timestamp_string, **filled_string}

        count = len(event_page['seq_num'])
        self._event_count[descriptor_id] += count

        return UpdateOne(
            {'descriptor': descriptor_id, 'size': {'$lt': self._PAGE_SIZE}},
            {'$push': {'uid': {'$each': event_page['uid']},
                       'time': {'$each': event_page['time']},
                       'seq_num': {'$each': event_page['seq_num']},
                       **update_string},
             '$inc': {'size': event_size},
             '$min': {'first_index': self._event_count[descriptor_id] - count},
             '$max': {'last_index': self._event_count[descriptor_id] - 1}},
            upsert=True)

    def _updateone_datumpage(self, resource_id, datum_page):
        """
        Creates the UpdateOne command that gets used with bulk_write.
        """
        datum_size = sys.getsizeof(datum_page)

        kwargs_string = {'datum_kwargs.' + key: {'$each': value_array}
                         for key, value_array
                         in datum_page['datum_kwargs'].items()}

        count = len(datum_page['datum_id'])
        self._datum_count[resource_id] += count

        return UpdateOne(
            {'resource': resource_id, 'size': {'$lt': self._PAGE_SIZE}},
            {'$push': {'datum_id': {'$each': datum_page['datum_id']},
                       **kwargs_string},
             '$inc': {'size': datum_size},
             '$min': {'first_index': self._datum_count[resource_id] - count},
             '$max': {'last_index': self._datum_count[resource_id] - 1}},
            upsert=True)

    def _check_start(self, doc):
        if self._start_found:
            raise RuntimeError(
                "The serializer in suitcase-mongo expects "
                "documents from one run only. Two `start` documents were "
                "received.")
        else:
            self._start_found = True


class DocBuffer():

    """
    DocBuffer is a thread-safe "embedding" buffer for bluesky event or datum
    documents.

    "embedding" refers to combining multiple documents from a stream of
    documents into a single document, where the values of matching keys are
    stored as a list, or dictionary of lists. "embedding" converts event docs
    to event_pages, or datum doc to datum_pages. event_pages and datum_pages
    are defined by the bluesky event-model.

    Events with different descriptors, or datum with different resources are
    stored in separate embedded documents in the buffer. The buffer uses a
    defaultdict so new embedded documents are automatically created when they
    are needed. The dump method totally clears the buffer. This mechanism
    automatically manages the lifetime of the embeded documents in the buffer.

    The doc_type argument which can be either 'event' or 'datum'.
    The the details of the embedding differ for event and datum documents.

    Internally the buffer is a dictionary that maps event decriptors to
    event_pages or datum resources to datum_pages.

    Parameters
    ----------
    doc_type: str
        {'event', 'datum'}
    buffer_size: int, optional
        Maximum buffer size in bytes.

    Attributes
    ----------
    current_size: int
        Current size of the buffer.

    """

    def __init__(self, doc_type, buffer_size):
        self.worker_error = None
        self._frozen = False
        self._mutex = threading.Lock()
        self._not_full = threading.Condition(self._mutex)
        self._not_empty = threading.Condition(self._mutex)
        self._doc_buffer = defaultdict(lambda: defaultdict(lambda:
                                                           defaultdict(list)))
        self.current_size = 0

        if (buffer_size >= 400) and (buffer_size <= 15000000):
            self._buffer_size = buffer_size
        else:
            raise AttributeError(f"Invalid buffer_size {buffer_size}, "
                                 "buffer_size must be between 10000 and "
                                 "15000000 inclusive.")

        # Event docs and datum docs are embedded differently, this configures
        # the buffer for the specified document type.
        if doc_type == "event":
            self._array_keys = set(["seq_num", "time", "uid"])
            self._dataframe_keys = set(["data", "timestamps", "filled"])
            self._stream_id_key = "descriptor"
        elif doc_type == "datum":
            self._array_keys = set(["datum_id"])
            self._dataframe_keys = set(["datum_kwargs"])
            self._stream_id_key = "resource"
        else:
            raise AttributeError(f"Invalid doc_type {doc_type}, doc_type must "
                                 "be either 'event' or 'datum'")

    def insert(self, doc):
        """
        Inserts a bluesky event or datum document into buffer. This method
        blocks if the buffer is full.

        Parameters
        ----------
        doc: json
            A validated bluesky event or datum document.
        """

        if self.worker_error:
            raise RuntimeError("Worker exception: " + str(self.worker_error))

        if self._frozen:
            raise RuntimeError("Cannot insert documents into a "
                               "frozen DocBuffer")

        doc_size = sys.getsizeof(doc)

        if doc_size >= self._buffer_size:
            raise RuntimeError("Failed to insert. Document size is greater "
                               "than max buffer size")

        # Block if buffer is full.
        with self._not_full:
            self._not_full.wait_for(lambda: (self.current_size + doc_size)
                                    < self._buffer_size)
            self._buffer_insert(doc)
            self.current_size += doc_size
            # Wakes up threads that are waiting to dump.
            self._not_empty.notify_all()

    def dump(self):
        """
        Get everything in the buffer and clear the buffer.  This method blocks
        if the buffer is empty.

        Returns
        -------
        doc_buffer_dump: dict
            A dictionary that maps event descriptor to event_page, or a
            dictionary that maps datum resource to datum_page.
        """
        # Block if the buffer is empty.
        # Don't block if the buffer is frozen, this allows all workers threads
        # finish when freezing the run.
        with self._not_empty:
            self._not_empty.wait_for(lambda: (
                                self.current_size or self._frozen))
            # Get a reference to the current buffer, create a new buffer.
            doc_buffer_dump = self._doc_buffer
            self._doc_buffer = defaultdict(lambda:
                                           defaultdict(lambda:
                                                       defaultdict(list)))
            self.current_size = 0
            # Wakes up all threads that are waiting to insert.
            self._not_full.notify_all()
            return doc_buffer_dump

    def _buffer_insert(self, doc):
        # Embed the doc in the buffer
        for key, value in doc.items():
            if key in self._array_keys:
                self._doc_buffer[doc[self._stream_id_key]][key] = list(
                    self._doc_buffer[doc[self._stream_id_key]][key])
                self._doc_buffer[doc[self._stream_id_key]][key].append(value)
            elif key in self._dataframe_keys:
                for inner_key, inner_value in doc[key].items():
                    (self._doc_buffer[doc[self._stream_id_key]][key]
                        [inner_key].append(inner_value))
            else:
                self._doc_buffer[doc[self._stream_id_key]][key] = value

    def freeze(self):
        """
        Freeze the buffer preventing new inserts and stop dump from blocking
        """
        self._frozen = True
        self._mutex.acquire()
        self._not_empty.notify_all()
        self._mutex.release()
