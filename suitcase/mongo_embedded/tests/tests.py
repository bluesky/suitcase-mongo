# Tests should generate (and then clean up) any files they need for testing. No
# binary files should be included in the repository.
import json
import event_model
from suitcase.mongo_embedded import Serializer
import pytest


def test_export(db_factory, example_data):
    """
    Test suitcase-mongo-embedded serializer with default parameters.
    """
    permanent_db = db_factory()
    serializer = Serializer(permanent_db)
    run(example_data, serializer, permanent_db)
    if not serializer._frozen:
        serializer.close()


def test_multithread(db_factory, example_data):
    """
    Test suitcase-mongo-embedded serializer with multiple worker threads.
    """
    permanent_db = db_factory()
    serializer = Serializer(permanent_db, num_threads=5)
    run(example_data, serializer, permanent_db)
    if not serializer._frozen:
        serializer.close()


def test_smallbuffer(db_factory, example_data):
    """
    Test suitcase-mongo-embedded serializer with a small buffer.
    """
    permanent_db = db_factory()
    serializer = Serializer(permanent_db, embedder_size=3000)
    run(example_data, serializer, permanent_db)
    if not serializer._frozen:
        serializer.close()


def test_smallqueue(db_factory, example_data):
    """
    Test suitcase-mongo-embedded serializer with a small buffer.
    """
    permanent_db = db_factory()
    serializer = Serializer(permanent_db, queue_size=1)
    run(example_data, serializer, permanent_db)
    if not serializer._frozen:
        serializer.close()


def test_smallpage(db_factory, example_data):
    """
    Test suitcase-mongo-embedded serializer with a small mongo page saize.
    """
    permanent_db = db_factory()
    serializer = Serializer(permanent_db, page_size=10000)
    run(example_data, serializer, permanent_db)
    if not serializer._frozen:
        serializer.close()


def test_evil_db(db_factory, example_data):
    """
    Test suitcase-mongo-embedded serializer with a db that raises an exception
    on bulk_write.
    """
    def evil_func(*args, **kwargs):
        raise RuntimeError

    permanent_db = db_factory()
    serializer = Serializer(permanent_db)
    serializer._bulkwrite_event = evil_func
    serializer._bulkwrite_datum = evil_func
    with pytest.raises(RuntimeError):
        run(example_data, serializer, permanent_db)
    if not serializer._frozen:
        serializer.close()


def run(example_data, serializer, permanent_db):
    """
    Testbench for suitcase-mongo-embedded serializer.
    This stores all documents that are going to the serializer into a
    dictionary. After the run completes, it then queries the permanent
    mongo database, and reads the documents to a separate dictionary. The two
    dictionaries are checked to see if they match.
    """
    run_dict = {'start': {}, 'stop': {}, 'descriptor': [],
                'resource': [], 'event': [], 'datum': []}
    documents = example_data()
    mongo_serializer = serializer
    for item in documents:

        # Fix formatting for JSON.
        item = event_model.sanitize_doc(item)
        # Send the bluesky doc to the serializer
        mongo_serializer(*item)

        # Bulk_event/datum need to be converted to a list of events/datum
        # before inserting in the run_dict.
        if item[0] in {'bulk_events', 'bulk_datum'}:
            pages = bulk_to_pages(*item)
            doc_list = pages_to_list(pages)
            for doc in doc_list:
                run_dict[doc[0]].append(doc[1])
        else:
            if item[0] in {'event_page', 'datum_page'}:
                doc_list = page_to_list(*item)
                for doc in doc_list:
                    run_dict[doc[0]].append(doc[1])
            else:
                if type(run_dict.get(item[0])) == list:
                    run_dict[item[0]].append(item[1])
                else:
                    run_dict[item[0]] = item[1]

    # Read the run from the mongo database and store in a dict.
    frozen_run_dict = run_list_to_dict(get_embedded_run(
                                permanent_db, run_dict['start']['uid']))

    # Sort the event field of each dictionary. With multiple streams, the
    # documents that don't go through the serializer don't appear to be sorted
    # correctly.
    if len(run_dict['event']):
        run_dict['event'] = sorted(run_dict['event'],
                                   key=lambda x: x['descriptor'])
        frozen_run_dict['event'] = sorted(frozen_run_dict['event'],
                                          key=lambda x: x['descriptor'])
    # Compare the two dictionaries.
    assert (json.loads(json.dumps(run_dict, sort_keys=True))
            == json.loads(json.dumps(frozen_run_dict, sort_keys=True)))


def run_list_to_dict(embedded_run_list):
    """
    Converts a run from the mongo database to a dictionary.
    """
    run_dict = {'start': {},
                'stop': {},
                'descriptor': [],
                'resource': [],
                'event': [],
                'datum': []}

    header = embedded_run_list[0][1]
    run_dict['start'] = header['start'][0]
    run_dict['stop'] = header['stop'][0]
    run_dict['descriptor'] = header.get('descriptors', [])
    run_dict['resource'] = header.get('resources', [])

    for name, doc in embedded_run_list[1:]:
        if name == 'event':
            run_dict['event'] += list(event_model.unpack_event_page(doc))
        elif name == 'datum':
            run_dict['datum'] += list(event_model.unpack_datum_page(doc))

    return run_dict


def get_embedded_run(db, run_uid):
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


def bulk_to_pages(name, doc):
    """
    Converts bulk_events/datum to event/datum_page.
    """
    key_map = {'bulk_events': 'event_page', 'bulk_datum': 'datum_page'}

    if name == 'bulk_events':
        doc = event_model.bulk_events_to_event_pages(doc)
    elif name == 'bulk_datum':
        doc = event_model.bulk_datum_to_datum_pages(doc)

    page_list = [[key_map[name], item] for item in doc]

    return page_list


def pages_to_list(pages):
    """
    Converts event/datum_page to event/datum lists.
    """
    doc_list = []

    for page in pages:
        if page[0] == 'event_page':
            doc_list.extend([['event', event] for event
                            in event_model.unpack_event_page(page[1])])
        if page[0] == 'datum_page':
            doc_list.extend([['datum', datum] for datum
                            in event_model.unpack_datum_page(page[1])])

    return doc_list


def page_to_list(name, page):
    """
    Converts event/datum_page to event/datum lists.
    """
    doc_list = []

    if name == 'event_page':
        doc_list.extend([['event', event] for event
                        in event_model.unpack_event_page(page)])
    if name == 'datum_page':
        doc_list.extend([['datum', datum] for datum
                        in event_model.unpack_datum_page(page)])

    return doc_list
