# Tests should generate (and then clean up) any files they need for testing. No
# binary files should be included in the repository.
import json
import event_model
from suitcase.mongo_embedded import Serializer


def test_export(db_factory, example_data):
    run_dict = {'start' : {},
                'stop': {},
                'descriptor': [],
                'resource' : [],
                'event': [],
                'datum': []}
    documents = example_data()
    volatile_db = db_factory()
    permanent_db = db_factory()
    serializer = Serializer(volatile_db, permanent_db)
    for item in documents:
        item = event_model.sanitize_doc(item)
        edited_item = bulk_to_list(item)
        if edited_item[0] == 'event_page':
            run_dict['event'] += edited_item[1]
        elif item[0] == 'datum_page':
            run_dict['datum'] += edited_item[1]
        elif type(run_dict.get(item[0])) == list:
            run_dict[item[0]].append(item[1])
        else:
            run_dict[item[0]] = item[1]
        serializer(*item)

    frozen_run_dict = run_list_to_dict(get_embedded_run(
                                permanent_db, run_dict['start']['uid']))

    assert (json.dumps(run_dict, sort_keys=True)
            == json.dumps(frozen_run_dict, sort_keys=True))


def test_multithread(db_factory, example_data):
    run_dict = {'start' : {},
                'stop': {},
                'descriptor': [],
                'resource' : [],
                'event': [],
                'datum': []}
    documents = example_data()
    volatile_db = db_factory()
    permanent_db = db_factory()
    serializer = Serializer(volatile_db, permanent_db, num_threads=5)
    for item in documents:
        item = event_model.sanitize_doc(item)
        edited_item = bulk_to_list(item)
        if edited_item[0] == 'event_page':
            run_dict['event'] += edited_item[1]
        elif edited_item[0] == 'datum_page':
            run_dict['datum'] += edited_item[1]
        elif type(run_dict.get(item[0])) == list:
            run_dict[item[0]].append(item[1])
        else:
            run_dict[item[0]] = item[1]
        serializer(*item)

    frozen_run_dict = run_list_to_dict(get_embedded_run(
                                permanent_db, run_dict['start']['uid']))

    assert (json.dumps(run_dict, sort_keys=True)
            == json.dumps(frozen_run_dict, sort_keys=True))


def test_smallbuffer(db_factory, example_data):
    run_dict = {'start' : {},
                'stop': {},
                'descriptor': [],
                'resource' : [],
                'event': [],
                'datum': []}
    documents = example_data()
    volatile_db = db_factory()
    permanent_db = db_factory()
    serializer = Serializer(volatile_db, permanent_db, buffer_size=1000)
    for item in documents:
        item = event_model.sanitize_doc(item)
        edited_item = bulk_to_list(item)
        if edited_item[0] == 'event_page':
            run_dict['event'] += edited_item[1]
        elif edited_item[0] == 'datum_page':
            run_dict['datum'] += edited_item[1]
        elif type(run_dict.get(item[0])) == list:
            run_dict[item[0]].append(item[1])
        else:
            run_dict[item[0]] = item[1]
        serializer(*item)

    frozen_run_dict = run_list_to_dict(get_embedded_run(
                                permanent_db, run_dict['start']['uid']))

    assert (json.dumps(run_dict, sort_keys=True)
            == json.dumps(frozen_run_dict, sort_keys=True))


def test_smallpage(db_factory, example_data):
    run_dict = {'start' : {},
                'stop': {},
                'descriptor': [],
                'resource' : [],
                'event': [],
                'datum': []}
    documents = example_data()
    volatile_db = db_factory()
    permanent_db = db_factory()
    serializer = Serializer(volatile_db, permanent_db, page_size=10000)
    for item in documents:
        item = event_model.sanitize_doc(item)
        edited_item = bulk_to_list(item)
        if edited_item[0] == 'event_page':
            run_dict['event'] += edited_item[1]
        elif edited_item[0] == 'datum_page':
            run_dict['datum'] += edited_item[1]
        elif type(run_dict.get(item[0])) == list:
            run_dict[item[0]].append(item[1])
        else:
            run_dict[item[0]] = item[1]
        serializer(*item)

    frozen_run_dict = run_list_to_dict(get_embedded_run(
                                permanent_db, run_dict['start']['uid']))

    assert (json.dumps(run_dict, sort_keys=True)
            == json.dumps(frozen_run_dict, sort_keys=True))

def run_list_to_dict(embedded_run_list):
    run_dict = {'start' : {},
                'stop': {},
                'descriptor': [],
                'resource' : [],
                'event': [],
                'datum': []}

    header = embedded_run_list[0][1]
    print("HEADER", header)
    run_dict['start'] = header['start'][0]
    run_dict['stop'] = header['stop'][0]
    run_dict['descriptor'] = header.get('descriptors', [])
    run_dict['resource'] = header.get('resources', [])

    for name, doc in embedded_run_list[1:]:
        if name  == 'event':
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

def bulk_to_list(doc):
    """
    Converts bulk_events/datum or event/datum_page to lists.
    """
    doc = list(doc)

    if doc[0] == 'bulk_event':
        doc[0] = 'event_page'
        doc[1] = event_model.bulk_events_to_event_pages(doc[1])
    elif doc[0] == 'bulk_datum':
        doc[0] = 'datum_page'
        doc[1] = event_model.bulk_datum_to_datum_pages(doc[1])

    if doc[0] == 'event_page':
        doc[1] = list(event_model.unpack_event_page(doc[1]))
    elif doc[0] == 'datum_page':
        doc[1] = list(event_model.unpack_datum_page(doc[1]))

    return tuple(doc)
