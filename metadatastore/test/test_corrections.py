from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import time as ttime
import datetime

import pytz
from nose.tools import assert_equal, assert_raises, raises, assert_not_equal
import metadatastore.commands as mdsc
from metadatastore.utils.testing import mds_setup, mds_teardown
from metadatastore.examples.sample_data import temperature_ramp
from metadatastore.api import (find_run_starts, find_run_stops,
                               find_event_descriptors, find_beamline_configs,
                               find_events, find_corrections)
from metadatastore.corrections import update, _replace_embedded_document
from itertools import product
import logging
loglevel = logging.DEBUG
logger = logging.getLogger(__name__)
logger.setLevel(loglevel)
handler = logging.StreamHandler()
handler.setLevel(loglevel)
logger.addHandler(handler)

# some useful globals
blc_uid = None
run_start_uid = None
document_insertion_time = None
descriptor1_uid = None
descriptor2_uid = None
run_stop_uid = None

#### Nose setup/teardown methods ###############################################

def teardown():
    mds_teardown()


def setup():
    mds_setup()
    global blc_uid, run_start_uid, document_insertion_time, run_stop_uid
    global descriptor1_uid, descriptor2_uid
    document_insertion_time = ttime.time()
    blc_uid = mdsc.insert_beamline_config({}, time=document_insertion_time)
    run_start_uid = mdsc.insert_run_start(scan_id=3022013,
                                          beamline_id='testbed',
                                          beamline_config=blc_uid,
                                          owner='tester',
                                          group='awesome-devs',
                                          project='Nikea',
                                          time=document_insertion_time)
    run_stop_uid = mdsc.insert_run_stop(run_start=run_start_uid,
                                        time=ttime.time())
    temperature_ramp.run(run_start_uid=run_start_uid)
    descriptors = find_event_descriptors(run_start=run_start_uid)
    descriptor1_uid = next(descriptors).uid
    descriptor2_uid = next(descriptors).uid


def _insert_document_helper(find_function, uid, attr_name, attr_value):
    # find the original document
    doc1, = find_function(uid=uid)
    # add a new field
    setattr(doc1, attr_name, attr_value)
    # save it
    doc2 = update(doc1)
    # reset doc1 to the original value
    doc1, = find_function(uid=uid, use_newest_correction=False)
    # search for it from metadatastore
    doc3, = find_function(uid=doc2.uid)
    # make sure that doc2 and doc3 have the same sample_to_detector_distance
    try:
        doc1.sample_to_detector_distance
        raise RuntimeError("doc1 is not supposed to have a "
                           "sample_to_detector_distance property. Something has"
                           " gone wrong")
    except AttributeError:
        # behaving properly
        pass
    # make sure that doc2 and doc3 have the same value for
    # sample_to_detector_distance
    assert_equal(getattr(doc2, attr_name), getattr(doc3, attr_name))

    return doc1, doc2, doc3

def test_update_beamline_config():
    doc1, doc2, doc3 = _insert_document_helper(find_beamline_configs,
                                               blc_uid,
                                               'sample_to_detector_distance',
                                               100)
    # make sure that all are beamline config documents
    assert_equal(doc1._name, doc2._name)
    assert_equal(doc2._name, doc3._name)
    # make sure doc2 and doc3 have the same uid fields
    assert_equal(doc2.uid, doc3.uid)
    # make sure that doc2 has a different uid from doc1
    assert_not_equal(doc2.uid, doc1.uid)


def _update_document_helper(find_function, document_name, doc1, doc2, doc3):
    # make sure that all are beamline config documents
    assert_equal(doc1._name, document_name)
    assert_equal(doc2._name, 'Correction')
    assert_equal(doc2._name, doc3._name)
    # make sure all uids are the same
    assert_equal(doc1.uid, doc2.uid)
    assert_equal(doc2.uid, doc3.uid)

    # exercise the use_newest_correction kwarg
    doc4, = find_function(uid=doc1.uid, use_newest_correction=False)
    assert_equal(doc1.uid, doc4.uid)
    doc5, = find_function(uid=doc1.uid)
    assert_equal(doc3.uid, doc5.uid)
    return doc4, doc5


def test_update_run_start():
    doc1, doc2, doc3 = _insert_document_helper(
        find_run_starts, run_start_uid, 'published', True)
    doc4, doc5 = _update_document_helper(
        find_run_starts, "RunStart", doc1, doc2, doc3)

    # check beamline config updating is working as expected


def test_update_run_stop():
    doc1, doc2, doc3 = _insert_document_helper(find_run_stops,
                                               run_stop_uid,
                                               'published', True)
    _update_document_helper(find_run_stops, "RunStop", doc1, doc2, doc3)

    original_run_start, = find_run_starts(uid=run_start_uid,
                                          use_newest_correction=False)
    # ensure there is an updated run start document in the Corrections
    # collection
    update(original_run_start)
    newest_run_start, = find_run_starts(uid=run_start_uid,
                                        use_newest_correction=True)

    newest_run_stop, = find_run_stops(uid=run_stop_uid,
                                      use_newest_correction=True)
    original_run_stop, = find_run_stops(uid=run_stop_uid,
                                        use_newest_correction=False)

    # Make sure that the newest_run_stop is a correction and that its
    # run_start is also a correction and that their uid's are equivalent
    assert_equal(newest_run_stop.run_start.correction_uid,
                 newest_run_start.correction_uid)
    # Make sure that the original run stop can still be found with the
    # `use_newest_correction` flag and that its run_start is also a raw document
    assert_equal(original_run_start.uid, original_run_stop.run_start.uid)
    for doc in [original_run_start, original_run_stop]:
        try:
            doc.correction_uid
            raise Exception("The original document should not be a "
                            "`Correction` document. And yet it is. Puzzling. "
                            "document = {}".format(doc))
        except AttributeError:
            # proper behavior
            pass


def test_update_event_descriptor():
    doc1, doc2, doc3 = _insert_document_helper(find_event_descriptors,
                                               descriptor1_uid,
                                               'awesome_data_stream', True)
    _update_document_helper(find_event_descriptors, "EventDescriptor", doc1,
                            doc2, doc3)

    original_run_start, = find_run_starts(uid=run_start_uid,
                                          use_newest_correction=False)
    # ensure there is an updated run start document in the Corrections
    # collection
    update(original_run_start)
    newest_run_start, = find_run_starts(uid=run_start_uid,
                                        use_newest_correction=True)

    newest_descriptor, = find_event_descriptors(uid=descriptor1_uid,
                                                use_newest_correction=True)
    original_descriptor, = find_event_descriptors(uid=descriptor1_uid,
                                                  use_newest_correction=False)

    # Make sure that the newest_run_stop is a correction and that its
    # run_start is also a correction and that their uid's are equivalent
    assert_equal(newest_descriptor.run_start.correction_uid,
                 newest_run_start.correction_uid)
    # Make sure that the original run stop can still be found with the
    # `use_newest_correction` flag and that its run_start is also a raw document
    assert_equal(original_run_start.uid, original_descriptor.run_start.uid)
    for doc in [original_run_start, original_descriptor]:
        try:
            doc.correction_uid
            raise Exception("The original document should not be a "
                            "`Correction` document. And yet it is. Puzzling. "
                            "document = {}".format(doc))
        except AttributeError:
            # proper behavior
            pass


def test_find_corrections():
    original_run_start, = find_run_starts(uid=run_start_uid,
                                          use_newest_correction=False)
    newest_run_start, = find_run_starts(uid=run_start_uid)

    run_start_corrections = list(find_corrections(uid=run_start_uid))
    
    assert_equal(run_start_corrections[0].correction_uid,
                 newest_run_start.correction_uid)
    assert_equal(run_start_corrections[0].uid,
                 original_run_start.uid)
    assert_equal(run_start_corrections[0].uid,
                 newest_run_start.uid)



def test_replace_embedded_document():
    descriptor1, = find_event_descriptors(uid=descriptor1_uid)
    assert(isinstance(descriptor1.data_keys, dict))
