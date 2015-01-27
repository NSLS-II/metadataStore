from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import six
import uuid

import mongoengine
import mongoengine.connection
from mongoengine.context_managers import switch_db

from nose.tools import make_decorator
from nose.tools import assert_equal, assert_raises


from metadataStore.database import (BeamlineConfig, EventDescriptor,
                                    Event, Header)
import metadataStore.commands as mdsc

db_name = str(uuid.uuid4())
dummy_db_name = str(uuid.uuid4())


def setup():
    # need to make 'default' connection to point to no-where, just to be safe
    mongoengine.connect(dummy_db_name)
    # connect to the db we are actually going to use
    mongoengine.connect(db_name, alias='test_db')


def teardown():
    conn = mongoengine.connection.get_connection('test_db')
    conn.drop_database(db_name)
    conn.drop_database(dummy_db_name)


def context_decorator(func):
    def inner(*args, **kwargs):
        with switch_db(BeamlineConfig, 'test_db'), \
          switch_db(EventDescriptor, 'test_db'), \
          switch_db(Event, 'test_db'), \
          switch_db(Header, 'test_db'):
            func(*args, **kwargs)

    return make_decorator(func)(inner)


@context_decorator
def _blc_tester(config_dict):
    blc = mdsc.save_beamline_config(config_dict)
    BeamlineConfig.objects.get(id=blc.id)
    if config_dict is None:
        config_dict = dict()
    assert_equal(config_dict, blc.config_params)


def test_blc_insert():
    for cfd in [None, {}, {'foo': 'bar', 'baz': 5, 'biz': .05}]:
        yield _blc_tester, cfd


@context_decorator
def _ev_desc_tester(event_type_id, descriptor_name,
                    data_keys, type_descriptor):
    print(data_keys)
    ev_desc = mdsc.save_event_descriptor(event_type_id, descriptor_name,
                             data_keys, type_descriptor=type_descriptor)

    ret = EventDescriptor.objects.get(id=ev_desc.id)
    for k, v in zip(['event_type_id', 'descriptor_name',
                     'data_keys', 'type_descriptor'],
                     [event_type_id, descriptor_name,
                      data_keys, type_descriptor]
                     ):

        assert_equal(getattr(ret, k), v)


def test_ev_desc():
    test_vals = [(0, 'ascan', {'a': {'source': 'PV:A'},
                               'b': {'source': 'PV:b'},
                               'c': {'source': 'CCD',
                                     'external': 'FS'}}, {}),
                 ]
    for eti, dn, dk, td in test_vals:
        yield _ev_desc_tester, eti, dn, dk, td


def test_dict_key_replace_rt():
    test_d = {'a.b': 1, 'b': .5, 'c.d.e': None}
    src_in, dst_in = mdsc.__src_dst('in')
    test_d_in = mdsc.__replace_dict_keys(test_d, src_in, dst_in)
    src_out, dst_out = mdsc.__src_dst('out')
    test_d_out = mdsc.__replace_dict_keys(test_d_in, src_out, dst_out)
    assert_equal(test_d_out, test_d)


def test_src_dst_fail():
    assert_raises(ValueError, mdsc.__src_dst, 'aardvark')
