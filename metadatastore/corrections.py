from __future__ import (unicode_literals, print_function, division,
                        absolute_import)
import six
from functools import wraps
from . import conf
from .commands import (_ensure_connection, find_run_starts,
                       _get_mongo_document, db_disconnect, db_connect,
                       _dereference_uid_fields)

from mongoengine.context_managers import switch_collection
from mongoengine.connection import (register_connection, disconnect,
                                    get_connection, connect)
from .document import Document

from mongoengine.document import DynamicDocument
from mongoengine.fields import StringField, ReferenceField
import metadatastore
from metadatastore.commands import _AsDocument, find_corrections
from metadatastore.odm_templates import BeamlineConfig
from uuid import uuid4
import time as ttime
from collections import Mapping, Iterable

class Foo(object):
    pass

# subclass the odm tempaltes
from .odm_templates import (RunStart, EventDescriptor, BeamlineConfig,
                            RunStop, Correction, Event)

def _replace_embedded_document(event_descriptor):
    new_data_keys = {}
    for k, v in six.iteritems(event_descriptor.data_keys):
        new_data_keys[k] = {k1: v1 for k1, v1 in six.iteritems(v)}
    return new_data_keys


def update(mds_document, correction_uid=None):
    """Update a metadatastore document

    Note that this does not actually touch the raw data, it creates a
    RunStart document in a separate document with only the diff from the
    original RunStart document

    Parameters
    ----------
    mds_document : metadatastore.document.Document
        A metadatastore document to update
    uid : str, optional
        Globally unique id string provided to metadatastore. Defaults to uuid4

    Returns
    -------
    correction_document : metadatastore.odm_templates.Correction
    """
    if correction_uid is None:
        correction_uid = str(uuid4())

    if mds_document._name == 'Event':
        raise ValueError("You are not allowed to modify Events")

    if mds_document._name in ['BeamlineConfig']:
        mds_cls = globals()[mds_document._name]
        c = mds_cls(time=ttime.time(), uid=correction_uid)
    else:
        c = Correction(uid=mds_document.uid, correction_uid=correction_uid,
                       time=ttime.time())
        original_document_type = mds_document._name
        if mds_document._name == "Correction":
            original_document_type = mds_document.original_document_type

        setattr(c, 'original_document_type', original_document_type)
    # skipping uid because it is already taken care of in the initialization
    # of the correction document
    fields_to_skip = ['uid', 'correction_uid', 'time']

    # have to look for Embedded Documents and turn them into straight
    # dictionaries
    if mds_document._name == "EventDescriptor":
        mds_document.data_keys = _replace_embedded_document(mds_document)

    for k, v in six.iteritems(mds_document):
        if k in fields_to_skip:
            continue
        if isinstance(v, metadatastore.document.Document):
            # reference document objects with their uid only
            # this will be dereferenced via the `_dereference_uid_fields`
            # function in `metadatastore.commands.py`
            document = update(v)
            setattr(c, k, document.uid)
        else:
            setattr(c, k, v)

    original_document_type = mds_document._name

    if mds_document._name == "Correction":
        original_document_type = mds_document.original_document_type
        setattr(c, 'original_document_type', original_document_type)
    print(vars(c))
    c.save(validate=True, write_concern={"w": 1})
    return _AsDocument()(_dereference_uid_fields(c))
