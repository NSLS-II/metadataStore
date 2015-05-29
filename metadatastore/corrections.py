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
from uuid import uuid4
import time as ttime

class Foo(object):
    pass

# subclass the odm tempaltes
from .odm_templates import (RunStart, EventDescriptor, BeamlineConfig,
                            RunStop, Correction, Event)


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

    c = Correction(uid=mds_document.uid, correction_uid=correction_uid,
                   time=ttime.time())
    # skipping uid because it is already taken care of in the initialization
    # of the correction document
    fields_to_skip = ['uid', 'correction_uid', 'time']

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

    c.save(validate=True, write_concern={"w": 1})
    return _AsDocument()(_dereference_uid_fields(c))
