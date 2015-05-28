from __future__ import (unicode_literals, print_function, division,
                        absolute_import)
import six
from functools import wraps
from . import conf
from .commands import (_ensure_connection, find_run_starts,
                       _get_mongo_document, db_disconnect, db_connect)

from mongoengine.context_managers import switch_collection
from mongoengine.connection import (register_connection, disconnect,
                                    get_connection, connect)
from .document import Document
from mongoengine.document import DynamicDocument
from mongoengine.fields import StringField
from uuid import uuid4
import time as ttime

class Foo(object):
    pass

# subclass the odm tempaltes
from .odm_templates import (RunStart, EventDescriptor, BeamlineConfig,
                            RunStop, Correction, Event)


def update(mds_document, correction_uid=None):
    """Update bad entries in the run start document

    Note that this does not actually touch the raw data, it creates a
    RunStart document in a separate document with only the diff from the
    original RunStart document

    Parameters
    ----------
    mds_document : metadatastore.document.Document
        A metadatastore document to update
    uid : str, optional
        Globally unique id string provided to metadatastore. Defaults to uuid4
    """
    classes = [RunStart, RunStop, EventDescriptor, BeamlineConfig,
               Correction, Event]

    parent_document_type = None

    for cls in classes:
        try:
            parent_document_type = _get_mongo_document(mds_document, cls)
        except IndexError:
            pass

    if isinstance(parent_document_type, Event):
        raise ValueError("You are not allowed to modify Events because that "
                         "allows the possibility to fake data")

    if correction_uid is None:
        correction_uid = str(uuid4())

    correction_document = Correction(uid=mds_document.uid,
                                     correction_uid=correction_uid,
                                     time=ttime.time())
    fields_to_skip = ['uid', 'correction_uid', 'time']
    for k, v in six.iteritems(mds_document):
        if k in fields_to_skip:
            continue
        setattr(correction_document, k, v)

    correction_document.save(validate=True, write_concern={"w": 1})
