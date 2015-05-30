from __future__ import (unicode_literals, print_function, division,
                        absolute_import)
from uuid import uuid4
import time as ttime
import logging

import six
import metadatastore
from metadatastore.commands import _AsDocument
from .odm_templates import *

from .commands import (_dereference_uid_fields)

logger = logging.getLogger(__name__)


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

    Note2 that the mds_document is going to be updated IN-PLACE to the new
    document

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
    _as_document = _AsDocument()
    if correction_uid is None:
        correction_uid = str(uuid4())

    if mds_document._name == 'Event':
        raise ValueError("You are not allowed to modify Events")

    # get the original document
    if 'Correction' in mds_document._name:
        mds_cls = Correction
        # the unique identifier for the Correction collection is
        # `correction_uid`, not `uid`. `uid` refers to the original document
        # in the other collections.
        kw = {'correction_uid': mds_document.correction_uid}
    else:
        mds_cls = globals()[mds_document._name]
        kw = {'uid': mds_document.uid}
    original_document = _as_document(mds_cls.objects.get(**kw))
    if mds_document == original_document:
        # the documents are identical, no need to create a new one!
        logger.debug('mds_document {} and original_document {} are identical. '
                     'No update required'.format(mds_document,
                                                 original_document))
        return

    if mds_document._name in ['BeamlineConfig']:
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

    # special case the event descriptor because it is the only thing that
    #  contains an Embedded Document
    if mds_document._name == "EventDescriptor":
        mds_document.data_keys = _replace_embedded_document(mds_document)

    for k, v in six.iteritems(mds_document):
        if k in fields_to_skip:
            continue
        if isinstance(v, metadatastore.document.Document):
            # reference document objects with their uid only
            # this will be dereferenced via the `_dereference_uid_fields`
            # function in `metadatastore.commands.py`
            update(v)
            setattr(c, k, v.uid)
        else:
            setattr(c, k, v)

    original_document_type = mds_document._name

    if mds_document._name == "Correction":
        original_document_type = mds_document.original_document_type
        setattr(c, 'original_document_type', original_document_type)
    c.save(validate=True, write_concern={"w": 1})

    documentized = _AsDocument()(_dereference_uid_fields(c))
    # update the correction in-place
    for k, v in six.iteritems(documentized):
        mds_document[k] = v
    mds_document._name = documentized._name
    # return _AsDocument()(_dereference_uid_fields(c))
