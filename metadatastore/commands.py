from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
from functools import wraps
from itertools import count
import logging
from collections import MutableMapping
import uuid
from datetime import datetime
from itertools import chain
from collections import Mapping
import collections

from mongoengine.context_managers import no_dereference
import pytz
import six
import mongoengine
from mongoengine.base.datastructures import BaseDict, BaseList
from mongoengine.base.document import BaseDocument
from bson.objectid import ObjectId
from bson.dbref import DBRef
from prettytable import PrettyTable
import humanize
import numpy as np

from .odm_templates import (RunStart, BeamlineConfig, RunStop,
                            EventDescriptor, Event, DataKey, ALIAS, Correction)
from metadatastore import conf
from mongoengine import connect,  ReferenceField
from six.moves import reduce

logger = logging.getLogger(__name__)


__all__ = ['insert_beamline_config', 'insert_run_start', 'insert_event',
           'insert_run_stop', 'insert_event_descriptor', 'find_run_stops',
           'find_beamline_configs', 'find_event_descriptors', 'find_last',
           'find_events', 'find_run_starts', 'db_connect', 'db_disconnect',
           'format_data_keys', 'format_events', 'reorganize_event',
           'find_corrections']


def _ensure_connection(func):

    @wraps(func)
    def inner(*args, **kwargs):
        database = conf.connection_config['database']
        host = conf.connection_config['host']
        port = int(conf.connection_config['port'])
        db_connect(database=database, host=host, port=port)
        return func(*args, **kwargs)
    return inner


def db_disconnect():
    """Helper function to deal with stateful connections to mongoengine"""
    mongoengine.connection.disconnect(ALIAS)
    for collection in [RunStart, BeamlineConfig, RunStop, EventDescriptor,
                       Event, DataKey]:
        collection._collection = None


def db_connect(database, host, port):
    try:
        conn = mongoengine.connection.get_connection(ALIAS)
    except mongoengine.ConnectionError:
        conn = None

    """Helper function to deal with stateful connections to mongoengine"""
    return connect(db=database, host=host, port=port, alias=ALIAS)


def format_data_keys(data_key_dict):
    """Helper function that allows ophyd to send info about its data keys
    to metadatastore and have metadatastore format them into whatever the
    current spec dictates. This functions formats the data key info for
    the event descriptor

    Parameters
    ----------
    data_key_dict : dict
        The format that ophyd is sending to metadatastore
        {'data_key1': {
            'source': source_value,
            'dtype': dtype_value,
            'shape': shape_value},
         'data_key2': {...}
        }

    Returns
    -------
    formatted_dict : dict
        Data key info for the event descriptor that is formatted for the
        current metadatastore spec.The current metadatastore spec is:
        {'data_key1':
         'data_key2': mds.odm_templates.DataKeys
        }
    """
    data_key_dict = {key_name: (
                     DataKey(**data_key_description) if
                     not isinstance(data_key_description, DataKey) else
                     data_key_description)
                     for key_name, data_key_description
                     in six.iteritems(data_key_dict)}
    return data_key_dict


def format_events(event_dict):
    """Helper function for ophyd to format its data dictionary in whatever
    flavor of the week metadatastore's spec says. This insulates ophyd from
    changes to the mds spec

    Currently formats the dictionary as {key: [value, timestamp]}

    Parameters
    ----------
    event_dict : dict
        The format that ophyd is sending to metadatastore
        {'data_key1': {
            'timestamp': timestamp_value, # should be a float value!
            'value': data_value
         'data_key2': {...}
        }

    Returns
    -------
    formatted_dict : dict
        The event dict formatted according to the current metadatastore spec.
        The current metadatastore spec is:
        {'data_key1': [data_value, timestamp_value],
         'data_key2': [...],
        }
    """
    return {key: [data_dict['value'], data_dict['timestamp']]
            for key, data_dict in six.iteritems(event_dict)}


# database INSERTION ###################################################

@_ensure_connection
def insert_run_start(time, scan_id, beamline_id, beamline_config, uid=None,
                     owner=None, group=None, project=None, custom=None):
    """Provide a head for a sequence of events. Entry point for an
    experiment's run.

    Parameters
    ----------
    time : float
        The date/time as found at the client side when an event is
        created.
    scan_id : int
        Unique scan identifier visible to the user and data analysis
    beamline_id : str
        Beamline String identifier. Not unique, just an indicator of
        beamline code for multiple beamline systems
    beamline_config : metadatastore.documents.Document or str
        if Document:
            The metadatastore beamline config document
        if str:
            uid of beamline config corresponding to a given run
    uid : str, optional
        Globally unique id string provided to metadatastore
    owner : str, optional
        A username associated with the entry
    group : str, optional
        A group (e.g., UNIX group) associated with the entry
    project : str, optional
        Any project name to help users locate the data
    custom: dict, optional
        Any additional information that data acquisition code/user wants
        to append to the Header at the start of the run.

    Returns
    -------
    run_start: mongoengine.Document
        Inserted mongoengine object

    """
    if uid is None:
        uid = str(uuid.uuid4())
    if custom is None:
        custom = {}
    if owner is None:
        owner = ''
    if group is None:
        group = ''
    if project is None:
        project = ''

    beamline_config = _get_mongo_document(beamline_config, BeamlineConfig)
    run_start = RunStart(time=time, scan_id=scan_id,
                         time_as_datetime=_todatetime(time), uid=uid,
                         beamline_id=beamline_id,
                         beamline_config=beamline_config,
                         owner=owner, group=group, project=project,
                         **custom)

    run_start.save(validate=True, write_concern={"w": 1})
    logger.debug('Inserted RunStart with uid %s', run_start.uid)

    return uid


@_ensure_connection
def insert_run_stop(run_start, time, uid=None, exit_status='success',
                    reason=None, custom=None):
    """ Provide an end to a sequence of events. Exit point for an
    experiment's run.

    Parameters
    ----------
    run_start : metadatastore.documents.Document or str
        if Document:
            The metadatastore RunStart document
        if str:
            uid of RunStart object to associate with this record
    time : float
        The date/time as found at the client side when an event is
        created.
    uid : str, optional
        Globally unique id string provided to metadatastore
    exit_status : {'success', 'abort', 'fail'}, optional
        indicating reason run stopped, 'success' by default
    reason : str, optional
        more detailed exit status (stack trace, user remark, etc.)
    custom : dict, optional
        Any additional information that data acquisition code/user wants
        to append to the Header at the end of the run.

    Returns
    -------
    run_stop : mongoengine.Document
        Inserted mongoengine object
    """
    if uid is None:
        uid = str(uuid.uuid4())
    if custom is None:
        custom = {}
    run_start = _get_mongo_document(run_start, RunStart)
    run_stop = RunStop(run_start=run_start, reason=reason, time=time,
                       time_as_datetime=_todatetime(time), uid=uid,
                       exit_status=exit_status, **custom)

    run_stop.save(validate=True, write_concern={"w": 1})
    logger.debug("Inserted RunStop with uid %s referencing RunStart "
                 " with uid %s", run_stop.uid, run_start.uid)

    return uid


@_ensure_connection
def insert_beamline_config(config_params, time, uid=None):
    """ Create a beamline_config  in metadatastore database backend

    Parameters
    ----------
    config_params : dict
        Name/value pairs that indicate beamline configuration
        parameters during capturing of data
    time : float
        The date/time as found at the client side when the
        beamline configuration is created.
    uid : str, optional
        Globally unique id string provided to metadatastore

    Returns
    -------
    blc : BeamlineConfig
        The document added to the collection
    """
    if uid is None:
        uid = str(uuid.uuid4())
    blc = BeamlineConfig(config_params=config_params, time=time, uid=uid)
    blc.save(validate=True, write_concern={"w": 1})
    logger.debug("Inserted BeamlineConfig with uid %s",
                 blc.uid)

    return uid


@_ensure_connection
def insert_event_descriptor(run_start, data_keys, time, uid=None,
                            custom=None):
    """ Create an event_descriptor in metadatastore database backend

    Parameters
    ----------
    run_start : metadatastore.documents.Document or str
        if Document:
            The metadatastore RunStart document
        if str:
            uid of RunStart object to associate with this record
    data_keys : dict
        Provides information about keys of the data dictionary in
        an event will contain
    time : float
        The date/time as found at the client side when an event
        descriptor is created.
    uid : str, optional
        Globally unique id string provided to metadatastore
    custom : dict, optional
        Any additional information that data acquisition code/user wants
        to append to the EventDescriptor.

    Returns
    -------
    ev_desc : EventDescriptor
        The document added to the collection.

    """
    if uid is None:
        uid = str(uuid.uuid4())
    if custom is None:
        custom = {}
    data_keys = format_data_keys(data_keys)
    run_start = _get_mongo_document(run_start, RunStart)
    event_descriptor = EventDescriptor(run_start=run_start,
                                       data_keys=data_keys, time=time,
                                       uid=uid,
                                       time_as_datetime=_todatetime(time),
                                       **custom)

    event_descriptor.save(validate=True, write_concern={"w": 1})
    logger.debug("Inserted EventDescriptor with uid %s referencing "
                 "RunStart with uid %s", event_descriptor.uid, run_start.uid)

    return uid


@_ensure_connection
def insert_event(descriptor, time, data, timestamps, seq_num, uid=None):
    """Create an event in metadatastore database backend

    Parameters
    ----------
    descriptor : metadatastore.documents.Document or str
        if Document:
            The metadatastore EventDescriptor document
        if str:
            uid of EventDescriptor object to associate with this record
    time : float
        The date/time as found at the client side when an event is
        created.
    data : dict
        Dictionary of measured values (or external references)
    timestamps : dict
        Dictionary of measured timestamps for each values, having the
        same keys as `data` above
    seq_num : int
        Unique sequence number for the event. Provides order of an event in
        the group of events
    uid : str, optional
        Globally unique id string provided to metadatastore
    """
    if set(data.keys()) != set(timestamps.keys()):
        raise ValueError("The fields in 'data' and 'timestamps' must match.")
    val_ts_tuple = _transform_data(data, timestamps)

    # Allow caller to beg forgiveness rather than ask permission w.r.t
    # EventDescriptor creation.
    if descriptor is None:
        raise EventDescriptorIsNoneError()

    if uid is None:
        uid = str(uuid.uuid4())

    descriptor = _get_mongo_document(descriptor, EventDescriptor)
    event = Event(descriptor_id=descriptor, uid=uid,
                  data=val_ts_tuple, time=time, seq_num=seq_num)

    event.save(validate=True, write_concern={"w": 1})
    logger.debug("Inserted Event with uid %s referencing "
                 "EventDescriptor with uid %s", event.uid,
                 descriptor.uid)
    return uid


def _transform_data(data, timestamps):
    """
    Transform from Document spec:
        {'data': {'key': <value>},
         'timestamps': {'key': <timestamp>}}
    to storage format:
        {'data': {<key>: (<value>, <timestamp>)}.
    """
    return {k: (data[k], timestamps[k]) for k in data}


class EventDescriptorIsNoneError(ValueError):
    """Special error that ophyd looks for when it passes a `None` event
    descriptor. Ophyd will then create an event descriptor and create the event
    again
    """
    pass


# DATABASE RETRIEVAL ##########################################################

# TODO: Update all query routine documentation
class _AsDocument(object):
    """
    A caching layer to avoid creating reference objects for _every_
    """
    def __init__(self):
        self._cache = dict()

    def __call__(self, mongoengine_object):
        return Document.from_mongo(mongoengine_object, self._cache)


class _AsDocumentRaw(object):
    """
    A caching layer to avoid creating reference objects for _every_
    """
    def __init__(self):
        self._cache = dict()

    def __call__(self, name, input_dict, dref_fields, newest):

        return Document.from_dict(name, input_dict, newest, dref_fields,
                                  self._cache)


def _format_time(search_dict):
    """Helper function to format the time arguments in a search dict

    Expects 'start_time' and 'stop_time'

    ..warning: Does in-place mutation of the search_dict
    """
    time_dict = {}
    start_time = search_dict.pop('start_time', None)
    stop_time = search_dict.pop('stop_time', None)
    if start_time:
        time_dict['$gte'] = _normalize_human_friendly_time(start_time)
    if stop_time:
        time_dict['$lte'] = _normalize_human_friendly_time(stop_time)
    if time_dict:
        search_dict['time'] = time_dict


# human friendly timestamp formats we'll parse
_TS_FORMATS = [
    '%Y-%m-%d %H:%M:%S',
    '%Y-%m-%d %H:%M',  # these 2 are not as originally doc'd,
    '%Y-%m-%d %H',     # but match previous pandas behavior
    '%Y-%m-%d',
    '%Y-%m',
    '%Y']

# build a tab indented, '-' bulleted list of supported formats
# to append to the parsing function docstring below
_doc_ts_formats = '\n'.join('\t- {}'.format(_) for _ in _TS_FORMATS)


def _normalize_human_friendly_time(val):
    """Given one of :
    - string (in one of the formats below)
    - datetime (eg. datetime.now()), with or without tzinfo)
    - timestamp (eg. time.time())
    return a timestamp (seconds since jan 1 1970 UTC).

    Non string/datetime values are returned unaltered.
    Leading/trailing whitespace is stripped.
    Supported formats:
    {}
    """
    # {} is placeholder for formats; filled in after def...

    tz = conf.connection_config['timezone']  # e.g., 'US/Eastern'
    zone = pytz.timezone(tz)  # tz as datetime.tzinfo object
    epoch = pytz.UTC.localize(datetime(1970, 1, 1))
    check = True

    if isinstance(val, six.string_types):
        # unix 'date' cmd format '%a %b %d %H:%M:%S %Z %Y' works but
        # doesn't get TZ?

        # Could cleanup input a bit? remove leading/trailing [ :,-]?
        # Yes, leading/trailing whitespace to match pandas behavior...
        # Actually, pandas doesn't ignore trailing space, it assumes
        # the *current* month/day if they're missing and there's
        # trailing space, or the month is a single, non zero-padded digit.?!
        val = val.strip()

        for fmt in _TS_FORMATS:
            try:
                ts = datetime.strptime(val, fmt)
                break
            except ValueError:
                pass

        try:
            if isinstance(ts, datetime):
                val = ts
                check = False
            else:
                raise TypeError('expected datetime,'
                                ' got {:r}'.format(ts))

        except NameError:
            raise ValueError('failed to parse time: ' + repr(val))

    if check and not isinstance(val, datetime):
        return val

    if val.tzinfo is None:
        # is_dst=None raises NonExistent and Ambiguous TimeErrors
        # when appropriate, same as pandas
        val = zone.localize(val, is_dst=None)

    return (val - epoch).total_seconds()


# fill in the placeholder we left in the previous docstring
_normalize_human_friendly_time.__doc__ = (
    _normalize_human_friendly_time.__doc__.format(_doc_ts_formats)
    )


def _normalize_object_id(kwargs, key):
    """Ensure that an id is an ObjectId, not a string.

    ..warning: Does in-place mutation of the search_dict
    """
    try:
        kwargs[key] = ObjectId(kwargs[key])
    except KeyError:
        # This key wasn't used by the query; that's fine.
        pass
    except TypeError:
        # This key was given a more complex query.
        pass
    # Database errors will still raise.


def _get_mongo_document(document, document_cls):
    """Helper function to get the mongo id of the mongo document of type
    ``document_cls``

    Parameters
    ----------
    document : mds.odm_templates.Document or str
        if str: The externally supplied unique identifier of the mongo document
    document_cls : object
        One of the class objects from the metadatastore.odm_templates module
        {RunStart, RunStop, Event, EventDescriptor, etc...}
    """
    if isinstance(document, Document):
        document = document.uid
    # .get() is slower than .first() which is slower than [0].
    # __raw__=dict() is faster than kwargs
    # see http://nbviewer.ipython.org/gist/ericdill/ca047302c2c1f1865415
    mongo_document = document_cls.objects(__raw__={'uid': document})[0]
    return mongo_document


def _dereference_uid_fields(correction_document):
    uid_field_name_map = {'run_start': Correction,
                          'beamline_config': BeamlineConfig}
    _as_document = _AsDocument()
    for k, v in six.iteritems(correction_document._data):
        if hasattr(v, 'uid'):
            v = v.uid
        if isinstance(v, DBRef):
            continue
        if k in uid_field_name_map:
            if k == 'run_start':
                # it could be a Correction or a RunStart. See if it is a
                # Correction first
                correction = Correction.objects(
                    __raw__={'uid': v}).order_by('-id')
                if len(correction):
                    correction = correction[0]
                else:
                    correction = RunStart.objects(
                        __raw__={'uid': v}).order_by('-id')[0]
            elif k == 'beamline_config':
                correction = BeamlineConfig.objects(
                    __raw__={'uid': v}).order_by('-id')[0]
            # if not len(correction):
            #     # see if the uid is pointing to a single correction document
            #     correction = mds_cls.objects(
            #         __raw__={'correction_uid': v}).order_by('-id')
            #     if not len(correction):
            #         raise ValueError(
            #             "No documents were found in the %s collection for uid "
            #             "%s" % (str(mds_cls), v))
            #     correction = correction[0]
            # else:
            #     correction = correction[0]
            # correction = _as_document(correction)
            correction = _dereference_uid_fields(correction)
            setattr(correction_document, k, correction)
    return correction_document


@_ensure_connection
def find_corrections(newest=False, dereference_uids=True, **kwargs):
    """
    Parameters
    ----------
    newest : bool, optional
        True: only return the newest one
    uid : str, optional
        The uid of the original metadatastore Document, shared between all
        instances of its Corrections
    correction_uid : str, optional
        The unique identifier for one Correction document in the Correction
        collection
    start_time : time-like, optional
        time-like representation of the earliest time that a RunStart
        was created. Valid options are:
           - timestamps --> time.time()
           - '2015'
           - '2015-01'
           - '2015-01-30'
           - '2015-03-30 03:00:00'
           - datetime.now()
    stop_time : time-like, optional
        timestamp of the latest time that a RunStart was created. See
        docs for `start_time` for examples.
    uid : str, optional
        Globally unique id string provided to metadatastore
    """
    _as_document = _AsDocument()
    return (_as_document(c) for c in _find_corrections_helper(
        newest=newest, dereference_uids=dereference_uids, **kwargs))


def _find_corrections_helper(newest=True, dereference_uids=True, **kwargs):
    """Helper function that does not nuke mongo fields.

    See ``find_corrections`` for relevant kwargs

    Parameters
    ----------
    newest : bool, optional
        True: only return the newest one. Defaults to True
    for all other kwargs, see ``find_corrections`` docstring

    Returns
    -------
    corrections : list
        List of mongo objects with uid fields dereferenced
    """
    _normalize_object_id(kwargs, '_id')
    _format_time(kwargs)
    corrections = Correction.objects(__raw__=kwargs).order_by('-id')
    if not corrections:
        return []
    if newest:
        corrections = [corrections[0]]
    if dereference_uids:
        corrections = [_dereference_uid_fields(correction) for correction in
                       corrections]
    print('corrections: {}'.format(corrections))
    return corrections


def _dereference_reference_fields(mongo_document):

    fields = set(chain(mongo_document._fields.keys(),
                       mongo_document._data.keys()))

    for field in fields:
        attr = getattr(mongo_document, field)
        attr_type = type(attr)
        if isinstance(attr, mongoengine.document.Document):
            attr = getattr(mongo_document, field)
            # see if there is a correction
            corrected = _find_corrections_helper(uid=attr.uid)
            if corrected:
                corrected = _dereference_reference_fields(corrected[0])
                setattr(mongo_document, field, corrected)

    return mongo_document


def _find_documents(DocumentClass, **kwargs):
    """Helper function to extract copy/paste code from find_* functions

    Parameters
    ----------
    DocumentClass : metadatastore.document.* class
        The metadatastore class
    kwargs : dict
        See relevant find_* function for relevant kwargs

    Returns
    -------
    as_document : list
        List of mongoengine objects
    """
    _normalize_object_id(kwargs, '_id')
    _format_time(kwargs)
    with no_dereference(DocumentClass) as DocumentClass:
        # ordering by '-_id' sorts by newest first
        newest = kwargs.pop('newest', None)
        search_results = DocumentClass.objects(__raw__=kwargs).order_by('-id')
        print('search_results: {}'.format(search_results))
        return search_results


def _correct_results(search_results):
    print('search_results: {}'.format(search_results))
    print("searching for newer documents")
    corrected_results = []
    for res in search_results:
        corrected = _find_corrections_helper(newest=True,
                                             uid=res.uid)
        if corrected:
            res = corrected[0]
        corrected_results.append(res)
    # now recursively dereference ReferenceFields
    dereferenced_results = []
    for res in corrected_results:
        res = _dereference_reference_fields(res)
        dereferenced_results.append(res)
    search_results = dereferenced_results
    return search_results


def _get_uid(document):
    try:
        return document.uid
    except AttributeError:
        # the document is either a string or None. It does not matter which
        # at this point
        return document


@_ensure_connection
def find_run_starts(newest=True, **kwargs):
    """Given search criteria, locate RunStart Documents.

    Parameters
    ----------
    newest : bool
        True: Find and use the most recent correction for this document
        False: Do not search for corrections and use the raw data
    start_time : time-like, optional
        time-like representation of the earliest time that a RunStart
        was created. Valid options are:
           - timestamps --> time.time()
           - '2015'
           - '2015-01'
           - '2015-01-30'
           - '2015-03-30 03:00:00'
           - datetime.now()
    stop_time : time-like, optional
        timestamp of the latest time that a RunStart was created. See
        docs for `start_time` for examples.
    beamline_id : str, optional
        String identifier for a specific beamline
    project : str, optional
        Project name
    owner : str, optional
        The username of the logged-in user when the scan was performed
    scan_id : int, optional
        Integer scan identifier
    uid : str, optional
        Globally unique id string provided to metadatastore
    _id : str or ObjectId, optional
        The unique id generated by mongo

    Returns
    -------
    rs_objects : iterable of metadatastore.document.Document objects

    Note
    ----
    All documents that the RunStart Document points to are dereferenced.
    These include RunStop, BeamlineConfig, and Sample.

    Examples
    --------
    >>> find_run_starts(scan_id=123)
    >>> find_run_starts(owner='arkilic')
    >>> find_run_starts(start_time=1421176750.514707, stop_time=time.time()})
    >>> find_run_starts(start_time=1421176750.514707, stop_time=time.time())

    >>> find_run_starts(owner='arkilic', start_time=1421176750.514707,
    ...                stop_time=time.time())

    """
    _as_document = _AsDocument()
    mongo_run_starts = _find_documents(RunStart, **kwargs)
    if newest:
        mongo_run_starts = _correct_results(mongo_run_starts)
    # lazily turn the mongo objects into safe objects via generator
    return (_as_document(doc) for doc in mongo_run_starts)


@_ensure_connection
def find_beamline_configs(**kwargs):
    """Given search criteria, locate BeamlineConfig Documents.

    Parameters
    ----------
    start_time : time-like, optional
        time-like representation of the earliest time that a BeamlineConfig
        was created. Valid options are:
           - timestamps --> time.time()
           - '2015'
           - '2015-01'
           - '2015-01-30'
           - '2015-03-30 03:00:00'
           - datetime.now()
    stop_time : time-like, optional
        timestamp of the latest time that a BeamlineConfig was created. See
            docs for `start_time` for examples.
    uid : str, optional
        Globally unique id string provided to metadatastore
    _id : str or ObjectId, optional
        The unique id generated by mongo

    Returns
    -------
    beamline_configs : iterable of metadatastore.document.Document objects
    """
    _as_document = _AsDocument()
    # lazily turn the mongo objects into safe objects via generator
    return (_as_document(doc) for doc in _find_documents(BeamlineConfig,
                                                         **kwargs))


@_ensure_connection
def find_run_stops(run_start=None, newest=True, **kwargs):
    """Given search criteria, locate RunStop Documents.

    Parameters
    ----------
    run_start : metadatastore.document.Document or str, optional
        The metadatastore run start document or the metadatastore uid to get
        the corresponding run end for
    newest : bool
        True: Find and use the most recent correction for this document
        False: Do not search for corrections and use the raw data
    start_time : time-like, optional
        time-like representation of the earliest time that a RunStop
        was created. Valid options are:
           - timestamps --> time.time()
           - '2015'
           - '2015-01'
           - '2015-01-30'
           - '2015-03-30 03:00:00'
           - datetime.now()
    stop_time : time-like, optional
        timestamp of the latest time that a RunStop was created. See
        docs for `start_time` for examples.
    exit_status : {'success', 'fail', 'abort'}, optional
        provides information regarding the run success.
    reason : str, optional
        Long-form description of why the run was terminated.
    uid : str, optional
        Globally unique id string provided to metadatastore
    _id : str or ObjectId, optional
        The unique id generated by mongo

    Returns
    -------
    run_stop : iterable of metadatastore.document.Document objects
    """
    run_start_uid = _get_uid(run_start)
    if run_start:
        mongo_run_start = _find_documents(RunStart, uid=run_start_uid)[0]
        kwargs['run_start_id'] = mongo_run_start.id

    _normalize_object_id(kwargs, 'run_start_id')
    mongo_run_stops = _find_documents(RunStop, **kwargs)
    if newest:
        mongo_run_stops = _correct_results(mongo_run_stops)
    _as_document = _AsDocument()
    # lazily turn the mongo objects into safe objects via generator
    return (_as_document(doc) for doc in mongo_run_stops)


@_ensure_connection
def find_event_descriptors(run_start=None, newest=True,
                           **kwargs):
    """Given search criteria, locate EventDescriptor Documents.

    Parameters
    ----------
    run_start : metadatastore.document.Document or uid, optional
        if ``Document``:
            The metadatastore run start document or the metadatastore uid to get
            the corresponding run end for
        if ``str``:
            Globally unique id string provided to metadatastore for the
            RunStart Document.
    newest : bool

    start_time : time-like, optional
        time-like representation of the earliest time that an EventDescriptor
        was created. Valid options are:
           - timestamps --> time.time()
           - '2015'
           - '2015-01'
           - '2015-01-30'
           - '2015-03-30 03:00:00'
           - datetime.now()
    stop_time : time-like, optional
        timestamp of the latest time that an EventDescriptor was created. See
        docs for `start_time` for examples.
    uid : str, optional
        Globally unique id string provided to metadatastore
    _id : str or ObjectId, optional
        The unique id generated by mongo

    Returns
    -------
    event_descriptor : iterable of metadatastore.document.Document objects
    """
    _format_time(kwargs)
    # get the actual mongo document
    run_start_uid = _get_uid(run_start)
    if run_start_uid:
        mongo_run_start = _find_documents(RunStart, uid=run_start_uid)[0]
        kwargs['run_start_id'] = mongo_run_start.id

    _normalize_object_id(kwargs, '_id')
    _normalize_object_id(kwargs, 'run_start_id')
    _as_document = _AsDocument()
    mongo_descriptors = _find_documents(EventDescriptor, **kwargs)
    if newest:
        mongo_descriptors = _correct_results(mongo_descriptors)
    return (_as_document(doc) for doc in mongo_descriptors)


@_ensure_connection
def find_events(descriptor=None, newest=True, **kwargs):
    """Given search criteria, locate Event Documents.

    Parameters
    -----------
    start_time : time-like, optional
        time-like representation of the earliest time that an Event
        was created. Valid options are:
           - timestamps --> time.time()
           - '2015'
           - '2015-01'
           - '2015-01-30'
           - '2015-03-30 03:00:00'
           - datetime.now()
    stop_time : time-like, optional
        timestamp of the latest time that an Event was created. See
        docs for `start_time` for examples.
    descriptor : metadatastore.document.Document or uid, optional
        if Document:
            The metadatastore run start document or the metadatastore uid to get
            the corresponding run end for
        if uid:
            Globally unique id string provided to metadatastore for the
            EventDescriptor Document.
    uid : str, optional
        Globally unique id string provided to metadatastore
    _id : str or ObjectId, optional
        The unique id generated by mongo

    Returns
    -------
    events : iterable of metadatastore.document.Document objects
    """
    # Some user-friendly error messages for an easy mistake to make
    if 'event_descriptor' in kwargs:
        raise ValueError("Use 'descriptor', not 'event_descriptor'.")
    if 'event_descriptor_id' in kwargs:
        raise ValueError("Use 'descriptor_id', not 'event_descriptor_id'.")

    _format_time(kwargs)
    # get the actual mongo document
    descriptor_uid = _get_uid(descriptor)
    if descriptor_uid:
        # then we are clearly searching for one "set" of events
        mongo_descriptor = _find_documents(EventDescriptor,
                                           uid=descriptor_uid)[0]
        kwargs['descriptor_id'] = mongo_descriptor.id

    _normalize_object_id(kwargs, '_id')
    _normalize_object_id(kwargs, 'descriptor_id')
    events = Event.objects(__raw__=kwargs).order_by('-time')
    events = events.as_pymongo()
    dref_dict = dict()
    name = Event.__name__
    for n, f in six.iteritems(Event._fields):
        if isinstance(f, ReferenceField):
            lookup_name = f.db_field
            dref_dict[lookup_name] = f

    _as_document = _AsDocumentRaw()
    return (reorganize_event(_as_document(name, ev, dref_dict, newest))
            for ev in events)


@_ensure_connection
def find_last(num=1, newest=True):
    """Locate the last `num` RunStart Documents

    Parameters
    ----------
    num : integer, optional
        number of RunStart documents to return, default 1

    Returns
    -------
    run_start: iterable of metadatastore.document.Document objects
    """
    c = count()
    _as_document = _AsDocument()
    mongo_run_starts = RunStart.objects.order_by('-_id')[:num]
    for rs in mongo_run_starts:
        if next(c) == num:
            raise StopIteration
        rs = _correct_results([rs])[0]
        yield _as_document(rs)


def _todatetime(time_stamp):
    if isinstance(time_stamp, float):
        return datetime.fromtimestamp(time_stamp)
    else:
        raise TypeError('Timestamp format is not correct!')


def reorganize_event(event_document):
    """Reorganize Event attributes, unnormalizing 'data'.

    Convert from Event.data = {'data_key': (value, timestamp)}
    to Event.data = {'data_key': value}
    and Event.timestamps = {'data_key': timestamp}

    Parameters
    ----------
    event_document : metadatastore.document.Document

    Returns
    -------
    event_document
    """
    doc = event_document  # for brevity
    pairs = [((k, v[0]), (k, v[1])) for k, v in six.iteritems(doc.data)]
    doc.data, doc.timestamps = [dict(tuples) for tuples in zip(*pairs)]
    return doc


# FIELDS THAT LIVED IN DOCUMENT

class Document(MutableMapping):
    """A dictionary where d.key is the same as d['key']
    and attributes/keys beginning with '_' are skipped
    in iteration."""

    def __init__(self):
        self._fields = set()

    def __setattr__(self, k, v):
        self.__dict__[k] = v
        if not k.startswith('_'):
            self._fields.add(k)
        assert hasattr(self, k)
        assert k in self.__dict__

    def __delattr__(self, k):
        del self.__dict__[k]
        if not k.startswith('_'):
            self._fields.remove(k)
        assert k not in self._fields

    def __iter__(self):
        return iter(self._fields)

    def __getitem__(self, key):
        try:
            return getattr(self, key)
        except AttributeError:
            raise KeyError(key)

    def __delitem__(self, key):
        delattr(self, key)

    def __setitem__(self, key, val):
        setattr(self, key, val)

    def __len__(self):
        return len(self._fields)

    def __contains__(self, key):
        return key in self._fields

    @classmethod
    def from_mongo(cls, mongo_document, cache=None, document_name=None,
                   newest=True):
        """
        Copy the data out of a mongoengine.Document, including nested
        Documents, but do not copy any of the mongo-specific methods or
        attributes.

        Parameters
        ----------
        mongo_document : mongoengine.Document
        cache : dict-like, optional
            Cache of already seen objects in the DB so that we do not
            have to de-reference and build them again.
        document_name : str, optional
            The name for the document (i.e., name='Run Start')
            if none, document_name = mongo_document.__class__.__name__
        newest : bool
            Use the newest correction documents

        Returns
        -------
        doc : Document
            The result as a mds Document
        """
        if cache is None:
            cache = dict()
        document = Document()
        if document_name is None:
            document_name = mongo_document.__class__.__name__

        document._name = document_name

        fields = set(chain(mongo_document._fields.keys(),
                           mongo_document._data.keys()))

        for field in fields:
            if field == 'id':
                # we are no longer supporting mongo id's making it out of
                # metadatastore
                continue
            attr = getattr(mongo_document, field)
            if isinstance(attr, DBRef):
                oid = attr.id
                try:
                    attr = cache[oid]
                except KeyError:
                    # do de-reference
                    mongo_document.select_related()
                    # grab the attribute again
                    attr = getattr(mongo_document, field)
                    # normalize it
                    attr = _normalize(attr, cache)
                    # and stash for later use
                    cache[oid] = attr
            else:
                attr = _normalize(attr, cache)

            document[field] = attr
        # For debugging, add a human-friendly time_as_datetime attribute.
        if 'time' in document:
            document.time_as_datetime = datetime.fromtimestamp(document.time)
        return document

    @classmethod
    def from_dict(cls, name, input_dict, newest, dref_fields=None, cache=None):
        """Document from dictionary

        Turn a dictionary into a MDS Document, de-referencing
        ObjectId fields as required

        Parameters
        ----------
        name : str
            The class name to assign to the result object
        input_dict : dict
            Raw pymongo document
        dref_fields : dict, optional
            Dictionary keyed on field name mapping to the ReferenceField object
            to use to de-reference the field.
        cache : dict
            Cache dictionary
        newest : bool
            Use the newest correction documents

        Returns
        -------
        doc : Document
            The result as a mds Document
        """
        if cache is None:
            cache = {}
        if dref_fields is None:
            dref_fields = {}

        document = Document()
        document._name = name
        for k, v in six.iteritems(input_dict):
            if k == '_id':
                document['id'] = str(v)
                continue
            if isinstance(v, ObjectId):
                ref_klass = dref_fields[k]
                new_key = ref_klass.name
                try:
                    document[new_key] = cache[v]
                except KeyError:

                    ref_obj = ref_klass.document_type_obj
                    # this search is basically free
                    ref_doc = cls.from_mongo(ref_obj.objects.get(id=v),
                                             newest=True)
                    if newest:
                        new_doc = _find_corrections_helper(
                            newest, dereference_uids=True, uid=ref_doc.uid)
                        if new_doc:
                            ref_doc = _AsDocument()(new_doc[0])

                    cache[v] = ref_doc
                    document[new_key] = ref_doc
            else:
                document[k] = v
        # For debugging, add a human-friendly time_as_datetime attribute.
        if 'time' in document:
            document.time_as_datetime = datetime.fromtimestamp(
                document.time)
        return document

    def __repr__(self):
        try:
            infostr = '. %s' % self.uid
        except AttributeError:
            infostr = ''
        return "<%s Document%s>" % (self._name, infostr)

    def __str__(self):
        return _str_helper(self)

    def _repr_html_(self):
        return html_table_repr(self)


def _normalize(in_val, cache):
    """
    Helper function for cleaning up the mongoegine documents to be safe.

    Converts Mongoengine.Document to mds.Document objects recursively

    Converts:

     -  mongoengine.base.datastructures.BaseDict -> dict
     -  mongoengine.base.datastructures.BaseList -> list
     -  ObjectID -> str

    Parameters
    ----------
    in_val : object
        Object to be sanitized

    cache : dict-like
        Cache of already seen objects in the DB so that we do not
        have to de-reference and build them again.

    Returns
    -------
    ret : object
        The 'sanitized' object

    """
    if isinstance(in_val, BaseDocument):
        return Document.from_mongo(in_val, cache)
    elif isinstance(in_val, BaseDict):
        return {_normalize(k, cache): _normalize(v, cache)
                for k, v in six.iteritems(in_val)}
    elif isinstance(in_val, BaseList):
        return [_normalize(v, cache) for v in in_val]
    elif isinstance(in_val, ObjectId):
        return str(in_val)
    return in_val


def _format_dict(value, name_width, value_width, name, tabs=0):
    ret = ''
    for k, v in six.iteritems(value):
        if isinstance(v, Mapping):
            ret += _format_dict(v, name_width, value_width, k, tabs=tabs+1)
        else:
            ret += ("\n%s%-{}s: %-{}s".format(
                name_width, value_width) % ('  '*tabs, k[:16], v))
    return ret


def _format_data_keys_dict(data_keys_dict):
    fields = reduce(set.union,
                    (set(v) for v in six.itervalues(data_keys_dict)))
    fields = sorted(list(fields))
    table = PrettyTable(["data keys"] + list(fields))
    table.align["data keys"] = 'l'
    table.padding_width = 1
    for data_key, key_dict in sorted(data_keys_dict.items()):
        row = [data_key]
        for fld in fields:
            row.append(key_dict.get(fld, ''))
        table.add_row(row)
    return table


def html_table_repr(obj):
    """Organize nested dict-like and list-like objects into HTML tables."""
    if hasattr(obj, 'items'):
        output = "<table>"
        for key, value in sorted(obj.items()):
            output += "<tr>"
            output += "<td>{key}</td>".format(key=key)
            output += ("<td>" + html_table_repr(value) + "</td>")
            output += "</tr>"
        output += "</table>"
    elif (isinstance(obj, collections.Iterable) and
          not isinstance(obj, six.string_types) and
          not isinstance(obj, np.ndarray)):
        output = "<table style='border: none;'>"
        # Sort list if possible.
        try:
            obj = sorted(obj)
        except TypeError:
            pass
        for value in obj:
            output += "<tr style='border: none;' >"
            output += "<td style='border: none;'>" + html_table_repr(value)
            output += "</td></tr>"
        output += "</table>"
    elif isinstance(obj, datetime):
        # '1969-12-31 19:00:00' -> '1969-12-31 19:00:00 (45 years ago)'
        human_time = humanize.naturaltime(datetime.now() - obj)
        return str(obj) + '  ({0})'.format(human_time)
    else:
        return str(obj)
    return output


def _str_helper(document, name=None, indent=0):
    """Recursive document walker and formatter

    Parameters
    ----------
    name : str, optional
        Document header name. Defaults to ``self._name``
    indent : int, optional
        The indentation level. Defaults to starting at 0 and adding one tab
        per recursion level
    """
    if name is None:
        name = document._name
        if name == "Correction":
            name = document.original_document_type + " -- Correction"

    headings = [
        # characters recommended as headers by ReST docs
        '=', '-', '`', ':', '.', "'", '"', '~', '^', '_', '*', '+', '#',
        # all other valid header characters according to ReST docs
        '!', '$', '%', '&', '(', ')', ',', '/', ';', '<', '>', '?', '@',
        '[', '\\', ']', '{', '|', '}'
    ]

    mapping = collections.OrderedDict(
        {idx: char for idx, char in enumerate(headings)})
    ret = "\n%s\n%s" % (name, mapping[indent]*len(name))

    documents = []
    name_width = 16
    value_width = 40
    for name, value in sorted(document.items()):
        if isinstance(value, Document):
            documents.append((name, value))
        elif name == 'event_descriptors':
            for val in value:
                documents.append((name, val))
        elif name == 'data_keys':
            ret += "\n%s" % _format_data_keys_dict(value).__str__()
        elif isinstance(value, Mapping):
            # format dicts reasonably
            ret += "\n%-{}s:".format(name_width, value_width) % (name)
            ret += _format_dict(value, name_width, value_width, name, tabs=1)
        else:
            ret += ("\n%-{}s: %-{}s".format(name_width, value_width) %
                    (name[:16], value))
    for name, value in documents:
        ret += "\n%s" % (_str_helper(value, indent=indent+1))
        # ret += "\n"
    ret = ret.split('\n')
    ret = ["%s%s" % ('  '*indent, line) for line in ret]
    ret = "\n".join(ret)
    return ret
