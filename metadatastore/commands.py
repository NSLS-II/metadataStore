from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import six
from functools import wraps
from itertools import count
from .odm_templates import (RunStart, BeamlineConfig, RunStop,
                            EventDescriptor, Event, DataKey, ALIAS, Correction)
from .document import Document
import datetime
import logging
from metadatastore import conf
from mongoengine import connect,  ReferenceField
import mongoengine.connection
from mongoengine.context_managers import no_dereference
import datetime
import pytz
from collections import Mapping

import uuid
from bson import ObjectId

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

    def __call__(self, name, input_dict, dref_fields):

        return Document.from_dict(name, input_dict, dref_fields, self._cache)


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
    - datetime (eg. datetime.datetime.now()), with or without tzinfo)
    - timestamp (eg. time.time())
    return a timestamp (seconds since jan 1 1970 UTC).

    Non string/datetime.datetime values are returned unaltered.
    Leading/trailing whitespace is stripped.
    Supported formats:
    {}
    """
    # {} is placeholder for formats; filled in after def...

    tz = conf.connection_config['timezone']  # e.g., 'US/Eastern'
    zone = pytz.timezone(tz)  # tz as datetime.tzinfo object
    epoch = pytz.UTC.localize(datetime.datetime(1970, 1, 1))
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
                ts = datetime.datetime.strptime(val, fmt)
                break
            except ValueError:
                pass

        try:
            if isinstance(ts, datetime.datetime):
                val = ts
                check = False
            else:
                raise TypeError('expected datetime.datetime,'
                                ' got {:r}'.format(ts))

        except NameError:
            raise ValueError('failed to parse time: ' + repr(val))

    if check and not isinstance(val, datetime.datetime):
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
    uid_field_name_map = {'descriptor': Correction,
                          'run_start': Correction,
                          'beamline_config': BeamlineConfig}
    _as_document = _AsDocument()
    for k, v in six.iteritems(correction_document._data):
        if hasattr(v, 'uid'):
            v = v.uid
        if k in uid_field_name_map:
            mds_cls = uid_field_name_map[k]
            # assume it is the uid for a document in another collection
            correction = mds_cls.objects(__raw__={'uid': v}).order_by('-id')
            if not len(correction):
                # see if the uid is pointing to a single correction document
                correction = mds_cls.objects(
                    __raw__={'correction_uid': v}).order_by('-id')
                if not len(correction):
                    raise ValueError(
                        "No documents were found in the %s collection for uid "
                        "%s" % (str(mds_cls), v))
                correction = correction[0]
            else:
                correction = correction[0]
            # correction = _as_document(correction)
            correction = _dereference_uid_fields(correction)
            setattr(correction_document, k, correction)
    return correction_document


@_ensure_connection
def find_corrections(newest_only=False, dereference_uids=True, **kwargs):
    """
    Parameters
    ----------
    newest_only : bool, optional
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
           - datetime.datetime.now()
    stop_time : time-like, optional
        timestamp of the latest time that a RunStart was created. See
        docs for `start_time` for examples.
    uid : str, optional
        Globally unique id string provided to metadatastore
    """
    _as_document = _AsDocument()
    return (_as_document(c) for c in _find_corrections_helper(
        newest_only=newest_only, dereference_uids=dereference_uids, **kwargs))


def _find_corrections_helper(newest_only=True, dereference_uids=True, **kwargs):
    """Helper function that does not nuke mongo fields.

    See ``find_corrections`` for relevant kwargs

    Parameters
    ----------
    newest_only : bool, optional
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
    if newest_only:
        corrections = [corrections[0]]
    if dereference_uids:
        corrections = [_dereference_uid_fields(correction) for correction in
                       corrections]
    else:
        corrections = [c for c in corrections]
    return corrections


def _find_updated_documents(document):
    """Helper function to find updated documents in a document list

    This helper function makes the assumption that, if you are calling it,
    you only care about getting the latest documents, this API does not
    support matching up a specific combination of RunStart, EventDescriptor,
    RunStop documents, etc.

    Parameters
    ----------
    document : mongoengine.Document
        A documents to find an update for
    """
    # search for mongo documents with the find_corrections_helper
    correction = _find_corrections_helper(uid=document.uid)
    # if there are no corrections, return the input document
    if correction:
        # only grab the newest correction
        return correction[0]
    # if there are no corrections, return the original
    return document


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
    as_document : generator
        Generator of mongoengine objects laundered through a safety net
    """
    _normalize_object_id(kwargs, '_id')
    _format_time(kwargs)
    with no_dereference(DocumentClass) as DocumentClass:
        # ordering by '-_id' sorts by newest first
        search_results = DocumentClass.objects(__raw__=kwargs).order_by('-id')
        if kwargs.pop('use_newest_correction', None):
            print("searching for newer documents")
            search_results = [_find_updated_documents(res) for res in
                              search_results]

        _as_document = _AsDocument()
        return (_as_document(document) for document in search_results)


@_ensure_connection
def find_run_starts(use_newest_correction=True, **kwargs):
    """Given search criteria, locate RunStart Documents.

    Parameters
    ----------
    use_newest_correction : bool
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
           - datetime.datetime.now()
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
    return _find_documents(RunStart,
                           use_newest_correction=use_newest_correction,
                           **kwargs)


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
           - datetime.datetime.now()
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
    return _find_documents(BeamlineConfig, **kwargs)


@_ensure_connection
def find_run_stops(run_start=None, use_newest_correction=True, **kwargs):
    """Given search criteria, locate RunStop Documents.

    Parameters
    ----------
    run_start : metadatastore.document.Document or str, optional
        The metadatastore run start document or the metadatastore uid to get
        the corresponding run end for
    use_newest_correction : bool
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
           - datetime.datetime.now()
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
    if run_start:
        run_start = _get_mongo_document(run_start, RunStart)
        kwargs['run_start_id'] = run_start.id
    _normalize_object_id(kwargs, 'run_start_id')
    print("kwargs: {}".format(kwargs))
    return _find_documents(RunStop,
                           use_newest_correction=use_newest_correction,
                           **kwargs)


@_ensure_connection
def find_event_descriptors(run_start=None, use_newest_correction=True,
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
    use_newest_correction : bool
    start_time : time-like, optional
        time-like representation of the earliest time that an EventDescriptor
        was created. Valid options are:
           - timestamps --> time.time()
           - '2015'
           - '2015-01'
           - '2015-01-30'
           - '2015-03-30 03:00:00'
           - datetime.datetime.now()
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
    if run_start:
        run_start = _get_mongo_document(run_start, RunStart)
        kwargs['run_start_id'] = run_start.id

    _normalize_object_id(kwargs, '_id')
    _normalize_object_id(kwargs, 'run_start_id')
    return _find_documents(EventDescriptor,
                           use_newest_correction=use_newest_correction,
                           **kwargs)


@_ensure_connection
def find_events(descriptor=None, **kwargs):
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
           - datetime.datetime.now()
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
    if descriptor:
        descriptor = _get_mongo_document(descriptor, EventDescriptor)
        kwargs['descriptor_id'] = descriptor.id

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
    return (reorganize_event(_as_document(name, ev, dref_dict))
            for ev in events)


@_ensure_connection
def find_last(num=1):
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
    for rs in RunStart.objects.order_by('-time'):
        if next(c) == num:
            raise StopIteration
        yield _as_document(rs)


def _todatetime(time_stamp):
    if isinstance(time_stamp, float):
        return datetime.datetime.fromtimestamp(time_stamp)
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
