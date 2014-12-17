__author__ = 'arkilic'
import getpass
import time
import os
from pymongo.errors import OperationFailure
from metadataStore.sessionManager.databaseInit import db
from metadataStore.database.collections import Header, BeamlineConfig, Event, EventDescriptor
from metadataStore.sessionManager.databaseInit import metadataLogger
from bson.objectid import ObjectId
from .. import header_version as CURRENT_HEADER_VERSION
from collections import defaultdict
import six

def save_header(scan_id, owner=None, start_time=None, beamline_id=None,
                header_version=None, status=None, tags=None, custom=None):
    """
    Saves a run header that serves as a container for even descriptors, beamline configurations, and events. Please see
    Introduction section for return document structure

    :param scan_id: Consumer specified id for a specific scan
    :type scan_id: int

    :param header_owner: Run header owner (default: debian session owner)
    :type header_owner: str

    :param start_time: Run header create time
    :type start_time: datetime object

    :param beamline_id: Beamline Id
    :type beamline_id: str

    :param status: Run header completion status( In Progress/Complete
    :type status: str

    :param custom: Additional attribute value fields that can be user defined
    :type custom: dict

    :returns: header object

    >>> save_header(scan_id=12)
    >>> save_header(scan_id=13, owner='arkilic')
    >>> save_header(scan_id=14, custom={'field1': 'value1', 'field2': 'value2'})
    >>> save_header(scan_id=15, owner='some owner', start_time=datetime.datetime(2014, 3, 4))
    >>> save_header(scan_id=15, owner='some owner', beamline_id='csx')
    >>> save_header(scan_id=15, owner='some owner', start_time=datetime.datetime(2014, 3, 4), beamline_id='csx')
    >>> save_header(scan_id=15, start_time=datetime.datetime(2014, 3, 4), custom={'att': 'value'})
    """
    args_dict = {}
    if owner is None:
        owner = getpass.getuser()
    if start_time is None:
        start_time = time.time()
    if beamline_id is None:
        beamline_id = os.uname()[1].split('-')[0][2:]
    if header_version is None:
        header_version = CURRENT_HEADER_VERSION
    if status is None or not status in save_header.status:
        status = 'In Progress'
    if custom is None:
        custom = {}
    if tags is None:
        tags = []

    args_dict['scan_id'] = scan_id
    args_dict['owner'] = owner
    args_dict['start_time'] = start_time
    args_dict['beamline_id'] = beamline_id
    args_dict['header_version'] = header_version
    args_dict['status'] = status
    args_dict['tags'] = tags
    args_dict['custom'] = custom

    try:
        header = Header(**args_dict).save(wtimeout=100, write_concern={'w': 1})
    except:
        metadataLogger.logger.warning('Header cannot be created')
        raise
    return header

# options for the `status` input parameter
save_header.status = ['In Progress', 'Complete']


def get_header_object(id):
    """
    Return a Header instance given id (unique hashed _id field assigned by mongo daemon).

    :param id: Header _id
    :type id: pymongo.ObjectId instance

    :returns: Header instance
    """
    try:
        header_object = db['header'].find({'_id': id})
    except:
        raise
    return header_object


def save_bulk_header(header_list):
    """
    Given a list of headers, creates a set of headers in bulk.

    :param header_list: List of headers from collection api to be created in bulk
    :type header_list: list

    :raises: OperationError, TypeError

    :returns: None
    """
    if isinstance(header_list, list):
        try:
            db['header'].insert(header_list)
        except:
            raise
    else:
        raise TypeError('header_list must be a python list')


def get_header_id(scan_id):
    """
    Retrieve Header _id given scan_id

    :param scan_id: scan_id for a run header
    :type scan_id: int

    :returns: header_id
    """
    collection = db['header']
    try:
        hdr_crsr = collection.find({'scan_id': scan_id}).limit(1)
    except:
        raise
    if hdr_crsr.count() == 0:
        raise Exception('header_id cannot be retrieved. Please validate scan_id')
    result = hdr_crsr[0]['_id']
    return result


def insert_event_descriptor(scan_id, event_type_id, data_keys, descriptor_name=None, type_descriptor=dict(),
                            tag=None):
    """
    Create event_descriptor entries that serve as descriptors for given events that are part of a run header.
    EventDescriptor instances serve as headers for a given set of events that hold metadata for a run

    :param scan_id: Consumer specified id for a specific scan
    :type scan_id: int

    :param event_type_id: Simple identifier for a given event type. Refer to api documentation for specific event types
    :type event_type_id: int

    :param descriptor_name: Name information for a given event
    :type descriptor_name: str

    :param type_descriptor: Additional user/data collection define attribute-value pairs
    :type type_descriptor: dict

    :param tag: Provides information regarding nature of event
    :type tag: str

    >>> insert_event_descriptor(descriptor_id=134, header_id=1345, event_type_id=0, descriptor_name='scan')
    >>> insert_event_descriptor(descriptor_id=134, header_id=1345, event_type_id=0, descriptor_name='scan',
    ... type_descriptor={'custom_field': 'value', 'custom_field2': 'value2'})
    >>> insert_event_descriptor(descriptor_id=134, header_id=1345, event_type_id=0, descriptor_name='scan',
    ... type_descriptor={'custom_field': 'value', 'custom_field2': 'value2'}, tag='analysis')
    """
    header_id = get_header_id(scan_id)
    formatted_data_keys = replace_dot(data_keys)
    try:
        event_descriptor = EventDescriptor(header_id=header_id, event_type_id=event_type_id,
                                           descriptor_name=descriptor_name, type_descriptor=type_descriptor,
                                           data_keys=formatted_data_keys,
                                           tag=tag).save(wtimeout=100, write_concern={'w': 1})
    except:
        metadataLogger.logger.warning('EventDescriptor cannot be created')
        raise
    return event_descriptor


def replace_dot(keys):
    formatted_keys = list()
    if isinstance(keys, list):
        for entry in keys:
            if '.' in entry:
                if isinstance(entry, str):
                    formatted_keys.append(entry.replace('.', '[dot]'))
                else:
                    raise TypeError('data_key items must be strings')
            else:
                formatted_keys.append(entry)
    else:
        raise TypeError('data_keys must be a list')
    return formatted_keys


def insert_bulk_event(event_list):
    """
    Given a list of events, event entries are inserted in bulk. This routine uses the bulk execution routine made
    available in pymongo v2.6.

    :param event_list: List of events to be bulk inserted
    :type event_list: list

    :returns: None
    """
    if isinstance(event_list, list):
        bulk = db['event'].initialize_ordered_bulk_op()
        for entry in event_list:
            bulk.insert(entry)
        bulk.execute({'write_concern': 1})
    else:
        raise TypeError('header_list must be a python list')


def get_event_descriptor_hid_edid(name, s_id):
    """
    Returns specific EventDescriptor object given _id

    :param id: Unique identifier for EventDescriptor instance (refers to _id in mongodb schema)
    :type id: int

    :returns: header_id and descriptor_id
    """
    header_id = get_header_id(s_id)
    try:
        result = db['event_type_descriptor'].find({'header_id': header_id, 'descriptor_name': name}).limit(1)[0]
    except:
        raise
    return result['header_id'], result['_id']


def insert_event(scan_id, descriptor_name, seq_no, description=None,
                 owner=None, data=None):
    """
    :param descriptor_name: event_descriptor name. Used to find the foreign key for event_descriptor
    :type descriptor_id: str

    :param description: User generated text field
    :type description: str

    :param seq_no: sequence number for the data collected
    :type seq_no: int

    :param owner: data collection or system defined user info
    :type owner: str

    :param data: data point name-value pair container
    :type data: dict

    """
    if owner is None:
        owner = getpass.getuser()
    if data is None:
        data = dict()
    if description is None:
        description = 'None'
    header_id, descriptor_id = get_event_descriptor_hid_edid(descriptor_name,
                                                             scan_id)


    desc_data_keys = db['event_type_descriptor'].find_one({'_id': descriptor_id})['data_keys']
    data_keys = data.keys()
    formatted_data_keys = replace_dot(data_keys)
    formatted_data = dict()
    for entry in data_keys:
        temp = data[entry]
        if '.' in entry:
            new_key = entry.replace('.', '[dot]')
            formatted_data[new_key] = temp
        else:
            formatted_data[entry] = temp
    __validate_keys(formatted_data_keys, desc_data_keys)
    try:
        event = Event(descriptor_id=descriptor_id, header_id=header_id,
                      description=description, owner=owner, seq_no=seq_no,
                      data=formatted_data).save(wtimeout=100, write_concern={'w': 1})
    except:
        metadataLogger.logger.warning('Event cannot be recorded')
        raise
    return event


def __validate_keys(target, norm):
    """
    Given two set of keys, check if keys in target are covered within the norm
    :param target: keys to be evaluated
    :param norm: norm list to evaluate the keys
    :return: None
    """
    for key in target:
        if key in norm:
            pass
        else:
            raise ValueError('Data keys for event data and descriptor data do not match! Check ' + str(key))


def save_beamline_config(scan_id, config_params):
    """
    Save beamline configuration. See collections documentation for details.

    :param scan_id: Unique identifier for a scan
    :type scan_id: bson.ObjectId

    :param config_params: Name-value pairs that specify contents of specific configuration
    :type config_params: dict
    """
    header_id = get_header_id(scan_id)
    beamline_cfg = BeamlineConfig(header_id=header_id, config_params=config_params)
    try:
        beamline_cfg.save(wtimeout=100, write_concern={'w': 1})
    except:
        metadataLogger.logger.warning('Beamline config cannot be saved')
        raise OperationFailure('Beamline config cannot be saved')
    return beamline_cfg


def update_header_end_time(header_id, end_time):
    """
    Updates header end_time to current timestamp given end_time and header_id.

    :param header_id: Hashed unique identifier that specifies an entry into Header collection
    :type header_id: ObjectId instance

    :param end_time: Time of header closure
    :type end_time: datetime object

    :returns: None
    """
    coll = db['header']
    try:
        result = coll.find({'_id': header_id})
    except:
        raise
    original_entry = list()
    for entry in result:
        original_entry.append(entry)
    original_entry[0]['end_time'] = end_time
    try:
        coll.update({'_id': header_id}, original_entry[0] ,upsert=False)
    except:
        metadataLogger.logger.warning('Header end_time cannot be updated')
        raise


def update_header_status(header_id, status):
    """
    Updates run header status given header_id and status

    :param header_id: Hashed unique identifier that specifies an entry into Header collection
    :type header_id: ObjectId instance

    :param status: Header usage status. In Progress for open header & Complete for closed header
    :type status: str

    :return: None
    """
    coll = db['header']
    try:
        result = coll.find({'_id': header_id})
    except:
        raise
    original_entry = list()
    for entry in result:
        original_entry.append(entry)
    original_entry[0]['status'] = status
    try:
        coll.update({'_id': header_id}, original_entry[0] ,upsert=False)
    except:
        metadataLogger.logger.warning('Header end_time cannot be updated')
        raise


def find(header_id=None, scan_id=None, owner=None, start_time=None, beamline_id=None, end_time=None, data=False,
         tags=None, event_classifier=dict(), num_header=50):
    """
    Find by event_id, beamline_config_id, header_id. As of MongoEngine 0.8 the querysets utilise a local cache.
    So iterating it multiple times will only cause a single query.
    If this is not the desired behavour you can call no_cache (version 0.8.3+) to return a non-caching queryset.

    **contents=False**, only run_header information is returned.
    **contents=True** will return beamline_configs, event_descriptors, and events related to given run_header(s)

     >>> find(scan_id='last')
     >>> find(scan_id='last', contents=True)
     >>> find(scan_id=130, contents=True)
     >>> find(scan_id=[130,123,145,247,...])
     >>> find(scan_id={'start': 129, 'end': 141})
     >>> find(start_time=date.datetime(2014, 6, 13, 17, 51, 21, 987000)))
     >>> find(start_time=date.datetime(2014, 6, 13, 17, 51, 21, 987000)))
     >>> find(start_time={'start': datetime.datetime(2014, 6, 13, 17, 51, 21, 987000),
                      ... 'end': datetime.datetime(2014, 6, 13, 17, 51, 21, 987000)})
     >>> find(event_time=datetime.datetime(2014, 6, 13, 17, 51, 21, 987000)
     >>> find(event_time={'start': datetime.datetime(2014, 6, 13, 17, 51, 21, 987000})
    """
    query_dict = dict()
    try:
        coll = db['header']
    except:
        metadataLogger.logger.warning('Collection Header cannot be accessed')
        raise
    if scan_id is 'current':
        raise NotImplementedError('Current is obsolete. Please use scan_id=last or find_last() instead')
        header_cursor = coll.find().sort([('end_time', -1)]).limit(1)
        header = header_cursor[0]
        event_desc = find_event_descriptor(header['_id'])
        i = 0
        for e_d in event_desc:
            header['event_descriptor_' + str(i)] = e_d
            events = find_event(descriptor_id=e_d['_id'])
            if data is True:
                header['event_descriptor_' + str(i)]['events'] = __decode_cursor(events)
                i += 1
            else:
                i += 1
    elif scan_id is 'last':
        result = find_last()
        header = result[0]
        header['event_descriptors'] = result[1]
        header['events'] = result[2]
        header['beamline_configs'] = result[3]
    else:
        if header_id is not None:
            query_dict['_id'] = ObjectId(header_id)
        if owner is not None:
            if "*" in owner:
                query_dict['owner'] = {'$regex': "^" + owner.replace("*", ".*"), '$options': 'i'}
            else:
                query_dict['owner'] = owner
        if scan_id is not None:
            if isinstance(scan_id, int):
                query_dict['scan_id'] = scan_id
            else:
                raise TypeError('scan_id must be an integer')
        if beamline_id is not None:
            query_dict['beamline_id'] = beamline_id
        if start_time is not None:
                if isinstance(start_time, list):
                    for time_entry in start_time:
                        # __validate_time([time_entry])
                        query_dict['start_time'] = {'$in': start_time}
                elif isinstance(start_time, dict):
                    # __validate_time([start_time['start'], start_time['end']])
                    query_dict['start_time'] = {'$gte': start_time['start'], '$lt': start_time['end']}
                else:
                    # if __validate_time([start_time]):
                    query_dict['start_time'] = {'$gte': start_time,
                                                '$lt': time.time()}
        if end_time is not None:
                if isinstance(end_time, list):
                    for time_entry in end_time:
                        # __validate_time([time_entry])
                        query_dict['end_time'] = {'$in': end_time}
                elif isinstance(end_time, dict):
                    query_dict['end_time'] = {'$gte': end_time['start'], '$lt': end_time['end']}
                else:
                    query_dict['end_time'] = {'$gte': end_time,
                                              '$lt': time.time()}
        if tags is not None:
            query_dict['tags'] = {'$in': [tags]}
        header = __decode_hdr_cursor(find_header(query_dict).limit(num_header))
        hdr_keys = header.keys()
        for key in hdr_keys:
            beamline_cfg = find_beamline_config(header_id=header[key]['_id'])
            header[key]['configs'] = __decode_bcfg_cursor(beamline_cfg)
            event_desc = find_event_descriptor(header[key]['_id'])
            i = 0
            header[key]['event_descriptors'] = dict()
            for e_d in event_desc:
                tmp_data_keys = e_d['data_keys']
                new_data_keys = list()
                for raw_key in tmp_data_keys:
                    if '[dot]' in raw_key:
                        new_data_keys.append(raw_key.replace('[dot]', '.'))
                    else:
                        new_data_keys.append(raw_key)
                e_d['data_keys'] = new_data_keys
                header[key]['event_descriptors']['event_descriptor_' + str(i)] = e_d
                if data is True:
                    event_cursor = find_event(descriptor_id=e_d['_id'], event_query_dict=event_classifier)
                    k=0
                    header[key]['event_descriptors']['event_descriptor_' + str(i)]['events'] = dict()
                    for ev in event_cursor:
                        raw_data_keys = ev['data'].keys()
                        for raw_key in raw_data_keys:
                            if '[dot]' in raw_key:
                                ev['data'][__inverse_dot(raw_key)] = ev['data'].pop(raw_key)
                        header[key]['event_descriptors']['event_descriptor_' + str(i)]['events']['event_' + str(k)] = ev
                        k += 1

                    # header[key]['event_descriptors']['event_descriptor_' + str(i)]['events'] = __decode_cursor(events)
                    data_keys = __get_event_keys(header[key]['event_descriptors']['event_descriptor_' + str(i)])
                    header[key]['event_descriptors']['event_descriptor_' + str(i)]['data_keys'] = data_keys
                    i += 1
                else:
                    i += 1
        if header_id is None and scan_id is None and owner is None and start_time is None and beamline_id is None \
                and tags is None:
            header = None
    return header


def __decode_hdr_cursor(cursor_object):
    headers = dict()
    i = 0
    for temp_dict in cursor_object:
        headers['header_' + str(i)] = temp_dict
        i += 1
    return headers


def __decode_bcfg_cursor(cursor_object):
    b_configs = dict()
    i = 0
    for temp_dict in cursor_object:
        b_configs['config_'+  str(i)] = temp_dict
        i += 1
    return b_configs


def __decode_e_d_cursor(cursor_object):
    event_descriptors = dict()
    for temp_dict in cursor_object:
        tmp_data_keys = temp_dict['data_keys']
        new_data_keys = list()
        for raw_key in tmp_data_keys:
            if '[dot]' in raw_key:
                new_data_keys.append(raw_key.replace('[dot]', '.'))
            else:
                new_data_keys.append(raw_key)
        temp_dict['data_keys'] = new_data_keys
        event_descriptors['event_descriptor_' + str(temp_dict['_id'])] = temp_dict
    return event_descriptors


def __decode_cursor(cursor_object):
    """
    Parses pymongo event cursor and returns an event dictionary
    :param cursor_object: event cursor
    :return:
    """
    event_dict = dict()
    k = 0
    for ev in cursor_object:
        raw_data_keys = ev['data'].keys()
        for raw_key in raw_data_keys:
            if '[dot]' in raw_key:
                ev['data'][__inverse_dot(raw_key)] = ev['data'].pop(raw_key)
        k += 1
    return event_dict


def __get_event_keys(event_descriptor):
    #TODO: In the future, place a mechanism that assures all events have the same set of data!!
    ev_keys = list()
    if event_descriptor['events']:
        ev_keys = event_descriptor['events']['event_0']['data'].keys()
    else:
        pass
    return ev_keys


def find_header(query_dict):
    collection = db['header']
    return collection.find(query_dict)


def find_event(descriptor_id, event_query_dict={}):
    event_query_dict['descriptor_id'] = descriptor_id
    collection = db['event']
    return collection.find(event_query_dict)


def find_event_descriptor(header_id, event_query_dict={}):
    event_query_dict['header_id'] = header_id
    collection = db['event_type_descriptor']
    return collection.find(event_query_dict)


def find_beamline_config(header_id, beamline_cfg_query_dict={}):
    beamline_cfg_query_dict['header_id'] = header_id
    collection = db['beamline_config']
    return collection.find(beamline_cfg_query_dict)


def convertToHumanReadable(date_time):
    """
    converts a python datetime object to the
    format "X days, Y hours ago"

    :param date_time: Python datetime object
    :type date_time: datetime.datetime

    :return: fancy datetime
    :rtype: str
    """
    raise NotImplementedError('Time format has changed. This function has to be modified to fit time()')
    current_datetime = datetime.datetime.now()
    delta = str(current_datetime - date_time)
    if delta.find(',') > 0:
        days, hours = delta.split(',')
        days = int(days.split()[0].strip())
        hours, minutes = hours.split(':')[0:2]
    else:
        hours, minutes = delta.split(':')[0:2]
        days = 0
    days, hours, minutes = int(days), int(hours), int(minutes)
    date_lets =[]
    years, months, xdays = None, None, None
    plural = lambda x: 's' if x!=1 else ''
    if days >= 365:
        years = int(days/365)
        date_lets.append('%d year%s' % (years, plural(years)))
        days = days%365
    if days >= 30 and days < 365:
        months = int(days/30)
        date_lets.append('%d month%s' % (months, plural(months)))
        days = days%30
    if not years and days > 0 and days < 30:
        xdays =days
        date_lets.append('%d day%s' % (xdays, plural(xdays)))
    if not (months or years) and hours != 0:
        date_lets.append('%d hour%s' % (hours, plural(hours)))
    if not (xdays or months or years):
        date_lets.append('%d minute%s' % (minutes, plural(minutes)))
    return ', '.join(date_lets) + ' ago.'


def find2(header_id=None, scan_id=None, owner=None, start_time=None, beamline_id=None, end_time=None, data=False,
         tags=None, event_classifier=dict(), num_header=50):
    """
    Find by event_id, beamline_config_id, header_id. As of MongoEngine 0.8 the querysets utilise a local cache.
    So iterating it multiple times will only cause a single query.
    If this is not the desired behavour you can call no_cache (version 0.8.3+) to return a non-caching queryset.

    **contents=False**, only run_header information is returned.
    **contents=True** will return beamline_configs, event_descriptors, and events related to given run_header(s)

     >>> find(scan_id='last')
     >>> find(scan_id='last', contents=True)
     >>> find(scan_id=130, contents=True)
     >>> find(scan_id=[130,123,145,247,...])
     >>> find(scan_id={'start': 129, 'end': 141})
     >>> find(start_time=date.datetime(2014, 6, 13, 17, 51, 21, 987000)))
     >>> find(start_time=date.datetime(2014, 6, 13, 17, 51, 21, 987000)))
     >>> find(start_time={'start': datetime.datetime(2014, 6, 13, 17, 51, 21, 987000),
                      ... 'end': datetime.datetime(2014, 6, 13, 17, 51, 21, 987000)})
     >>> find(event_time=datetime.datetime(2014, 6, 13, 17, 51, 21, 987000)
     >>> find(event_time={'start': datetime.datetime(2014, 6, 13, 17, 51, 21, 987000})
    """


    query_dict = dict()
    try:
        coll = db['header']
    except:
        metadataLogger.logger.warning('Collection Header cannot be accessed')
        raise
    if scan_id is 'current':
        raise NotImplementedError('Current is obsolete. Please use scan_id=last or find_last() instead')
        header_cursor = coll.find().sort([('end_time', -1)]).limit(1)
        header = header_cursor[0]
        event_desc = find_event_descriptor(header['_id'])
        i = 0
        for e_d in event_desc:
            header['event_descriptor_' + str(i)] = e_d
            events = find_event(descriptor_id=e_d['_id'])
            if data is True:
                header['event_descriptor_' + str(i)]['events'] = __decode_cursor(events)
                i += 1
            else:
                i += 1
    elif scan_id is 'last':
        result = find_last()
        return {'headers': result[0], 'beamline_configs': result[3],
                'event_descriptors': result[1],
                'events': result[2]}

    else:
        if header_id is not None:
            query_dict['_id'] = ObjectId(header_id)
        if owner is not None:
            if "*" in owner:
                query_dict['owner'] = {'$regex': "^" + owner.replace("*", ".*"), '$options': 'i'}
            else:
                query_dict['owner'] = owner
        if scan_id is not None:
            if isinstance(scan_id, int):
                query_dict['scan_id'] = scan_id
            else:
                raise TypeError('scan_id must be an integer')
        if beamline_id is not None:
            query_dict['beamline_id'] = beamline_id
        if start_time is not None:
                if isinstance(start_time, list):
                    for time_entry in start_time:
                        query_dict['start_time'] = {'$in': start_time}
                elif isinstance(start_time, dict):
                    query_dict['start_time'] = {'$gte': start_time['start'], '$lt': start_time['end']}
                else:
                    query_dict['start_time'] = {'$gte': start_time,
                                                '$lt': time.time()}
        if end_time is not None:
                if isinstance(end_time, list):
                    query_dict['end_time'] = {'$in': end_time}
                elif isinstance(end_time, dict):
                    query_dict['end_time'] = {'$gte': end_time['start'], '$lt': end_time['end']}
                else:
                    query_dict['end_time'] = {'$gte': end_time,
                                              '$lt': time.time()}
        if tags is not None:
            query_dict['tags'] = {'$in': [tags]}
        headers = __decode_hdr_cursor2(find_header(query_dict).limit(num_header))
        beamline_configs = dict()
        event_descriptors = dict()
        events = dict()
        hdr_keys = headers.keys()
        for key in hdr_keys:
            beamline_cfg = find_beamline_config(header_id=headers[key]['_id'])
            e_desc = find_event_descriptor(headers[key]['_id'])
            for entry in beamline_cfg:
                beamline_configs[entry['_id']] = entry
            for entry in e_desc:
                event_descriptors[entry['_id']] = entry
                evts = find_event(descriptor_id=entry['_id'], event_query_dict=event_classifier)
                for entry in evts:
                    events[entry['_id']] = entry
    return {'headers': headers, 'beamline_configs': beamline_configs,
            'event_descriptors': event_descriptors,
            'events': events}


def __decode_hdr_cursor2(cursor_object):
    headers = dict()
    i = 0
    for temp_dict in cursor_object:
        headers[temp_dict['_id']] = temp_dict
        i += 1
    return headers


def __decode_bcfg_cursor2(cursor_object):
    b_configs = dict()
    for temp_dict in cursor_object:
        b_configs[temp_dict['_id']] = temp_dict
    return b_configs


def __decode_e_d_cursor2(cursor_object):
    event_descriptors = dict()
    for temp_dict in cursor_object:
        event_descriptors[temp_dict['_id']] = temp_dict
    return event_descriptors


def __decode_cursor2(cursor_object):
    events = dict()
    i = 0
    for temp_dict in cursor_object:
        events['event_' + str(i)] = temp_dict
        i += 1
    return events


def __get_event_keys2(event_descriptor):
    #TODO: In the future, place a mechanism that assures all events have the same set of data!!
    ev_keys = list()
    if event_descriptor['events']:
        ev_keys = event_descriptor['events']['event_0']['data'].keys()
    else:
        pass
    return ev_keys


def find_last():
    """
    Returns the most recently created run_header and associated info:
            list of event descriptors that correspond to the run header
            list of events that correspond to the run header
    :return: run_header(dict), event_descriptors(list),  events(list), beamline_config(list)
    :rtype: tuple
    """
    event_descriptors = list()
    events = list()
    beamline_configs = list()
    try:
        coll = db['header']
    except:
        metadataLogger.logger.warning('Collection Header cannot be accessed')
        raise
    header_cursor = db.header.find().sort([('_id', -1)]).limit(1)
    header = header_cursor[0]
    beamline_cfg_crsr = find_beamline_config(header_id=header['_id'])
    for bcfg in beamline_cfg_crsr:
        beamline_configs.append(bcfg)
    event_desc_cursor = find_event_descriptor(header['_id'])
    event_descriptors = __decode_e_d_cursor_last(event_desc_cursor)
    for event_descriptor in event_descriptors:
        ev_cursor = find_event(descriptor_id=event_descriptor['_id'])
        for entry in ev_cursor:
            raw_data_keys = entry['data'].keys()
            for raw_key in raw_data_keys:
                if '[dot]' in raw_key:
                    entry['data'][__inverse_dot(raw_key)] = entry['data'].pop(raw_key)
            events.append(entry)
    return header, event_descriptors, events, beamline_configs


def __decode_cursor_event_last(cursor_object):
    """
    Parses pymongo event cursor and returns an event list. Replaces the [dot] with .
    :param cursor_object: event cursor
    :return: events
    :rtype: list
    """
    events = list()
    new_data_dict = dict()
    for temp_dict in cursor_object:
        tmp_data_keys = temp_dict['data'].keys()
        for raw_key in tmp_data_keys:
            raw_key_content = temp_dict['data'][raw_key]
            new_data_dict[__inverse_dot(raw_key)] = raw_key_content
        events.append(new_data_dict)
    return events


def __decode_e_d_cursor_last(cursor_object):
    """
    Parses pymongo event_descriptor cursor and returns an event desc list. Replaces the [dot] with .
    :param cursor_object: event cursor
    :return: event descriptors
    :rtype: list
    """
    event_descriptors = list()
    for temp_dict in cursor_object:
        tmp_data_keys = temp_dict['data_keys']
        new_data_keys = list()
        for raw_key in tmp_data_keys:
            if '[dot]' in raw_key:
                new_data_keys.append(raw_key.replace('[dot]', '.'))
            else:
                new_data_keys.append(raw_key)
        temp_dict['data_keys'] = new_data_keys
        event_descriptors.append(temp_dict)
    return event_descriptors


def __inverse_dot(data_key):
    key = data_key.replace('[dot]', '.')
    return key


# this needs to be moved up a level or three
def validate_dict_keys(input_dict, req_set):
    """
    Validate that the required keys are in the input dictionary.
    This function returns None if the dict is valid, and raises
    `ValueError` if it is not.
    Parameters
    ----------
    input_dict : dict
        The dictionary to have it's keys validate
    req_set : iterable
        The keys that must be in the dictionary
    Raises
    ------
    `ValueError` if any of the required keys are missing
    """
    missing = []
    for k in req_set:
        if k not in input_dict:
            missing.append(k)
    if len(missing):
        missing = ', '.join('{}'.format(k) for k in missing)
        raise ValueError("The required key(s) {} are missing".format(missing))


def create_event(event):
    """
    Events are saved given scan_id and descriptor name and additional
    optional parameters.
    Required fields: scan_id, descriptor_name
    Optional fields: owner, seq_no, data, description
    Parameters
    ----------
    event : dict
         Dictionary used in order to save name-value pairs for Event entries.
         Required fields (with tautological descriptions until I find
         where they are really documented)
         ===============   ==========================
         field             value
         ===============   ==========================
         scan_id           The scan id number
         seq_no            The sequence number
         descriptor_name   The name of the descriptor
         ===============   ==========================
         Optional fields
         ===========   ============  =============
         field         description   default value
         ===========   ============  =============
         description   description   None
         owner         the owner     current user
         data          data payload  empty dict
         ===========   ============  =============
    Raises
    ------
    ConnectionFailure, NotUniqueError, ValueError
    Examples
    --------
    >>> create_event(event={'scan_id': 1344, 'descriptor_name': 'ascan'})
    >>> create_event(event={'scan_id': 1344, 'descriptor_name': 'ascan',
                  ... 'owner': 'arkilic', 'seq_no': 0,
                  ... 'data':{'motor1': 13.4,
                  ...         'image1': '/home/arkilic/sample.tiff'}})
    >>> create_event(event={'scan_id': 1344, 'descriptor_name': 'ascan',
                  ... 'owner': 'arkilic', 'seq_no': 0,
                  ... 'data':{'motor1': 13.4,
                  ...         'image1': '/home/arkilic/sample.tiff'}},
                  ... 'description': 'Linear scan')
    """
    if isinstance(event, dict):
        # validate that the required keys are in the dict
        validate_dict_keys(event, {'scan_id',
                                   'descriptor_name', 'seq_no'})
        # grab the required fields
        descriptor_name = event['descriptor_name']
        scan_id = event['scan_id']
        seq_no = event['seq_no']
        # get the optional fields, use defaults if needed
        description = event.get('description', None)
        owner = event.get('owner', getpass.getuser())
        data = event.get('data', dict())

        ret = insert_event(scan_id, descriptor_name, seq_no,
                     description=description,
                     owner=owner, data=data)
        print("the returned thing is {}".format(ret))

    elif isinstance(event, list):
        errors = []
        for idx, single_event in enumerate(event):
            try:
                create_event(single_event)
            except ValueError as ve:
                errors.append('Event {} of {} raised error: {}. \nEvent: {}'
                              ''.format(idx, len(event)-1), ve, single_event)
        if errors:
            raise ValueError(errors)
    else:
        raise ValueError("Event must be a dict or a list. You provided a {}: "
                         "{}".format(type(event), event))


def as_object(events):
    '''Convert a list of event dictionaries into an object

    Parameters
    ----------
    events : list of event dictionaries that are returned from `find_last`
        or `find2`

    Returns
    -------
    Event : object
        Event object with attributes equal to the data keys in events.  The
        run header id(s) can be accessed by Event.run_hdr_ids and the event
        descriptor id(s) can be accessed by Event.ev_desc_ids
    '''
    data_dict = defaultdict(list)
    run_hdr_ids = []
    ev_desc_ids = []
    for e in events:
        if 'time' in e:
            data_dict['time'].append(e['time'])
        elif 'time' in e['data']:
            # this will get caught by the loop over data
            pass
        else:
            # not sure if anything else should be done here
            pass

        for data_key, data_val in six.iteritems(e['data']):
            data_dict[data_key].append(data_val)

        run_hdr_ids.append(e['header_id'])
        ev_desc_ids.append(e['descriptor_id'])

    class EventObject(object):
        pass
    # create the events object
    events_object = EventObject()
    # set the attributes for the event object
    for data_key, data_val in six.iteritems(data_dict):
        setattr(events_object, data_key, data_val)
    return events_object