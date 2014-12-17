__author__ = ['arkilic', 'dill']
import getpass
import datetime
import six

from collections import OrderedDict

from pymongo.errors import OperationFailure

from metadataStore.sessionManager.databaseInit import metadataLogger
from metadataStore.utilities import validate_dict_keys

from metadataStore.dataapi.commands import (save_header, save_beamline_config,
                                            insert_event,
                                            insert_event_descriptor,
                                            find)


logger = metadataLogger.logger


def create_header(header):
    """
    Create a header

    Parameters
    ----------
    header : dict
        Header attribute-value pairs

    """
    if not isinstance(header, dict):
        raise TypeError('Header must be a Python dictionary ')

    req_key_list = ['scan_id', ]

    validate_dict_keys(header, req_key_list)

    scan_id = int(header['scan_id'])

    start_time = header.get(['start_time'],
                            datetime.datetime.utcnow())

    owner = header.get('owner', getpass.getuser())
    beamline_id = header.get('beamline_id', None)
    custom = header.get('custom', dict())
    tags = header.get('tags', list())
    status = header.get('status', 'In Progress')

    save_header(scan_id, owner=owner,
                start_time=start_time, beamline_id=beamline_id,
                tags=tags, status=status, custom=custom)


def create_beamline_config(beamline_config):
    """
    Create a beamline config entry

    Parameters
    ----------
    beamline_config : dict
        Header attribute-value pairs

    """

    if not isinstance(beamline_config, dict):
        raise TypeError('BeamlineConfig must be a Python dictionary')

    req_key_list = ['scan_id', ]
    validate_dict_keys(beamline_config, req_key_list)

    scan_id = int(beamline_config['scan_id'])

    config_params = beamline_config.get('config_params', dict())

    save_beamline_config(scan_id=scan_id, config_params=config_params)


def create_event_descriptor(event_descriptor):
    """
    Create an event descriptor entry
    """
    if not isinstance(event_descriptor, dict):
        raise TypeError('EventDescriptor must be a Python dictionary')

    req_list = ['scan_id', 'descriptor_name', 'data_keys']

    validate_dict_keys(event_descriptor, req_list)
    # mandatory values
    scan_id = event_descriptor['scan_id']
    descriptor_name = event_descriptor['descriptor_name']
    data_keys = event_descriptor['data_keys']
    # optional values
    event_type_id = event_descriptor.get('event_type_id', None)
    type_descriptor = event_descriptor.get('type_descriptor', dict())
    tag = event_descriptor.get('tag', None)
    insert_event_descriptor(scan_id, event_type_id, data_keys,
                            descriptor_name=descriptor_name,
                            tag=tag,
                            type_descriptor=type_descriptor)


def create(header=None, beamline_config=None, event_descriptor=None):
    """
    Create header, beamline_config, and event_descriptor


    Parameters
    ----------
    header : dict, optional
        Header attribute-value pairs

    beamline_config : dict, optional
        BeamlineConfig attribute-value pairs

    event_descriptor : dict, optional
        EventDescriptor attribute-value pairs


    Examples
    --------
    >>> sample_header = {'scan_id': 1234}
    >>> create(header=sample_header)

    >>> create(header={'scan_id': 1235,
    ...           'start_time': datetime.datetime.utcnow(),
    ...           'beamline_id': 'my_beamline'})

    >>> create(header={'scan_id': 1235,
    ...                'start_time': datetime.datetime.utcnow(),
    ...                'beamline_id': 'my_beamline', 'owner': 'arkilic'})

    >>> create(header={'scan_id': 1235,
    ...                'start_time': datetime.datetime.utcnow(),
    ...                'beamline_id': 'my_beamline',
    ...                'owner': 'arkilic', 'custom': {'attribute1': 'value1',
    ...                                               'attribute2':'value2'}})

    >>> create(beamline_config={'scan_id': s_id})

    >>> create(event_descriptor={'scan_id': s_id,
    ...                  'descriptor_name': 'scan',
    ...                  'event_type_id': 12, 'tag': 'experimental'})

    >>> create(event_descriptor={'scan_id': s_id, 'descriptor_name': 'scan',
    ...                   'event_type_id': 12, 'tag': 'experimental',
    ...                   'type_descriptor': {'attribute1': 'value1',
    ...                                       'attribute2': 'value2'}})


    >>> sample_event_descriptor={'scan_id': s_id, 'descriptor_name': 'scan',
    ...                          'event_type_id': 12, 'tag': 'experimental',
    ...                          'type_descriptor': {'attribute1': 'value1',
    ...                                              'attribute2': 'value2'}})
    >>> sample_header={'scan_id': 1235,
    ...                'start_time': datetime.datetime.utcnow(),
    ...                'beamline_id': 'my_beamline',
    ...                'owner': 'arkilic',
    ...                'custom': {'attribute1': 'value1',
    ...                           'attribute2':'value2'}})
    >>> create(event_descriptor=sample_event_descriptor, header=sample_header)

    >>> create(beamline_config={'scan_id': 1234})

    >>> create(beamline_config={'scan_id': 1234,
    ...                 'config_params': {'attribute1': 'value1',
    ...                                   'attribute2': 'value2'}})

    >>> sample_event_descriptor={'scan_id': s_id, 'descriptor_name': 'scan',
    ...                  'event_type_id': 12, 'tag': 'experimental',
    ...                  'type_descriptor': {'attribute1': 'value1',
    ...                                      'attribute2': 'value2'}})
    >>> sample_header={'scan_id': 1235,
    ...                'start_time': datetime.datetime.utcnow(),
    ...                'beamline_id': 'my_beamline',
    ...                'owner': 'arkilic',
    ...                'custom': {'attribute1': 'value1',
    ...                           'attribute2':'value2'}})
    >>> sample_beamline_config = {'scan_id': 1234,
    ...                      'config_params': {'attribute1': 'value1',
    ...                                        'attribute2': 'value2'}}
    >>> create(header=sample_header,
    ...        event_descriptor=sample_event_descriptor,
    ...        beamline_config=sample_beamline_config)

    """
    if header is not None:
        create_header(header)

    if beamline_config is not None:
        create_beamline_config(beamline_config)

    if event_descriptor is not None:
        create_event_descriptor(event_descriptor)


def record(scan_id, descriptor_name, seq_no, owner=None,
           data=None, description=None):
    """
    Save event given scan_id, descriptor name and optional parameters

    :param scan_id: Unique run identifier
    :type scan_id: int, required

    :param descriptor_name: EventDescriptor that serves as an Event header
    :type descriptor_name: str, required

    :param seq_no: Data point sequence number
    :type seq_no: int, required

    :param owner: Run owner(default: unix session owner)
    :type owner: str, optional

    :param data: Serves as an experimental data storage structure
    :type data: dict, optional

    :param description: Provides user specified text to describe a given event
    :type description: str, optional

    :raises: ConnectionFailure, NotUniqueError, ValueError

    >>> record(scan_id=135, descriptor_name='some_scan', seq_no=0)
    >>> record(scan_id=135, descriptor_name='some_scan',
    ...        seq_no=1, owner='arkilic')

    >>> record(scan_id=135, descriptor_name='some_scan',
    ...        seq_no=2, data={'name': 'value'})

    >>> record(scan_id=135, descriptor_name='some_scan',
           ... seq_no=2, data={'name': 'value'}, description='some entry')
    """
    if data is None:
        data = dict()
    if owner is None:
        owner = getpass.getuser()
    insert_event(scan_id=scan_id, descriptor_name=descriptor_name,
                 owner=owner, seq_no=seq_no, data=data,
                 description=description)


def _str_cast_bool(input_val, *args, **kwargs):
    if isinstance(input_val, six.string_types):
        val = input_val.lower()
        if val == "true":
            input_val = True
        elif val == "false":
            input_val = False
        else:
            raise ValueError()
    return bool(input_val)


def _isinstance(val, target_type, *args, **kwargs):
    if isinstance(val, target_type):
        return val
    else:
        # try to cast it to the target type
        val = target_type(val)
        # check for the correct instance again
        if isinstance(val, target_type):
            return val
        else:
            raise KeyError()


search_keys_dict = OrderedDict()

search_keys_dict["scan_id"] = {
    "description": "The unique identifier of the run",
    "type": int,
    "validate_fun": _isinstance
    }
search_keys_dict["owner"] = {
    "description": "The user name of the person that created the header",
    "type": str,
    "validate_fun": _isinstance
    }
search_keys_dict["start_time"] = {
    "description": "The start time in utc",
    "type": datetime.datetime,
    "validate_fun": _isinstance,
    }
search_keys_dict["end_time"] = {
    "description": "The end time in utc",
    "type": datetime.datetime,
    "validate_fun": _isinstance,
    }
search_keys_dict["num_header"] = {
    "description": ("Number of run headers to return"),
    "type": int,
    "validate_fun": _isinstance
    }
search_keys_dict["data"] = {
    "description": ("False: returns all fields except for the data. True: "
                    "returns all fields, including data (Fair warning: 'True' "
                    "might be very slow)"),
    "type": bool,
    "validate_fun": _str_cast_bool
    }

search_keys_dict["tags"] = {
    "description": "Tags for a given header",
    "type": str,
    "validate_fun": _isinstance
    }
search_keys_dict['header_id'] = {
    "description": ("Unique ID generated from a hash function"),
    "type": str,
    "validate_fun": _isinstance
}
# search_keys_dict['event_classifier'] = {
#     "description": ("Qualifies events to be returned based on given condition"),
#     "type": dict,
#     "validate_fun": _isinstance
# }


def validate(var_dict, target_dict):
    """
    Helper function to validate parameter input, strip 'None' parameters
    and attempt to cast input parameters of the wrong type to the correct
    type

    :param var_dict : Dictionary whose keys are in target_dict and whose values are to be
    type checked against the "type" field in the target_dict. None values
    cannot be typechecked and are thus added to the return_dict as None.
    :type var_dict: dict

    :param target_dict : Dictionary whose keys are input parameter names and whose value is
    a dict with "description" and "type" keys.
    :type target_dict: dict

    :returns: Validated dictionary whose keys are all in target_dict and whose
    values are correctly typed or None
    :rtype: dict


    :raises: ValueError
        - If any of the input parameters are not correctly typed or they
        cannot be cast to the correct type
        - If var_dict has no entries.
        - If target_dict has no entries.
    KeyError
        If any of the keys in var_dict are not in target_dict
    """
    # check to make sure input dictionaries have keys
    if not var_dict:
        raise ValueError("var_dict has no keys")
    if not target_dict:
        raise ValueError("target_dict has no keys")

    # init the dict to return
    typechecked_dict = {}

    for key in var_dict:
        try:
            # will raise a KeyError if key is not in target_dict
            target_type = target_dict[key]["type"]
            validate_fun = target_dict[key]["validate_fun"]
        except KeyError:
            raise KeyError("key [[{0}]] is not in the target_dict. "
                           "Typechecking cannot proceed".format(key))
        # get the value
        val = var_dict[key]
        # typecheck the value
        try:
            val = validate_fun(val, target_type)
        except ValueError:
            raise ValueError("key [[{0}]] has a value of [[{1}]] which cannot "
                             "be cast to [[{2}]]".format(
                                                   key, val, target_type))

        # add the kv pair to the typechecked dict. Note that None values are
        # still added to the dictionary
        typechecked_dict[key] = val

    return typechecked_dict


def search(owner=None, start_time=None, end_time=None, tags=None,
           scan_id=None, header_id=None,
           data=False, num_header=50, event_classifier=None):
    """
    Search the experimental database with the provided search keys.

    If no search keys are provided, the default behavior is to return
    nothing.

    :param owner: User name to search on
    :type owner:str, optional

    :param start_time: Only return results after start_time
    :type start_time: datetime, optional

    :param end_time:Only return results before start_time
    :type end_time: datetime, optional

    :param scan_id : Search by specific scan_id.  If scan_id is a string, search() will try
                     to cast it to an integer.  If this fails, an error message will be logged
    :type scan_id: int, optional

    :param tags: Provides means to create tags for run headers as in Olog
    :type description: str, optional

    :param data: True: Add data to the returned dictionary
                 False: Don't include data in the returned dictionary. If data is a string, search() will test to see
                  if it's value is "True" or "False" and
    :type data: bool, optional

    :raise: TypeError, OperationError, ValueError

    :returns list :
        If the combination of search parameters finds something, a list of
        dictionaries is returned/
        If the combination of search parameters finds nothing or no search
        parameters are provided, None is returned
    """
    search_dict = {}
    # construct a dictionary whose keys are input parameter names and whose
    # values are the input parameter values. Drop values which are None
    # get the number of arguments
    argcount = search.func_code.co_argcount
    # get the input parameter names
    varnames = search.func_code.co_varnames[:argcount]
    # create the list of input parameter values
    varvals = [locals()[v] for v in varnames]
    for name, val in zip(varnames, varvals):
        if val is not None:
            search_dict[name] = val
    # validate the search dictionary
    # print('search_dict: {}'.format(search_dict))
    # print('search_keys_dict: {}'.format(search_keys_dict))
    search_dict = validate(search_dict, search_keys_dict)

    # log the search dictionary as info
    logger.info("Search dictionary: {0}".format(search_dict))
    # actually perform the search
    try:
        result = find(**search_dict)
    except OperationFailure:
        raise
    return result
