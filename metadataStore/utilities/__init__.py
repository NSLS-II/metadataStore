__author__ = 'arkilic'


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
