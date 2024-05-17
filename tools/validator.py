

def type_validator(variable: any, clazz: type, nullable: bool = True) -> bool:
    if type(nullable) is not bool:
        nullable = True
    if nullable and variable is None:
        return True
    if isinstance(variable, clazz):
        return True
    return False


def key_validator(dictionary: dict, key: str, clazz: type, nullable: bool = True):
    if not isinstance(dictionary, dict):
        raise TypeError('Validator for dict key')
    if key not in dictionary or not type_validator(dictionary[key], clazz, nullable):
        raise ValueError('Value of {} should be {}'.format(key, clazz.__name__))
    return False

def dict_key_mapping(dictionary: dict, key_mapping: dict):
    for k, v in key_mapping.items():
        if k in dictionary.keys():
            dictionary[v] = dictionary.pop(k)
    return dictionary