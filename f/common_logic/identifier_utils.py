import json
import re


def _reverse_parts(k, sep="/"):
    """Reverse the parts of a string separated by a given separator.

    Parameters
    ----------
    k : str
        The string to be reversed.
    sep : str, optional
        The separator used to split and join the string parts, by default "/".

    Returns
    -------
    str
        The string with its parts reversed.
    """
    return sep.join(reversed(k.split(sep)))


def _drop_nonsql_chars(s):
    """Remove non-SQL compatible characters from a string.

    Parameters
    ----------
    s : str
        The string from which to remove non-SQL characters.

    Returns
    -------
    str
        The cleaned string with non-SQL characters removed.
    """
    return re.sub(r"[ ./?\[\]\\,<>(){}]", "", s)


def _shorten_and_uniqify(identifier, conflicts, maxlen):
    """Shorten an identifier and ensure its uniqueness within a set of conflicts.

    This function truncates an identifier to a specified maximum length and appends a
    numeric suffix if necessary to ensure uniqueness within a set of conflicting identifiers.

    Parameters
    ----------
    identifier : str
        The original identifier to be shortened and made unique.
    conflicts : set
        A set of identifiers that the new identifier must not conflict with.
    maxlen : int
        The maximum allowed length for the identifier.

    Returns
    -------
    str
        A shortened and unique version of the identifier.
    """
    counter = 1
    new_identifier = identifier[:maxlen]
    while new_identifier in conflicts:
        new_identifier = "{}_{:03d}".format(identifier[: maxlen - 4], counter)
        counter += 1
    return new_identifier


def sanitize_sql_message(
    message,
    column_renames,
    reverse_properties_separated_by=None,
    str_replace=[],
    maxlen=63,  # https://stackoverflow.com/a/27865772
):
    """Sanitize a message for SQL compatibility and rename columns.

    This function processes a message dictionary, converting lists and dictionaries
    to JSON strings, renaming columns based on provided mappings, and ensuring
    SQL compatibility of keys.

    Parameters
    ----------
    message : dict
        The original message dictionary to be sanitized.
    column_renames : dict
        A dictionary mapping original column names to their new names.
    reverse_properties_separated_by : str, optional
        A separator for reversing property names, by default None.
    str_replace : list, optional
        A list of tuples specifying string replacements, by default [].
    maxlen : int, optional
        The maximum length for SQL-compatible keys, by default 63.

    Returns
    -------
    sanitized_sql_message : dict
        The sanitized message dictionary with SQL-compatible keys.
    updated_column_renames : dict
        The updated column renames dictionary.
    """

    updated_column_renames = column_renames.copy()
    sanitized_sql_message = {}
    for original_key, value in message.items():
        if isinstance(value, list) or isinstance(value, dict):
            value = json.dumps(value)

        if original_key in updated_column_renames:
            sanitized_sql_message[updated_column_renames[original_key]] = value
            continue

        key = original_key
        if reverse_properties_separated_by:
            key = _reverse_parts(original_key, reverse_properties_separated_by)
        for args in str_replace:
            key = key.replace(*args)
        key = _drop_nonsql_chars(key)
        key = _shorten_and_uniqify(key, updated_column_renames.values(), maxlen)

        updated_column_renames[original_key] = key
        sanitized_sql_message[key] = value
    return sanitized_sql_message, updated_column_renames


def camel_to_snake(name: str) -> str:
    """
    Convert CamelCase string to snake_case.

    c.f. https://stackoverflow.com/questions/1175208/elegant-python-function-to-convert-camelcase-to-snake-case
    """
    pattern = re.compile(r"(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")

    return pattern.sub("_", name).lower()


def normalize_and_snakecase_keys(dictionary, special_case_keys=None):
    """
    Converts the keys of a dictionary from camelCase to snake_case, handling key collisions and truncating long keys.
    Optionally leaves specified keys unchanged.

    Parameters
    ----------
    dictionary : dict
        The dictionary whose keys are to be converted.
    special_case_keys : set, optional
        A set of keys that should not be converted.

    Returns
    -------
    dict
        A new dictionary with the keys converted to snake_case and truncated if necessary.
    """
    if special_case_keys is None:
        special_case_keys = set()

    new_dict = {}
    items = list(dictionary.items())
    for key, value in items:
        if key in special_case_keys:
            final_key = key
        else:
            new_key = (
                re.sub("([a-z0-9])([A-Z])", r"\1_\2", key).replace("-", "_").lower()
            )
            base_key = new_key[:61] if len(new_key) > 63 else new_key
            final_key = base_key
            if len(new_key) > 63:
                final_key = f"{base_key}_1"

            counter = 1
            while final_key in new_dict:
                counter += 1
                final_key = f"{base_key}_{counter}"

        new_dict[final_key] = value
    return new_dict
