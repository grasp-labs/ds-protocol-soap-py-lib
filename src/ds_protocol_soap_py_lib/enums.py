"""
**File:** ``enums.py``
**Region:** ``ds_protocol_soap_py_lib/enums``

Constants for SOAP protocol.

Example:
    >>> ResourceType.LINKED_SERVICE
    'DS.RESOURCE.LINKED_SERVICE.SOAP'
    >>> ResourceType.DATASET
    'DS.RESOURCE.DATASET.SOAP'
"""

from enum import StrEnum


class AuthType(StrEnum):
    """
    Constants for authentication types.
    """

    BASIC = "Basic"
    PARAMETER_BASED = "ParameterBased"


class ResourceType(StrEnum):
    """
    Constants for SOAP protocol.
    """

    LINKED_SERVICE = "DS.RESOURCE.LINKED_SERVICE.SOAP"
    DATASET = "DS.RESOURCE.DATASET.SOAP"
