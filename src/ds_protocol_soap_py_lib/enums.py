"""
**File:** ``enums.py``
**Region:** ``ds_protocol_soap_py_lib/enums``

Constants for SOAP protocol.

Example:
    >>> ResourceType.LINKED_SERVICE
    'ds.resource.linked-service.soap'
    >>> ResourceType.DATASET
    'ds.resource.dataset.soap'
"""

from enum import StrEnum


class AuthType(StrEnum):
    """
    Constants for authentication types.
    """

    BASIC = "Basic"
    BASIC_WITH_TOKEN_EXCHANGE = "BasicWithTokenExchange"  # nosec B105
    PARAMETER_BASED = "ParameterBased"


class ResourceType(StrEnum):
    """
    Constants for SOAP protocol.
    """

    LINKED_SERVICE = "ds.resource.linked-service.soap"
    DATASET = "ds.resource.dataset.soap"
