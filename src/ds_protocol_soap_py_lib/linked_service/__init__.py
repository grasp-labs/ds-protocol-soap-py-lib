"""
**File:** ``__init__.py``
**Region:** ``ds_protocol_soap_py_lib/linked_service``

SOAP Linked Service

This module implements a linked service for SOAP APIs.

Example:
    >>> import uuid
    >>> from ds_protocol_soap_py_lib.enums import AuthType
    >>> from ds_protocol_soap_py_lib.linked_service import (
    ...     BasicAuthSettings,
    ...     SoapLinkedService,
    ...     SoapLinkedServiceSettings,
    ... )
    >>> linked_service = SoapLinkedService(
    ...     id=uuid.uuid4(),
    ...     name="example::linked_service",
    ...     version="1.0.0",
    ...     settings=SoapLinkedServiceSettings(
    ...         wsdl="https://api.example.com?WSDL",
    ...         auth_type=AuthType.BASIC,
    ...         basic=BasicAuthSettings(
    ...             username="user",
    ...             password="pass",
    ...         ),
    ...     ),
    ... )
    >>> linked_service.connect()
"""

from .soap import (
    BasicAuthSettings,
    ParameterBasedAuthSettings,
    SoapLinkedService,
    SoapLinkedServiceSettings,
)

__all__ = [
    "BasicAuthSettings",
    "ParameterBasedAuthSettings",
    "SoapLinkedService",
    "SoapLinkedServiceSettings",
]
