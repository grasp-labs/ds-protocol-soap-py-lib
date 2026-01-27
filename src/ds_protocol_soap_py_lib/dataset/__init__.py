"""
**File:** ``__init__.py``
**Region:** ``ds_protocol_soap_py_lib/dataset``

SOAP Dataset

This module implements a dataset for SOAP APIs.

Example:
    >>> import uuid
    >>> from ds_protocol_soap_py_lib.dataset.soap import SoapDataset, SoapDatasetSettings
    >>> from ds_protocol_soap_py_lib.enums import AuthType
    >>> from ds_protocol_soap_py_lib.linked_service.soap import (
    ...     ParameterBasedAuthSettings,
    ...     SoapLinkedService,
    ...     SoapLinkedServiceSettings,
    ... )
    >>> linked_service = SoapLinkedService(
    ...     id=uuid.uuid4(),
    ...     name="example::linked_service",
    ...     version="1.0.0",
    ...     settings=SoapLinkedServiceSettings(
    ...         wsdl="https://example.com/service?wsdl",
    ...         auth_type=AuthType.PARAMETER_BASED,
    ...         auth_test_method="Ping",
    ...         parameter_based=ParameterBasedAuthSettings(
    ...             auth_param_key1="apiKey",
    ...             auth_param_value1="my-token",
    ...         ),
    ...     ),
    ... )
    >>> dataset = SoapDataset(
    ...     id=uuid.uuid4(),
    ...     name="example::dataset",
    ...     version="1.0.0",
    ...     settings=SoapDatasetSettings(method="GetRecords"),
    ...     linked_service=linked_service,
    ... )
    >>> dataset.read()
    >>> data = dataset.output
"""

from .soap import SoapDataset, SoapDatasetSettings

__all__ = [
    "SoapDataset",
    "SoapDatasetSettings",
]
