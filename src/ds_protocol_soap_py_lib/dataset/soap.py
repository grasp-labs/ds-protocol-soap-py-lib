"""
**File:** ``soap.py``
**Region:** ``ds_protocol_soap_py_lib/dataset/soap``

SOAP Dataset

This module implements a dataset for SOAP APIs.

Example:
    >>> import uuid
    >>> from ds_protocol_soap_py_lib import SoapLinkedService, SoapLinkedServiceSettings
    >>> from ds_protocol_soap_py_lib.dataset.soap import SoapDataset, SoapDatasetSettings
    >>> from ds_protocol_soap_py_lib.enums import AuthType
    >>> from ds_protocol_soap_py_lib.linked_service.soap import ParameterBasedAuthSettings
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
    >>> linked_service.connect()
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

import builtins
from dataclasses import dataclass, field
from typing import Any, Generic, NoReturn, TypeVar

import pandas as pd
from ds_common_logger_py_lib import Logger
from ds_resource_plugin_py_lib.common.resource.dataset import (
    DatasetSettings,
    DatasetStorageFormatType,
    TabularDataset,
)
from ds_resource_plugin_py_lib.common.resource.dataset.errors import (
    CreateError,
    ReadError,
)
from ds_resource_plugin_py_lib.common.resource.errors import NotSupportedError
from ds_resource_plugin_py_lib.common.serde.deserialize import PandasDeserializer
from ds_resource_plugin_py_lib.common.serde.serialize import PandasSerializer
from zeep.helpers import serialize_object

from ..enums import ResourceType
from ..linked_service.soap import SoapLinkedService

logger = Logger.get_logger(__name__, package=True)


@dataclass(kw_only=True)
class SoapDatasetSettings(DatasetSettings):
    """
    Settings for SOAP dataset.
    """

    method: str
    """The SOAP method to call."""

    kwargs: dict[str, Any] = field(default_factory=dict)
    """
    Additional keyword arguments to pass to the SOAP method alongside
    the auth parameters from the linked service.
    """


SoapDatasetSettingsType = TypeVar(
    "SoapDatasetSettingsType",
    bound=SoapDatasetSettings,
)
SoapLinkedServiceType = TypeVar(
    "SoapLinkedServiceType",
    bound=SoapLinkedService[Any],
)


@dataclass(kw_only=True)
class SoapDataset(
    TabularDataset[
        SoapLinkedServiceType,
        SoapDatasetSettingsType,
        PandasSerializer,
        PandasDeserializer,
    ],
    Generic[SoapLinkedServiceType, SoapDatasetSettingsType],
):
    """
    Dataset for SOAP APIs.

    Calls a configured SOAP method via the linked service connection.
    Authentication parameters from the linked service are automatically
    injected into each call.

    ``read()`` fetches data from the SOAP endpoint and populates ``self.output``.
    ``create()`` sends data to the SOAP endpoint.
    All other operations raise ``NotSupportedError``.
    """

    linked_service: SoapLinkedServiceType
    settings: SoapDatasetSettingsType

    deserializer: PandasDeserializer | None = field(
        default_factory=lambda: PandasDeserializer(format=DatasetStorageFormatType.SEMI_STRUCTURED_JSON),
    )

    @property
    def type(self) -> ResourceType:
        return ResourceType.DATASET

    def _invoke_method(self, error_cls: builtins.type[ReadError | CreateError]) -> Any:
        """
        Call the configured SOAP method and return the serialized response.

        Returns ``None`` if the SOAP response is empty.

        Args:
            error_cls: The error class to raise on failure (``ReadError`` or ``CreateError``).

        Raises:
            ReadError | CreateError: If the SOAP call fails.
        """
        auth_params = self.linked_service.body_auth_params or {}

        try:
            method = getattr(self.linked_service.connection.service, self.settings.method)
            response = method(**auth_params, **self.settings.kwargs)
        except Exception as exc:
            logger.exception("SOAP call failed")
            raise error_cls(
                message=f"SOAP call failed for method {self.settings.method}: {exc}",
                details={"type": self.type.value, "method": self.settings.method},
            ) from exc

        serialized = serialize_object(response)  # type: ignore[no-untyped-call]

        if not serialized:
            logger.info(f"SOAP method {self.settings.method} returned empty response")
            return None

        return serialized

    def read(self) -> None:
        """
        Call the configured SOAP method and populate ``self.output``.

        The zeep response is serialised to native Python types via
        ``zeep.helpers.serialize_object`` and normalised into a DataFrame.

        Raises:
            ReadError: If the SOAP call fails or no deserializer is configured.
        """
        logger.info(f"Calling SOAP method {self.settings.method}")
        serialized = self._invoke_method(ReadError)

        if not serialized:
            self.output = pd.DataFrame()
            return

        if not self.deserializer:
            raise ReadError(
                message="No deserializer configured for SOAP dataset",
                details={"type": self.type.value, "method": self.settings.method},
            )

        self.output = self.deserializer(serialized)

    def create(self) -> None:
        """
        Call the configured SOAP method to create an entity.

        Returns immediately if ``self.input`` is empty (no-op).
        Calls the SOAP method with auth params and ``settings.kwargs``.
        Sets ``self.output`` to the deserialized backend response, or a
        copy of ``self.input`` if the response is empty or no deserializer is configured.

        Raises:
            CreateError: If the SOAP call fails.
        """
        if self.input is None or self.input.empty:
            return

        logger.info(f"Calling SOAP method {self.settings.method}")
        serialized = self._invoke_method(CreateError)

        if not serialized or not self.deserializer:
            self.output = self.input.copy()
            return

        self.output = self.deserializer(serialized)

    def update(self) -> NoReturn:
        raise NotSupportedError("Update operation is not supported for SOAP datasets")

    def delete(self) -> NoReturn:
        raise NotSupportedError("Delete operation is not supported for SOAP datasets")

    def upsert(self) -> NoReturn:
        raise NotSupportedError("Upsert operation is not supported for SOAP datasets")

    def purge(self) -> NoReturn:
        raise NotSupportedError("Purge operation is not supported for SOAP datasets")

    def list(self) -> NoReturn:
        raise NotSupportedError("List operation is not supported for SOAP datasets")

    def rename(self) -> NoReturn:
        raise NotSupportedError("Rename operation is not supported for SOAP datasets")

    def close(self) -> None:
        """
        Close the linked service connection.
        """
        self.linked_service.close()
