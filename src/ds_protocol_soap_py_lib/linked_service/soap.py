"""
**File:** ``soap.py``
**Region:** ``ds_protocol_soap_py_lib/linked_service/soap``

SOAP Linked Service

This module implements a linked service for SOAP APIs.

Example:
    >>> import uuid
    >>> from ds_protocol_soap_py_lib import SoapLinkedService, SoapLinkedServiceSettings
    >>> from ds_protocol_soap_py_lib.linked_service.soap import BasicAuthSettings
    >>> from ds_protocol_soap_py_lib.enums import AuthType
    >>> linked_service = SoapLinkedService(
    ...     id=uuid.uuid4(),
    ...     name="example::linked_service",
    ...     version="1.0.0",
    ...     settings=SoapLinkedServiceSettings(
    ...         wsdl="https://api.example.com?WSDL",
    ...         auth_type=AuthType.BASIC,
    ...         auth_test_method="SomeHealthCheckMethod",
    ...         basic=BasicAuthSettings(
    ...             username="user",
    ...             password="pass",
    ...         ),
    ...     ),
    ... )
    >>> linked_service.connect()
"""

from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

import requests
import zeep
from ds_common_logger_py_lib import Logger
from ds_resource_plugin_py_lib.common.resource.linked_service import (
    LinkedService,
    LinkedServiceSettings,
)
from ds_resource_plugin_py_lib.common.resource.linked_service.errors import (
    ConnectionError,
    LinkedServiceException,
)
from requests.auth import HTTPBasicAuth
from zeep.cache import Base

from ..enums import AuthType, ResourceType

logger = Logger.get_logger(__name__, package=True)


@dataclass(kw_only=True)
class BasicAuthSettings:
    """
    Settings for SOAP Basic authentication.

    Uses standard HTTP Basic auth with base64-encoded username:password
    on the underlying requests transport.
    """

    username: str
    """The username for basic auth."""

    password: str = field(metadata={"mask": True})
    """The password for basic auth."""


@dataclass(kw_only=True)
class ParameterBasedAuthSettings:
    """
    Settings for parameter-based authentication.

    Uses custom parameters passed in the SOAP body rather than HTTP headers.
    Access the resolved parameters via ``SoapLinkedService.body_auth_params``.
    """

    auth_param_key1: str
    """The key for the first parameter used for authentication in the SOAP body."""
    auth_param_value1: str = field(metadata={"mask": True})
    """The value for the first parameter used for authentication in the SOAP body."""

    auth_param_key2: str | None = None
    """Additional auth parameter key (optional)."""
    auth_param_value2: str | None = field(default=None, metadata={"mask": True})
    """Additional auth parameter value (optional)."""

    auth_param_key3: str | None = None
    """Additional auth parameter key (optional)."""
    auth_param_value3: str | None = field(default=None, metadata={"mask": True})
    """Additional auth parameter value (optional)."""


@dataclass(kw_only=True)
class SoapLinkedServiceSettings(LinkedServiceSettings):
    """
    Settings for SOAP linked service connections.

    Provide the appropriate auth settings object based on your auth_type:

    - ``AuthType.BASIC`` → ``basic``
    - ``AuthType.PARAMETER_BASED`` → ``parameter_based``

    Example:
        >>> settings = SoapLinkedServiceSettings(
        ...     wsdl="https://api.example.com?WSDL",
        ...     auth_type=AuthType.BASIC,
        ...     auth_test_method="SomeHealthCheckMethod",
        ...     basic=BasicAuthSettings(
        ...         username="user",
        ...         password="pass",
        ...     ),
        ... )
    """

    wsdl: str
    """The WSDL endpoint URL."""

    auth_type: AuthType
    """The authentication type to use."""

    auth_test_method: str | None = None
    """
    The SOAP operation name used to verify the connection during ``connect()``
    and ``test_connection()``. Authentication in SOAP happens at call time, so
    a real method must be invoked to verify credentials.
    When ``None``, the connection test is skipped and credentials are not verified
    until the first real call.
    """

    auth_test_method_params: dict[str, Any] = field(default_factory=dict)
    """
    Optional parameters to include when calling the auth_test_method during
    connection testing. Useful if the method requires additional non-auth parameters.
    """

    # Auth-specific settings (provide one based on auth_type)
    basic: BasicAuthSettings | None = None
    """Settings for Basic authentication. Required when auth_type=AuthType.BASIC."""

    parameter_based: ParameterBasedAuthSettings | None = None
    """Settings for parameter-based authentication. Required when auth_type=AuthType.PARAMETER_BASED."""

    # Transport settings
    cache: Base | None = None
    """Optional zeep cache backend. Defaults to no cache."""

    timeout: int | float = 300
    """Timeout in seconds for WSDL loading and SOAP calls. Defaults to 300."""

    operation_timeout: int | float | None = None
    """Timeout in seconds for individual SOAP operations. Defaults to ``timeout``."""

    # zeep Settings
    strict: bool = True
    """Raise errors on WSDL non-conformance. Defaults to True."""

    raw_response: bool = False
    """Return the raw requests response instead of parsed objects. Defaults to False."""

    forbid_dtd: bool = False
    """Forbid DTD in XML responses. Defaults to False."""

    forbid_entities: bool = True
    """Forbid external entity references in XML. Defaults to True."""

    forbid_external: bool = True
    """Forbid external resource access in XML. Defaults to True."""

    xml_huge_tree: bool = False
    """Enable lxml huge_tree option for very large XML responses. Defaults to False."""

    force_https: bool = True
    """Require HTTPS for SOAP calls. Defaults to True."""

    extra_http_headers: dict[str, str] | None = None
    """Additional HTTP headers to include in every request. Defaults to None."""

    xsd_ignore_sequence_order: bool = False
    """Ignore XSD sequence ordering constraints. Defaults to False."""


SoapLinkedServiceSettingsType = TypeVar(
    "SoapLinkedServiceSettingsType",
    bound=SoapLinkedServiceSettings,
)


@dataclass(kw_only=True)
class SoapLinkedService(
    LinkedService[SoapLinkedServiceSettingsType],
    Generic[SoapLinkedServiceSettingsType],
):
    """
    Linked service for SOAP APIs.

    Wraps a ``zeep.Client`` and handles WSDL loading, transport setup, and
    authentication. The client is available via the ``connection`` property
    after ``connect()`` is called.

    Supports use as a context manager::

        with linked_service:
            result = linked_service.connection.service.SomeMethod(...)
    """

    settings: SoapLinkedServiceSettingsType

    _client: zeep.Client | None = field(default=None, init=False, repr=False, metadata={"serialize": False})

    @property
    def type(self) -> ResourceType:
        """
        Get the type of the linked service.

        Returns:
            ResourceType
        """
        return ResourceType.LINKED_SERVICE

    @property
    def connection(self) -> zeep.Client:
        """
        Return the zeep Client established by ``connect()``.

        Returns:
            zeep.Client

        Raises:
            ConnectionError: If ``connect()`` has not been called.
        """
        if self._client is None:
            raise ConnectionError(
                message="Session is not initialised. Call connect() first.",
                details={"type": self.type.value, "wsdl": self.settings.wsdl},
            )
        return self._client

    @property
    def body_auth_params(self) -> dict[str, str]:
        """
        Return the resolved authentication parameters for parameter-based auth.

        Returns:
            dict[str, str]: Auth params for PARAMETER_BASED auth, empty dict otherwise.
        """
        if self.settings.auth_type != AuthType.PARAMETER_BASED:
            return {}

        return self._build_body_auth_params()

    def _init_client(self) -> zeep.Client:
        """
        Initialise the zeep Client from the WSDL and configured transport/settings.

        Returns:
            zeep.Client

        Raises:
            ConnectionError: If the WSDL cannot be reached or parsed.
        """
        session = requests.Session()
        try:
            transport = zeep.Transport(  # type: ignore[no-untyped-call]
                session=session,
                cache=self.settings.cache,
                timeout=self.settings.timeout,
                operation_timeout=self.settings.operation_timeout,
            )
            zeep_settings = zeep.Settings(
                strict=self.settings.strict,
                raw_response=self.settings.raw_response,
                forbid_dtd=self.settings.forbid_dtd,
                forbid_entities=self.settings.forbid_entities,
                forbid_external=self.settings.forbid_external,
                xml_huge_tree=self.settings.xml_huge_tree,
                force_https=self.settings.force_https,
                extra_http_headers=self.settings.extra_http_headers,
                xsd_ignore_sequence_order=self.settings.xsd_ignore_sequence_order,
            )
            return zeep.Client(  # type: ignore[no-untyped-call]
                wsdl=self.settings.wsdl,
                transport=transport,
                settings=zeep_settings,
            )
        except requests.exceptions.ConnectionError as exc:
            session.close()
            raise ConnectionError(
                message="Failed to reach the WSDL endpoint",
                details={"type": self.type.value, "wsdl": self.settings.wsdl},
            ) from exc
        except Exception as exc:
            session.close()
            raise ConnectionError(
                message=f"Failed to initialise SOAP client: {exc}",
                details={"type": self.type.value, "wsdl": self.settings.wsdl},
            ) from exc

    def _configure_basic_auth(self, client: zeep.Client) -> None:
        """
        Configure HTTP Basic authentication on the transport session.

        Args:
            client: The zeep Client to configure.

        Raises:
            LinkedServiceException: If basic auth settings are missing.
        """
        if not self.settings.basic:
            raise LinkedServiceException(
                message="Basic auth settings are missing in the linked service settings",
                details={"type": self.type.value},
            )

        client.transport.session.auth = HTTPBasicAuth(
            self.settings.basic.username,
            self.settings.basic.password,
        )

    def _configure_parameter_based_auth(self, client: zeep.Client) -> None:  # noqa: ARG002
        """
        Validate that parameter-based auth settings are present.

        Parameters are not applied to the transport — they are passed per-call
        via ``body_auth_params``.

        Args:
            client: The zeep Client (unused; present for dispatch-table consistency).

        Raises:
            LinkedServiceException: If parameter-based auth settings are missing.
        """
        if not self.settings.parameter_based:
            raise LinkedServiceException(
                message="Parameter-based auth settings are missing in the linked service settings",
                details={"type": self.type.value},
            )

    def connect(self) -> None:
        """
        Load the WSDL and configure authentication.

        Stores the initialised ``zeep.Client`` internally so it is accessible
        via the ``connection`` property. Verifies the connection by calling
        ``auth_test_method`` before returning. Safe to call multiple times —
        re-connects on each call.

        Returns:
            None

        Raises:
            ConnectionError: If the WSDL cannot be reached or parsed.
            LinkedServiceException: If auth settings are missing, auth_type is unsupported,
                or the connection test call fails (wrong credentials, method, or parameters).
        """
        self.close()
        client = self._init_client()

        handlers: dict[str, Any] = {
            AuthType.BASIC: self._configure_basic_auth,
            AuthType.PARAMETER_BASED: self._configure_parameter_based_auth,
        }

        try:
            handlers[self.settings.auth_type](client)
        except KeyError as exc:
            raise LinkedServiceException(
                message=f"Unsupported auth_type: {self.settings.auth_type}",
                details={
                    "type": self.type.value,
                    "auth_type": self.settings.auth_type,
                    "error_type": type(exc).__name__,
                    "valid_auth_types": list(handlers.keys()),
                },
            ) from exc

        self._client = client

        if self.settings.auth_test_method is None:
            logger.warning(
                "No auth_test_method configured, skipping connection test."
                "Credentials will not be verified until first dataset operation."
            )
            return

        ok, msg = self.test_connection()
        if not ok:
            self.close()
            raise LinkedServiceException(
                message=f"Connection test failed: {msg}",
                details={
                    "type": self.type.value,
                    "wsdl": self.settings.wsdl,
                    "auth_type": self.settings.auth_type,
                    "auth_test_method": self.settings.auth_test_method,
                },
            )

    def test_connection(self) -> tuple[bool, str]:
        """
        Verify the connection to the SOAP API by calling ``auth_test_method``.

        Does not raise on failure — returns ``(False, reason)`` instead.
        Returns ``(False, "No auth_test_method configured")`` if ``auth_test_method`` is not set.

        Returns:
            tuple[bool, str]: ``(True, "")`` on success, ``(False, reason)`` on failure.
        """
        if not self.settings.auth_test_method:
            return False, "No auth_test_method configured"

        try:
            method = getattr(
                self.connection.service,
                self.settings.auth_test_method,
            )
            body_auth_params = self.body_auth_params
            method(**body_auth_params, **self.settings.auth_test_method_params)
            return True, ""
        except Exception as exc:
            return False, str(exc)

    def close(self) -> None:
        """
        Close the underlying requests session and release the zeep Client.

        Safe to call multiple times.
        """
        if self._client is not None:
            self._client.transport.session.close()
            self._client = None

    def _build_body_auth_params(self) -> dict[str, str]:
        """
        Build the authentication parameter dict for parameter-based auth.

        Returns:
            dict[str, str]
        """
        auth_settings = self.settings.parameter_based
        body_auth_params: dict[str, str] = {}

        if not auth_settings:
            return body_auth_params

        if auth_settings.auth_param_key1 and auth_settings.auth_param_value1:
            body_auth_params[auth_settings.auth_param_key1] = auth_settings.auth_param_value1
        if auth_settings.auth_param_key2 and auth_settings.auth_param_value2:
            body_auth_params[auth_settings.auth_param_key2] = auth_settings.auth_param_value2
        if auth_settings.auth_param_key3 and auth_settings.auth_param_value3:
            body_auth_params[auth_settings.auth_param_key3] = auth_settings.auth_param_value3

        return body_auth_params
