"""
**File:** ``test_linked_service_soap.py``
**Region:** ``tests/linked_service/test_linked_service_soap``

SoapLinkedService behaviour tests.

Covers:
- connection property (raises before connect, returns client after).
- connect() authentication branches (Basic/ParameterBased) and error handling.
- connect() raises LinkedServiceException when auth test fails.
- test_connection() success/failure signalling.
- close() resource release and idempotency.
- Context manager contract.
- body_auth_params property for parameter-based auth.
"""

from __future__ import annotations

import uuid
from typing import cast

import pytest
import requests
from ds_resource_plugin_py_lib.common.resource.linked_service.errors import (
    ConnectionError,
    LinkedServiceException,
)
from requests.auth import HTTPBasicAuth

from ds_protocol_soap_py_lib.enums import AuthType
from ds_protocol_soap_py_lib.linked_service.soap import (
    BasicAuthSettings,
    ParameterBasedAuthSettings,
    SoapLinkedService,
    SoapLinkedServiceSettings,
)
from tests.mocks import ZeepClientStub, ZeepService

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────


def make_service(
    auth_type: AuthType = AuthType.BASIC,
    basic: BasicAuthSettings | None = None,
    parameter_based: ParameterBasedAuthSettings | None = None,
    auth_test_method: str = "Ping",
) -> SoapLinkedService:
    if auth_type == AuthType.BASIC and basic is None:
        basic = BasicAuthSettings(username="user", password="pass")
    if auth_type == AuthType.PARAMETER_BASED and parameter_based is None:
        parameter_based = ParameterBasedAuthSettings(
            auth_param_key1="apiKey",
            auth_param_value1="token",
        )
    return SoapLinkedService(
        id=uuid.uuid4(),
        name="test::service",
        version="1.0.0",
        settings=SoapLinkedServiceSettings(
            wsdl="https://example.com?wsdl",
            auth_type=auth_type,
            basic=basic,
            parameter_based=parameter_based,
            auth_test_method=auth_test_method,
        ),
    )


# ─────────────────────────────────────────────────────────────────────────────
# connection property
# ─────────────────────────────────────────────────────────────────────────────


def test_connection_raises_before_connect() -> None:
    """
    It raises ConnectionError when accessed before connect() is called.
    """
    service = make_service()
    with pytest.raises(ConnectionError):
        _ = service.connection


def test_connection_returns_client_after_connect(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    It returns the zeep Client stored by connect().
    """
    service = make_service()
    fake_client = ZeepClientStub()
    monkeypatch.setattr(service, "_init_client", lambda: fake_client)
    service.connect()
    assert service.connection is fake_client


# ─────────────────────────────────────────────────────────────────────────────
# connect() — Basic auth
# ─────────────────────────────────────────────────────────────────────────────


def test_connect_basic_sets_http_basic_auth_on_transport(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    It configures HTTPBasicAuth on the transport session for Basic auth.
    """
    service = make_service(
        auth_type=AuthType.BASIC,
        basic=BasicAuthSettings(username="alice", password="secret"),
    )
    fake_client = ZeepClientStub()
    monkeypatch.setattr(service, "_init_client", lambda: fake_client)
    service.connect()

    auth = fake_client.transport.session.auth
    assert isinstance(auth, HTTPBasicAuth)
    assert auth.username == "alice"
    assert auth.password == "secret"


def test_connect_basic_requires_basic_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    It raises LinkedServiceException when Basic auth settings are missing.
    """
    service = make_service(auth_type=AuthType.BASIC, basic=None)
    service.settings.basic = None
    fake_client = ZeepClientStub()
    monkeypatch.setattr(service, "_init_client", lambda: fake_client)
    with pytest.raises(LinkedServiceException):
        service.connect()


def test_connect_stores_client_after_connect(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    It stores the initialised client on _client after connect().
    """
    service = make_service()
    fake_client = ZeepClientStub()
    monkeypatch.setattr(service, "_init_client", lambda: fake_client)
    service.connect()
    assert service._client is fake_client


def test_connect_reconnects_on_second_call(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    It re-initialises the client on each connect() call.
    """
    service = make_service()
    client1 = ZeepClientStub()
    client2 = ZeepClientStub()
    clients = iter([client1, client2])
    monkeypatch.setattr(service, "_init_client", lambda: next(clients))
    service.connect()
    service.connect()
    assert service._client is client2


# ─────────────────────────────────────────────────────────────────────────────
# connect() — ParameterBased auth
# ─────────────────────────────────────────────────────────────────────────────


def test_connect_parameter_based_validates_settings_are_present(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    It succeeds without touching transport auth for parameter-based auth.
    """
    service = make_service(auth_type=AuthType.PARAMETER_BASED)
    fake_client = ZeepClientStub()
    monkeypatch.setattr(service, "_init_client", lambda: fake_client)
    service.connect()
    assert service._client is fake_client
    assert fake_client.transport.session.auth is None


def test_connect_parameter_based_raises_if_settings_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    It raises LinkedServiceException when parameter-based auth settings are absent.
    """
    service = make_service(auth_type=AuthType.PARAMETER_BASED)
    service.settings.parameter_based = None
    fake_client = ZeepClientStub()
    monkeypatch.setattr(service, "_init_client", lambda: fake_client)
    with pytest.raises(LinkedServiceException):
        service.connect()


# ─────────────────────────────────────────────────────────────────────────────
# connect() — error paths
# ─────────────────────────────────────────────────────────────────────────────


def test_init_client_raises_connection_error_on_requests_connection_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    It raises ConnectionError when zeep.Client raises requests.ConnectionError.
    """

    service = make_service()
    monkeypatch.setattr(
        "ds_protocol_soap_py_lib.linked_service.soap.zeep.Client",
        lambda **_kwargs: (_ for _ in ()).throw(requests.exceptions.ConnectionError("unreachable")),
    )
    with pytest.raises(ConnectionError):
        service._init_client()


def test_init_client_raises_connection_error_on_generic_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    It raises ConnectionError when zeep.Client raises an unexpected exception.
    """
    service = make_service()
    monkeypatch.setattr(
        "ds_protocol_soap_py_lib.linked_service.soap.zeep.Client",
        lambda **_kwargs: (_ for _ in ()).throw(ValueError("bad wsdl")),
    )
    with pytest.raises(ConnectionError):
        service._init_client()


def test_connect_raises_connection_error_on_wsdl_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    It propagates ConnectionError raised by _init_client when WSDL cannot be fetched.
    """
    service = make_service()
    monkeypatch.setattr(
        service,
        "_init_client",
        lambda: (_ for _ in ()).throw(ConnectionError(message="Failed to reach the WSDL endpoint", details={})),
    )
    with pytest.raises(ConnectionError):
        service.connect()


def test_connect_raises_linked_service_exception_for_unsupported_auth_type(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    It raises LinkedServiceException for unknown auth_type values.
    """
    service = make_service()
    service.settings.auth_type = cast("AuthType", "WeirdAuth")
    fake_client = ZeepClientStub()
    monkeypatch.setattr(service, "_init_client", lambda: fake_client)
    with pytest.raises(LinkedServiceException):
        service.connect()


def test_connect_raises_linked_service_exception_when_auth_test_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    It raises LinkedServiceException when the auth test method call fails.
    """
    service = make_service(auth_test_method="Ping")
    fake_client = ZeepClientStub(service=ZeepService(errors={"Ping": Exception("invalid credentials")}))
    monkeypatch.setattr(service, "_init_client", lambda: fake_client)
    with pytest.raises(LinkedServiceException):
        service.connect()


def test_connect_clears_client_after_failed_auth_test(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    It resets _client to None when the auth test fails, leaving no broken state.
    """
    service = make_service(auth_test_method="Ping")
    fake_client = ZeepClientStub(service=ZeepService(errors={"Ping": Exception("invalid credentials")}))
    monkeypatch.setattr(service, "_init_client", lambda: fake_client)
    with pytest.raises(LinkedServiceException):
        service.connect()
    assert service._client is None


# ─────────────────────────────────────────────────────────────────────────────
# test_connection
# ─────────────────────────────────────────────────────────────────────────────


def test_test_connection_returns_true_empty_string_on_success() -> None:
    """
    It returns (True, "") when the test method call succeeds.
    """
    service = make_service(auth_test_method="Ping")
    service._client = ZeepClientStub(service=ZeepService(responses={"Ping": {"ok": True}}))  # type: ignore[assignment]
    ok, msg = service.test_connection()
    assert ok is True
    assert msg == ""


def test_test_connection_passes_body_auth_params_to_method() -> None:
    """
    It injects body_auth_params as keyword arguments into the test method call.
    """
    service = make_service(
        auth_type=AuthType.PARAMETER_BASED,
        parameter_based=ParameterBasedAuthSettings(
            auth_param_key1="apiKey",
            auth_param_value1="my-token",
        ),
        auth_test_method="Ping",
    )
    fake_service = ZeepService(responses={"Ping": None})
    service._client = ZeepClientStub(service=fake_service)  # type: ignore[assignment]
    service.test_connection()
    assert fake_service.calls == [("Ping", {"apiKey": "my-token"})]


def test_test_connection_returns_false_with_reason_on_exception() -> None:
    """
    It returns (False, error message) when the test method raises.
    """
    service = make_service(auth_test_method="Ping")
    service._client = ZeepClientStub(service=ZeepService(errors={"Ping": Exception("Auth failed")}))  # type: ignore[assignment]
    ok, msg = service.test_connection()
    assert ok is False
    assert "Auth failed" in msg


def test_test_connection_returns_false_when_not_connected() -> None:
    """
    It returns (False, reason) when connect() has not been called.
    """
    service = make_service(auth_test_method="Ping")
    ok, msg = service.test_connection()
    assert ok is False
    assert msg != ""


# ─────────────────────────────────────────────────────────────────────────────
# close
# ─────────────────────────────────────────────────────────────────────────────


def test_close_releases_client_and_closes_session(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    It closes the transport session and sets _client to None.
    """
    service = make_service()
    fake_client = ZeepClientStub()
    monkeypatch.setattr(service, "_init_client", lambda: fake_client)
    service.connect()
    service.close()
    assert fake_client.transport.session.closed is True
    assert service._client is None


def test_close_is_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    It does not raise when called multiple times.
    """
    service = make_service()
    fake_client = ZeepClientStub()
    monkeypatch.setattr(service, "_init_client", lambda: fake_client)
    service.connect()
    service.close()
    service.close()


def test_close_safe_before_connect() -> None:
    """
    It does not raise when called before connect().
    """
    service = make_service()
    service.close()


def test_connection_raises_after_close(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    It raises ConnectionError when connection is accessed after close().
    """
    service = make_service()
    fake_client = ZeepClientStub()
    monkeypatch.setattr(service, "_init_client", lambda: fake_client)
    service.connect()
    service.close()
    with pytest.raises(ConnectionError):
        _ = service.connection


# ─────────────────────────────────────────────────────────────────────────────
# Context manager
# ─────────────────────────────────────────────────────────────────────────────


def test_context_manager_closes_on_exit(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    It calls close() when exiting the context manager normally.
    """
    service = make_service()
    fake_client = ZeepClientStub()
    monkeypatch.setattr(service, "_init_client", lambda: fake_client)
    with service:
        service.connect()
    assert service._client is None
    assert fake_client.transport.session.closed is True


def test_context_manager_closes_on_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    It calls close() even when an exception is raised inside the context.
    """
    service = make_service()
    fake_client = ZeepClientStub()
    monkeypatch.setattr(service, "_init_client", lambda: fake_client)
    with pytest.raises(RuntimeError), service:
        service.connect()
        raise RuntimeError("boom")
    assert service._client is None


# ─────────────────────────────────────────────────────────────────────────────
# body_auth_params
# ─────────────────────────────────────────────────────────────────────────────


def test_body_auth_params_returns_empty_dict_when_parameter_based_settings_missing() -> None:
    """
    It returns an empty dict when auth_type is PARAMETER_BASED but parameter_based is None.
    """
    service = make_service(auth_type=AuthType.PARAMETER_BASED)
    service.settings.parameter_based = None
    assert service.body_auth_params == {}


def test_body_auth_params_returns_empty_dict_for_basic_auth() -> None:
    """
    It returns an empty dict when auth_type is not PARAMETER_BASED.
    """
    service = make_service(auth_type=AuthType.BASIC)
    assert service.body_auth_params == {}


def test_body_auth_params_returns_params_for_one_pair() -> None:
    """
    It returns a single key/value pair for parameter-based auth with one param.
    """
    service = make_service(
        auth_type=AuthType.PARAMETER_BASED,
        parameter_based=ParameterBasedAuthSettings(
            auth_param_key1="apiKey",
            auth_param_value1="token123",
        ),
    )
    assert service.body_auth_params == {"apiKey": "token123"}


def test_body_auth_params_returns_params_for_two_pairs() -> None:
    """
    It returns two key/value pairs when both optional params are provided.
    """
    service = make_service(
        auth_type=AuthType.PARAMETER_BASED,
        parameter_based=ParameterBasedAuthSettings(
            auth_param_key1="apiKey",
            auth_param_value1="token123",
            auth_param_key2="domain",
            auth_param_value2="example",
        ),
    )
    assert service.body_auth_params == {"apiKey": "token123", "domain": "example"}


def test_body_auth_params_returns_params_for_three_pairs() -> None:
    """
    It returns all three key/value pairs when all optional params are provided.
    """
    service = make_service(
        auth_type=AuthType.PARAMETER_BASED,
        parameter_based=ParameterBasedAuthSettings(
            auth_param_key1="apiKey",
            auth_param_value1="token123",
            auth_param_key2="domain",
            auth_param_value2="example",
            auth_param_key3="version",
            auth_param_value3="v2",
        ),
    )
    assert service.body_auth_params == {
        "apiKey": "token123",
        "domain": "example",
        "version": "v2",
    }


def test_body_auth_params_omits_pairs_with_none_value() -> None:
    """
    It skips key/value pairs where the value is None.
    """
    service = make_service(
        auth_type=AuthType.PARAMETER_BASED,
        parameter_based=ParameterBasedAuthSettings(
            auth_param_key1="apiKey",
            auth_param_value1="token123",
            auth_param_key2="unused",
            auth_param_value2=None,
        ),
    )
    assert service.body_auth_params == {"apiKey": "token123"}
