"""
**File:** ``test_dataset_soap.py``
**Region:** ``tests/dataset/test_dataset_soap``

SoapDataset behaviour tests.

Covers:
- read(): populates output, empty response, kwargs forwarding, auth injection,
  ReadError on failure.
- create(): no-op on empty/None input, kwargs forwarding, auth injection,
  output = input copy on empty response, output = deserialized response,
  input not mutated, atomic single call, CreateError on failure.
- close(): delegates to linked service, idempotent.
- Unsupported operations (update, delete, upsert, purge, list, rename) raise
  NotSupportedError.
"""

from __future__ import annotations

import uuid
from typing import Any
from unittest.mock import patch

import pandas as pd
import pytest
from ds_resource_plugin_py_lib.common.resource.dataset.errors import (
    CreateError,
    ReadError,
)
from ds_resource_plugin_py_lib.common.resource.errors import NotSupportedError

from ds_protocol_soap_py_lib.dataset.soap import SoapDataset, SoapDatasetSettings
from ds_protocol_soap_py_lib.enums import AuthType
from ds_protocol_soap_py_lib.linked_service.soap import (
    ParameterBasedAuthSettings,
    SoapLinkedService,
    SoapLinkedServiceSettings,
)
from tests.mocks import ZeepClientStub, ZeepService

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────


def make_linked_service(
    auth_type: AuthType = AuthType.BASIC,
    parameter_based: ParameterBasedAuthSettings | None = None,
) -> SoapLinkedService:
    return SoapLinkedService(
        id=uuid.uuid4(),
        name="test::service",
        version="1.0.0",
        settings=SoapLinkedServiceSettings(
            wsdl="https://example.com?wsdl",
            auth_type=auth_type,
            auth_test_method="Ping",
            parameter_based=parameter_based,
        ),
    )


def make_dataset(
    method: str = "CreateOrders",
    kwargs: dict[str, Any] | None = None,
    linked_service: SoapLinkedService | None = None,
    soap_service: ZeepService | None = None,
) -> tuple[SoapDataset, ZeepService]:
    fake_service = soap_service or ZeepService()
    fake_client = ZeepClientStub(service=fake_service)

    ls = linked_service or make_linked_service()
    ls._client = fake_client  # type: ignore[assignment]

    dataset = SoapDataset(
        id=uuid.uuid4(),
        name="test::dataset",
        version="1.0.0",
        settings=SoapDatasetSettings(
            method=method,
            kwargs=kwargs or {},
        ),
        linked_service=ls,
    )
    return dataset, fake_service


SAMPLE_DF = pd.DataFrame([{"Id": 1, "Name": "Alice"}, {"Id": 2, "Name": "Bob"}])

# ─────────────────────────────────────────────────────────────────────────────
# read
# ─────────────────────────────────────────────────────────────────────────────


def test_read_populates_output_from_soap_response() -> None:
    """
    It deserializes the SOAP response into a DataFrame and sets output.
    """
    response_data = [{"Id": 1, "Name": "Alice"}, {"Id": 2, "Name": "Bob"}]
    dataset, _ = make_dataset(
        method="GetOrders",
        soap_service=ZeepService(responses={"GetOrders": response_data}),
    )
    with patch("ds_protocol_soap_py_lib.dataset.soap.serialize_object", return_value=response_data):
        dataset.read()

    assert len(dataset.output) == 2
    assert list(dataset.output.columns) == ["Id", "Name"]


def test_read_sets_output_to_empty_dataframe_on_empty_response() -> None:
    """
    It sets output to an empty DataFrame when the SOAP method returns nothing.
    """
    dataset, _ = make_dataset(
        method="GetOrders",
        soap_service=ZeepService(responses={"GetOrders": None}),
    )
    with patch("ds_protocol_soap_py_lib.dataset.soap.serialize_object", return_value=None):
        dataset.read()

    assert isinstance(dataset.output, pd.DataFrame)
    assert dataset.output.empty


def test_read_forwards_kwargs_to_soap_method() -> None:
    """
    It passes settings.kwargs to the SOAP method call.
    """
    dataset, fake_service = make_dataset(
        method="GetOrders",
        kwargs={"Page": "1"},
        soap_service=ZeepService(responses={"GetOrders": None}),
    )
    with patch("ds_protocol_soap_py_lib.dataset.soap.serialize_object", return_value=None):
        dataset.read()

    _, call_kwargs = fake_service.calls[0]
    assert call_kwargs["Page"] == "1"


def test_read_injects_parameter_based_auth_params() -> None:
    """
    It includes body auth params in the SOAP call for parameter-based auth.
    """
    ls = make_linked_service(
        auth_type=AuthType.PARAMETER_BASED,
        parameter_based=ParameterBasedAuthSettings(
            auth_param_key1="apiKey",
            auth_param_value1="my-token",
        ),
    )
    dataset, fake_service = make_dataset(
        method="GetOrders",
        linked_service=ls,
        soap_service=ZeepService(responses={"GetOrders": None}),
    )
    with patch("ds_protocol_soap_py_lib.dataset.soap.serialize_object", return_value=None):
        dataset.read()

    _, call_kwargs = fake_service.calls[0]
    assert call_kwargs["apiKey"] == "my-token"


def test_read_raises_read_error_when_deserializer_is_none() -> None:
    """
    It raises ReadError when the deserializer is not configured.
    """
    response_data = [{"Id": 1}]
    dataset, _ = make_dataset(
        method="GetOrders",
        soap_service=ZeepService(responses={"GetOrders": response_data}),
    )
    dataset.deserializer = None
    with patch("ds_protocol_soap_py_lib.dataset.soap.serialize_object", return_value=response_data), pytest.raises(ReadError):
        dataset.read()


def test_read_raises_read_error_on_soap_failure() -> None:
    """
    It raises ReadError when the SOAP method raises.
    """
    dataset, _ = make_dataset(
        method="GetOrders",
        soap_service=ZeepService(errors={"GetOrders": Exception("timeout")}),
    )
    with pytest.raises(ReadError):
        dataset.read()


def test_read_wraps_backend_exception_not_leaking_raw_error() -> None:
    """
    It wraps the backend exception in ReadError — the raw exception does not
    leak to the caller.
    """
    dataset, _ = make_dataset(
        method="GetOrders",
        soap_service=ZeepService(errors={"GetOrders": RuntimeError("raw backend error")}),
    )
    with pytest.raises(ReadError) as exc_info:
        dataset.read()
    assert isinstance(exc_info.value.__cause__, RuntimeError)


def test_read_returns_none() -> None:
    """
    It returns None (not the DataFrame).
    """
    dataset, _ = make_dataset(
        method="GetOrders",
        soap_service=ZeepService(responses={"GetOrders": None}),
    )
    with patch("ds_protocol_soap_py_lib.dataset.soap.serialize_object", return_value=None):
        result = dataset.read()

    assert result is None


# ─────────────────────────────────────────────────────────────────────────────
# create() — empty input
# ─────────────────────────────────────────────────────────────────────────────


def test_create_no_op_when_input_is_none() -> None:
    """
    It returns immediately without calling the SOAP method when input is None.
    """
    dataset, fake_service = make_dataset()
    dataset.input = None
    dataset.create()
    assert fake_service.calls == []


def test_create_no_op_when_input_is_empty_dataframe() -> None:
    """
    It returns immediately without calling the SOAP method when input is empty.
    """
    dataset, fake_service = make_dataset()
    dataset.input = pd.DataFrame()
    dataset.create()
    assert fake_service.calls == []


def test_create_returns_none() -> None:
    """
    It returns None on success.
    """
    dataset, _ = make_dataset()
    dataset.input = SAMPLE_DF
    result = dataset.create()
    assert result is None


# ─────────────────────────────────────────────────────────────────────────────
# create() — SOAP call
# ─────────────────────────────────────────────────────────────────────────────


def test_create_forwards_kwargs_to_soap_method() -> None:
    """
    It includes settings.kwargs in the SOAP method call.
    """
    dataset, fake_service = make_dataset(kwargs={"Version": "2"})
    dataset.input = SAMPLE_DF
    dataset.create()

    _, call_kwargs = fake_service.calls[0]
    assert call_kwargs["Version"] == "2"


def test_create_injects_parameter_based_auth_params() -> None:
    """
    It includes body auth params in the SOAP call for parameter-based auth.
    """
    ls = make_linked_service(
        auth_type=AuthType.PARAMETER_BASED,
        parameter_based=ParameterBasedAuthSettings(
            auth_param_key1="apiKey",
            auth_param_value1="my-token",
        ),
    )
    dataset, fake_service = make_dataset(linked_service=ls)
    dataset.input = SAMPLE_DF
    dataset.create()

    _, call_kwargs = fake_service.calls[0]
    assert call_kwargs["apiKey"] == "my-token"


def test_create_does_not_mutate_input() -> None:
    """
    It does not modify self.input during create().
    """
    dataset, _ = make_dataset()
    original = SAMPLE_DF.copy()
    dataset.input = SAMPLE_DF
    dataset.create()
    pd.testing.assert_frame_equal(dataset.input, original)


def test_create_is_atomic_single_soap_call() -> None:
    """
    It makes exactly one SOAP call regardless of how many rows are in input.
    """
    many_rows = pd.DataFrame([{"Id": i} for i in range(50)])
    dataset, fake_service = make_dataset()
    dataset.input = many_rows
    dataset.create()

    assert len(fake_service.calls) == 1


# ─────────────────────────────────────────────────────────────────────────────
# create() — output
# ─────────────────────────────────────────────────────────────────────────────


def test_create_sets_output_to_input_copy_on_empty_response() -> None:
    """
    It sets output to a copy of input when the SOAP method returns nothing.
    """
    dataset, _ = make_dataset(
        soap_service=ZeepService(responses={"CreateOrders": None}),
    )
    dataset.input = SAMPLE_DF
    dataset.create()

    pd.testing.assert_frame_equal(dataset.output, SAMPLE_DF)
    assert dataset.output is not dataset.input


def test_create_sets_output_to_deserialized_response() -> None:
    """
    It sets output to the deserialized response when the SOAP method returns data.
    """
    response_data = [{"Id": 1, "Status": "created"}, {"Id": 2, "Status": "created"}]
    dataset, _ = make_dataset(
        soap_service=ZeepService(responses={"CreateOrders": response_data}),
    )
    dataset.input = SAMPLE_DF

    with patch("ds_protocol_soap_py_lib.dataset.soap.serialize_object", return_value=response_data):
        dataset.create()

    assert list(dataset.output.columns) == ["Id", "Status"]
    assert len(dataset.output) == 2


# ─────────────────────────────────────────────────────────────────────────────
# create() — error handling
# ─────────────────────────────────────────────────────────────────────────────


def test_create_raises_create_error_when_deserializer_is_none() -> None:
    """
    It raises CreateError when the deserializer is not configured.
    """
    response_data = [{"Id": 1, "Status": "created"}]
    dataset, _ = make_dataset(
        soap_service=ZeepService(responses={"CreateOrders": response_data}),
    )
    dataset.input = SAMPLE_DF
    dataset.deserializer = None
    with patch("ds_protocol_soap_py_lib.dataset.soap.serialize_object", return_value=response_data):
        with pytest.raises(CreateError):
            dataset.create()


def test_create_raises_create_error_on_soap_failure() -> None:
    """
    It raises CreateError when the SOAP method raises.
    """
    dataset, _ = make_dataset(
        soap_service=ZeepService(errors={"CreateOrders": Exception("service unavailable")}),
    )
    dataset.input = SAMPLE_DF
    with pytest.raises(CreateError):
        dataset.create()


def test_create_wraps_backend_exception_not_leaking_raw_error() -> None:
    """
    It wraps the backend exception in CreateError — the raw exception does not
    leak to the caller.
    """
    dataset, _ = make_dataset(
        soap_service=ZeepService(errors={"CreateOrders": RuntimeError("raw backend error")}),
    )
    dataset.input = SAMPLE_DF
    with pytest.raises(CreateError) as exc_info:
        dataset.create()
    assert isinstance(exc_info.value.__cause__, RuntimeError)


# ─────────────────────────────────────────────────────────────────────────────
# close
# ─────────────────────────────────────────────────────────────────────────────


def test_close_delegates_to_linked_service() -> None:
    """
    It calls close() on the linked service.
    """
    dataset, _ = make_dataset()
    fake_client = dataset.linked_service._client
    dataset.close()
    assert fake_client.transport.session.closed is True
    assert dataset.linked_service._client is None


def test_close_is_idempotent() -> None:
    """
    It does not raise when called multiple times.
    """
    dataset, _ = make_dataset()
    dataset.close()
    dataset.close()


# ─────────────────────────────────────────────────────────────────────────────
# Unsupported operations
# ─────────────────────────────────────────────────────────────────────────────


def test_update_raises_not_supported_error() -> None:
    """
    It raises NotSupportedError for update().
    """
    dataset, _ = make_dataset()
    with pytest.raises(NotSupportedError):
        dataset.update()


def test_delete_raises_not_supported_error() -> None:
    """
    It raises NotSupportedError for delete().
    """
    dataset, _ = make_dataset()
    with pytest.raises(NotSupportedError):
        dataset.delete()


def test_upsert_raises_not_supported_error() -> None:
    """
    It raises NotSupportedError for upsert().
    """
    dataset, _ = make_dataset()
    with pytest.raises(NotSupportedError):
        dataset.upsert()


def test_purge_raises_not_supported_error() -> None:
    """
    It raises NotSupportedError for purge().
    """
    dataset, _ = make_dataset()
    with pytest.raises(NotSupportedError):
        dataset.purge()


def test_list_raises_not_supported_error() -> None:
    """
    It raises NotSupportedError for list().
    """
    dataset, _ = make_dataset()
    with pytest.raises(NotSupportedError):
        dataset.list()


def test_rename_raises_not_supported_error() -> None:
    """
    It raises NotSupportedError for rename().
    """
    dataset, _ = make_dataset()
    with pytest.raises(NotSupportedError):
        dataset.rename()
