"""
**File:** ``test_enums.py``
**Region:** ``tests/test_enums``

Enum contract tests.

Covers:
- Stability of ResourceType string values.
- Stability of AuthType string values.
- String-like behavior for serialization and logging.
"""

from __future__ import annotations

from ds_protocol_soap_py_lib.enums import AuthType, ResourceType


def test_resource_type_values_are_stable() -> None:
    """
    It defines stable string values for resource types.
    """
    assert ResourceType.LINKED_SERVICE == "ds.resource.linked-service.soap"
    assert ResourceType.DATASET == "ds.resource.dataset.soap"


def test_resource_type_is_string_like() -> None:
    """
    It behaves like a string for serialization purposes.
    """
    assert str(ResourceType.DATASET) == "ds.resource.dataset.soap"
    assert str(ResourceType.LINKED_SERVICE) == "ds.resource.linked-service.soap"


def test_auth_type_values_are_stable() -> None:
    """
    It defines stable string values for authentication types.
    """
    assert AuthType.BASIC == "Basic"
    assert AuthType.PARAMETER_BASED == "ParameterBased"


def test_auth_type_is_string_like() -> None:
    """
    It behaves like a string for serialization purposes.
    """
    assert str(AuthType.BASIC) == "Basic"
    assert str(AuthType.PARAMETER_BASED) == "ParameterBased"
