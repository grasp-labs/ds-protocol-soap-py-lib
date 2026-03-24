"""
**File:** ``mocks.py``
**Region:** ``tests/mocks``

Centralised test doubles used across unit tests.

Covers:
- Zeep-layer fakes (session, transport, service, client) for SoapLinkedService tests.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class ZeepSession:
    """
    Minimal requests.Session replacement for zeep transport tests.
    """

    auth: Any = None
    closed: bool = False

    def close(self) -> None:
        self.closed = True


@dataclass
class ZeepTransport:
    """
    Minimal zeep.Transport replacement.
    """

    session: ZeepSession = field(default_factory=ZeepSession)


class ZeepService:
    """
    Minimal zeep service proxy that records calls and supports configured responses/errors.
    """

    def __init__(
        self,
        responses: dict[str, Any] | None = None,
        errors: dict[str, Exception] | None = None,
    ) -> None:
        self._responses: dict[str, Any] = responses or {}
        self._errors: dict[str, Exception] = errors or {}
        self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    def __getattr__(self, name: str):  # type: ignore[override]
        def method(*args: Any, **kwargs: Any) -> Any:
            self.calls.append((name, args, kwargs))
            if name in self._errors:
                raise self._errors[name]
            return self._responses.get(name)

        return method


@dataclass
class ZeepClientStub:
    """
    Minimal zeep.Client replacement for linked service unit tests.
    """

    transport: ZeepTransport = field(default_factory=ZeepTransport)
    service: ZeepService = field(default_factory=ZeepService)
