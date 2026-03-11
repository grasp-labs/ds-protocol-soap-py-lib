from dataclasses import dataclass

from zeep.cache import Base


@dataclass(frozen=True, kw_only=True)
class TransportConfig:
    cache: Base | None = None
    timeout: int | float = 300
    operation_timeout: int | float | None = None


@dataclass(frozen=True, kw_only=True)
class SettingsConfig:
    strict: bool = True
    raw_response: bool = False
    forbid_dtd: bool = False
    forbid_entities: bool = True
    forbid_external: bool = True
    xml_huge_tree: bool = False
    force_https: bool = True
    extra_http_headers: dict[str, str] | None = None
    xsd_ignore_sequence_order: bool = False
