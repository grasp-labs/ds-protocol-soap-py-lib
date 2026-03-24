"""
**File:** ``03_write_file_basic_with_token_exchange.py``
**Region:** ``examples/03_write_file_basic_with_token_exchange``

Example 03: Upload a file via SOAP using Basic + token exchange authentication.

The linked service authenticates against the auth WSDL and exchanges credentials
for a session key (``sKey``). The session key is injected automatically as a
keyword argument of ``ReceiveFile`` via ``body_auth_params``.

The file content (``aFile``) and file name (``sFileName``) are dynamic per call
and must be prepared before constructing the dataset settings.

Mapping from workflow v1 ``typed_properties``:
    - ``sKey``        → injected from ``linked_service._credential`` via ``body_auth_params``
    - ``aFile``       → ``bytearray`` of encoded XML content, passed via kwargs
    - ``sFileName``   → file name string, passed via kwargs
    - ``iEntityCode`` → static, passed via kwargs
    - ``sApplication``→ static, passed via kwargs
    - ``sImportCode`` → static, passed via kwargs
    - ``sUserName``   → static, passed via kwargs
"""

from __future__ import annotations

import logging
import uuid

import pandas as pd
from ds_common_logger_py_lib import Logger
from ds_resource_plugin_py_lib.common.resource.errors import ResourceException

from ds_protocol_soap_py_lib.dataset.soap import SoapDataset, SoapDatasetSettings
from ds_protocol_soap_py_lib.enums import AuthType
from ds_protocol_soap_py_lib.linked_service.soap import (
    BasicWithTokenExchangeAuthSettings,
    SoapLinkedService,
    SoapLinkedServiceSettings,
)

Logger.configure(level=logging.DEBUG)
logger = Logger.get_logger(__name__)


def main(xml_content: str, file_name: str) -> None:
    linked_service = SoapLinkedService(
        id=uuid.uuid4(),
        name="example::linked_service",
        version="1.0.0",
        settings=SoapLinkedServiceSettings(
            wsdl="https://ws.data/FileUpload.asmx?WSDL",
            auth_type=AuthType.BASIC_WITH_TOKEN_EXCHANGE,
            basic_with_token_exchange=BasicWithTokenExchangeAuthSettings(
                auth_wsdl="https://ws.auth/Authentication.asmx?WSDL",
                username="user@test.no",
                password="pass",
                auth_method="LogonKey",
                auth_method_kwargs={"sApplication": "XLEDGER"},
                credential_param_key="sKey",
            ),
        ),
    )

    # aFile and sFileName are dynamic — prepare them before constructing the dataset.
    dataset = SoapDataset(
        id=uuid.uuid4(),
        name="example::dataset",
        version="1.0.0",
        settings=SoapDatasetSettings(
            method="ReceiveFile",
            kwargs={
                "aFile": bytearray(xml_content.encode()),
                "iEntityCode": "1234",
                "sApplication": "XLEDGER",
                "sFileName": file_name,
                "sImportCode": "GL02XML",
                "sUserName": "user@test.no",
            },
        ),
        linked_service=linked_service,
        deserializer=None,
    )

    # Provide input so create() does not early-exit.
    dataset.input = pd.DataFrame([{"content": xml_content, "file_name": file_name}])

    try:
        dataset.linked_service.connect()
        dataset.create()
        logger.info("File uploaded successfully")
    except ResourceException as exc:
        logger.error("Failed to upload file: %s", exc.__dict__)
    finally:
        dataset.close()


if __name__ == "__main__":
    sample_xml = "<GLTransaction><entry>...</entry></GLTransaction>"
    main(xml_content=sample_xml, file_name="gl02.xml")
