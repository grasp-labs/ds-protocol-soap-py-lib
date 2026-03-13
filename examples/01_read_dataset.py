"""
**File:** ``01_read_dataset.py``
**Region:** ``examples/01_read_dataset``

Example 01: Read a dataset from a SOAP API using ds-protocol-soap-py-lib.
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
    ParameterBasedAuthSettings,
    SoapLinkedService,
    SoapLinkedServiceSettings,
)

Logger.configure(level=logging.DEBUG)
logger = Logger.get_logger(__name__)


def main() -> pd.DataFrame:
    linked_service = SoapLinkedService(
        id=uuid.uuid4(),
        name="example::linked_service",
        version="1.0.0",
        settings=SoapLinkedServiceSettings(
            wsdl="https://example.com/service?wsdl",
            auth_type=AuthType.PARAMETER_BASED,
            auth_test_method="Ping",
            parameter_based=ParameterBasedAuthSettings(
                auth_param_key1="apiKey",
                auth_param_value1="******",
            ),
        ),
    )

    dataset = SoapDataset(
        id=uuid.uuid4(),
        name="example::dataset",
        version="1.0.0",
        linked_service=linked_service,
        settings=SoapDatasetSettings(
            method="GetRecords",
        ),
    )

    try:
        dataset.linked_service.connect()
        dataset.read()
        logger.debug("Schema: %s", dataset.schema)
        return dataset.output
    except ResourceException as exc:
        logger.error(f"Error reading dataset: {exc.__dict__}")
        return pd.DataFrame()
    finally:
        dataset.close()


if __name__ == "__main__":
    df = main()
    logger.info(df)
