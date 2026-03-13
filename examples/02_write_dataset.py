"""
**File:** ``02_write_dataset.py``
**Region:** ``examples/02_write_dataset``

Example 02: Write a dataset to a SOAP API using ds-protocol-soap-py-lib.
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
            method="CreateRecords",
            kwargs={"batchSize": 100},
        ),
    )

    rows = pd.DataFrame([
        {"Id": 1, "Name": "Alice"},
        {"Id": 2, "Name": "Bob"},
    ])

    try:
        dataset.linked_service.connect()
        dataset.input = rows
        dataset.create()
        logger.debug("Schema: %s", dataset.schema)
        return dataset.output
    except ResourceException as exc:
        logger.error(f"Error writing dataset: {exc.__dict__}")
        return pd.DataFrame()
    finally:
        dataset.close()


if __name__ == "__main__":
    df = main()
    logger.debug(df)
