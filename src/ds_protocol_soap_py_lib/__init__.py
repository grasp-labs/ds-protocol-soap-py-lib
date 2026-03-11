"""
**File:** ``__init__.py``
**Region:** ``ds-protocol-soap-py-lib``

Description
-----------
A Python package from the ds-protocol-soap-py-lib library.

Example
-------
.. code-block:: python

    from ds_protocol_soap_py_lib import __version__

    print(f"Package version: {__version__}")
"""

from importlib.metadata import version

PACKAGE_NAME = "ds-protocol-soap-py-lib"
__version__ = version(PACKAGE_NAME)

from .dataset import SoapDataset, SoapDatasetSettings  # noqa: E402
from .linked_service import SoapLinkedService, SoapLinkedServiceSettings  # noqa: E402

__all__ = [
    "SoapDataset",
    "SoapDatasetSettings",
    "SoapLinkedService",
    "SoapLinkedServiceSettings",
    "__version__",
]
