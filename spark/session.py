"""Common Spark process configuration for local and Windows execution."""

import os
import sys

import pyspark


def configure_python_worker() -> None:
    """Run Spark workers with the same interpreter as the submitted job."""
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault(
        "SPARK_HOME", os.path.dirname(os.path.realpath(pyspark.__file__))
    )
