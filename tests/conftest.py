import os
import tempfile

# Must run before claims_pipeline is imported: config resolves paths at import time.
# Always isolated, even inside the Airflow image where LAKEHOUSE_DIR points at the real lake.
os.environ["LAKEHOUSE_DIR"] = tempfile.mkdtemp(prefix="claims-lakehouse-")
os.environ.setdefault("SPARK_SHUFFLE_PARTITIONS", "2")
os.environ.setdefault("SPARK_DRIVER_MEMORY", "1g")

import pytest


@pytest.fixture(scope="session")
def spark():
    pytest.importorskip("pyspark")
    from claims_pipeline.spark import build_spark

    session = build_spark("claims-pipeline-tests")
    yield session
    session.stop()
