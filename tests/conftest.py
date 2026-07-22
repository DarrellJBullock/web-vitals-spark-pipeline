import pytest

from src.utils.spark import get_spark


@pytest.fixture(scope="session")
def spark():
    session = get_spark("web-vitals-pipeline-tests")
    yield session
    session.stop()
