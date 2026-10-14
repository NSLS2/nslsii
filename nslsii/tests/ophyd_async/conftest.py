import pytest
from ophyd_async.core import StaticFilenameProvider, StaticPathProvider


@pytest.fixture
def path_provider(tmp_path) -> StaticPathProvider:
    return StaticPathProvider(StaticFilenameProvider("abc"), tmp_path)
