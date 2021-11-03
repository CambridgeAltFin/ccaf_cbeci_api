
import pytest

from chart_API import app


@pytest.fixture(scope='session')
def client():
    with app.test_client() as client:
        yield client
