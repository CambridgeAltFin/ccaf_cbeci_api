
import pytest
from flask import Response

from tests.fixtures import client
from tests.helpers import response_is_successful


@pytest.mark.usefixtures('client')
def test_download_endpoint_text_pages(client):
    response = client.get('/cbeci/api/text_pages/')  # type: Response
    assert response_is_successful(response)


@pytest.mark.usefixtures('client')
def test_download_endpoint_sponsors(client):
    response = client.get('/cbeci/api/sponsors/')  # type: Response
    assert response_is_successful(response)


@pytest.mark.usefixtures('client')
def test_download_endpoint_reports(client):
    response = client.get('/cbeci/api/reports/')  # type: Response
    assert response_is_successful(response)
