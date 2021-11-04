
import pytest
from flask import Response

from tests.fixtures import client

import json


@pytest.mark.usefixtures('client')
def test_charts_endpoint_mining_equipment_efficiency(client):
    response = client.get('/cbeci/api/charts/mining_equipment_efficiency')  # type: Response
    assert response.status_code == 200
    response_data = json.loads(response.data)
    assert 'data' in response_data
    assert type(response_data['data']) == list
    assert len(response_data['data']) > 0
    item = response_data['data'][0]
    assert 'name' in item
    assert 'x' in item
    assert 'y' in item


@pytest.mark.usefixtures('client')
def test_charts_endpoint_profitability_threshold(client):
    response = client.get('/cbeci/api/charts/profitability_threshold')  # type: Response
    assert response.status_code == 200
    response_data = json.loads(response.data)
    assert 'data' in response_data
    assert type(response_data['data']) == list
    assert len(response_data['data']) > 0
    item = response_data['data'][0]
    assert 'x' in item
    assert 'y' in item
