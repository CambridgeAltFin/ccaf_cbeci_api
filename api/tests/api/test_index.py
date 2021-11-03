
import pytest
from flask import Response

from tests.fixtures import client

import json


@pytest.mark.usefixtures('client')
def test_data_endpoint_without_price(client):
    response = client.get('/cbeci/api/data')  # type: Response

    assert response.status_code == 200
    assert b"Welcome to the CBECI API data endpoint. " \
        b"To get bitcoin electricity consumption estimate timeseries, " \
        b"specify electricity price parameter 'p' (in USD), for example /api/data?p=0.05" == response.data


@pytest.mark.usefixtures('client')
def test_data_endpoint_price_path(client):
    response = client.get('/cbeci/api/data/0.05')  # type: Response

    assert response.status_code == 200
    response_data = json.loads(response.data)
    assert 'data' in response_data
    assert type(response_data['data']) == list
    assert len(response_data['data']) > 0
    item = response_data['data'][0]
    assert 'date' in item
    assert 'guess_consumption' in item
    assert 'max_consumption' in item
    assert 'min_consumption' in item
    assert 'timestamp' in item


@pytest.mark.usefixtures('client')
def test_data_monthly_endpoint_without_price(client):
    response = client.get('/cbeci/api/data/monthly')  # type: Response

    assert response.status_code == 200
    assert b"Welcome to the CBECI API data endpoint. " \
        b"To get bitcoin electricity consumption estimate timeseries, " \
        b"specify electricity price parameter 'p' (in USD), for example /api/data?p=0.05" == response.data


@pytest.mark.usefixtures('client')
def test_data_monthly_endpoint_price_path(client):
    response = client.get('/cbeci/api/data/monthly/0.05')  # type: Response

    assert response.status_code == 200
    response_data = json.loads(response.data)
    assert 'data' in response_data
    assert type(response_data['data']) == list
    assert len(response_data['data']) > 0
    item = response_data['data'][0]
    assert 'cumulative_value' in item
    assert 'month' in item
    assert 'timestamp' in item
    assert 'value' in item
