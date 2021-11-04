
import pytest
from flask import Response

from tests.fixtures import client
from tests.helpers import response_has_data_list, response_is_successful, response_data_item_has_keys, item_has_keys

import json


@pytest.mark.usefixtures('client')
def test_data_endpoint_without_price(client):
    response = client.get('/cbeci/api/data/')  # type: Response

    assert response_is_successful(response)
    assert b"Welcome to the CBECI API data endpoint. " \
        b"To get bitcoin electricity consumption estimate timeseries, " \
        b"specify electricity price parameter 'p' (in USD), for example /api/data/?p=0.05" == response.data


@pytest.mark.usefixtures('client')
def test_data_endpoint_price_path(client):
    response = client.get('/cbeci/api/data/0.05')  # type: Response

    assert response_is_successful(response)
    assert response_has_data_list(response)
    assert response_data_item_has_keys(response, [
        'date',
        'guess_consumption',
        'max_consumption',
        'min_consumption',
        'timestamp'
    ])


@pytest.mark.usefixtures('client')
def test_data_monthly_endpoint_without_price(client):
    response = client.get('/cbeci/api/data/monthly/')  # type: Response

    assert response_is_successful(response)
    assert b"Welcome to the CBECI API data endpoint. " \
        b"To get bitcoin electricity consumption estimate timeseries, " \
        b"specify electricity price parameter 'p' (in USD), for example /api/data/monthly/?p=0.05" == response.data


@pytest.mark.usefixtures('client')
def test_data_monthly_endpoint_price_path(client):
    response = client.get('/cbeci/api/data/monthly/0.05')  # type: Response

    assert response_is_successful(response)
    assert response_has_data_list(response)
    assert response_data_item_has_keys(response, ['month', 'timestamp', 'value', 'cumulative_value'])


@pytest.mark.usefixtures('client')
def test_data_stats_endpoint_without_price(client):
    response = client.get('/cbeci/api/data/stats/')  # type: Response

    assert response_is_successful(response)
    assert b"Welcome to the CBECI API data endpoint. " \
        b"To get bitcoin electricity consumption estimate timeseries, " \
        b"specify electricity price parameter 'p' (in USD), for example /api/data/stats/?p=0.05" == response.data


@pytest.mark.usefixtures('client')
def test_data_stats_endpoint_price_path(client):
    response = client.get('/cbeci/api/data/stats/0.05')  # type: Response

    assert response_is_successful(response)
    response_data = json.loads(response.data)
    assert item_has_keys(response_data, [
        'guess_consumption',
        'guess_power',
        'max_consumption',
        'max_power',
        'min_consumption',
        'min_power'
    ])
