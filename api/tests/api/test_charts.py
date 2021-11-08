
import pytest
from flask import Response

from tests.fixtures import client
from tests.helpers import response_has_data_list, response_is_successful, response_data_item_has_keys


@pytest.mark.usefixtures('client')
def test_charts_endpoint_mining_equipment_efficiency(client):
    response = client.get('/cbeci/api/charts/mining_equipment_efficiency')  # type: Response
    assert response_is_successful(response)
    assert response_has_data_list(response)
    assert response_data_item_has_keys(response, ['name', 'x', 'y'])


@pytest.mark.usefixtures('client')
def test_charts_endpoint_profitability_threshold(client):
    response = client.get('/cbeci/api/charts/profitability_threshold')  # type: Response
    assert response_is_successful(response)
    assert response_has_data_list(response)
    assert response_data_item_has_keys(response, ['x', 'y'])


@pytest.mark.usefixtures('client')
def test_charts_endpoint_mining_countries(client):
    response = client.get('/cbeci/api/charts/mining_countries')  # type: Response
    assert response_is_successful(response)
    assert response_has_data_list(response)
    assert response_data_item_has_keys(response, ['code', 'name', 'x', 'y'])


@pytest.mark.usefixtures('client')
def test_charts_endpoint_absolute_mining_countries(client):
    response = client.get('/cbeci/api/charts/absolute_mining_countries')  # type: Response
    assert response_is_successful(response)
    assert response_has_data_list(response)
    assert response_data_item_has_keys(response, ['code', 'name', 'x', 'y'])


@pytest.mark.usefixtures('client')
def test_charts_endpoint_mining_provinces(client):
    response = client.get('/cbeci/api/charts/mining_provinces')  # type: Response
    assert response_is_successful(response)
    assert response_has_data_list(response)
    assert response_data_item_has_keys(response, ['name1', 'x', 'y'])


@pytest.mark.usefixtures('client')
def test_charts_endpoint_mining_map_countries(client):
    response = client.get('/cbeci/api/charts/mining_map_countries')  # type: Response
    assert response_is_successful(response)
    assert response_has_data_list(response)
    assert response_data_item_has_keys(response, ['code', 'name', 'x', 'y'])


@pytest.mark.usefixtures('client')
def test_charts_endpoint_mining_map_provinces(client):
    response = client.get('/cbeci/api/charts/mining_map_provinces')  # type: Response
    assert response_is_successful(response)
    assert response_has_data_list(response)
    assert response_data_item_has_keys(response, ['name', 'x', 'y'])
