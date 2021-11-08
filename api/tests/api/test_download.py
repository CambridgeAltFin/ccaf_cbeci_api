
import pytest
from flask import Response

from tests.fixtures import client
from tests.helpers import response_is_successful, response_is_csv, csv_has_headers


@pytest.mark.usefixtures('client')
def test_download_endpoint_v1_1_0_download_mining_countries(client):
    response = client.get('/cbeci/api/v1.1.0/download/mining_countries')  # type: Response
    assert response_is_successful(response)
    assert response_is_csv(response)
    assert csv_has_headers(response, ['Date', 'Country', 'Share of global hashrate'])


@pytest.mark.usefixtures('client')
def test_download_endpoint_v1_1_1_download_mining_countries(client):
    response = client.get('/cbeci/api/v1.1.1/download/mining_countries')  # type: Response
    assert response_is_successful(response)
    assert response_is_csv(response)
    assert csv_has_headers(response, ['date', 'country', 'monthly_hashrate_%', 'monthly_absolute_hashrate_EH/S'])


@pytest.mark.usefixtures('client')
def test_download_endpoint_v1_1_0_download_mining_provinces(client):
    response = client.get('/cbeci/api/v1.1.0/download/mining_provinces')  # type: Response
    assert response_is_successful(response)
    assert response_is_csv(response)
    assert csv_has_headers(response, ['Date', 'Province', 'Share of Chinese hashrate'])


@pytest.mark.usefixtures('client')
def test_download_endpoint_v1_1_1_download_mining_provinces(client):
    response = client.get('/cbeci/api/v1.1.1/download/mining_provinces')  # type: Response
    assert response_is_successful(response)
    assert response_is_csv(response)
    assert csv_has_headers(response, ['Date', 'Province', 'Share of Chinese hashrate'])


@pytest.mark.usefixtures('client')
def test_download_endpoint_v1_0_5_download_data(client):
    response = client.get('/cbeci/api/v1.0.5/download/data')  # type: Response
    assert response_is_successful(response)
    assert response_is_csv(response)
    assert csv_has_headers(response, ['Timestamp', 'Date and Time', 'MIN', 'MAX', 'GUESS'], row_i=1)


@pytest.mark.usefixtures('client')
def test_download_endpoint_v1_1_0_download_data(client):
    response = client.get('/cbeci/api/v1.1.0/download/data')  # type: Response
    assert response_is_successful(response)
    assert response_is_csv(response)
    assert csv_has_headers(response, ['Timestamp', 'Date and Time', 'MIN', 'MAX', 'GUESS'], row_i=1)


@pytest.mark.usefixtures('client')
def test_download_endpoint_v1_1_1_download_data(client):
    response = client.get('/cbeci/api/v1.1.1/download/data')  # type: Response
    assert response_is_successful(response)
    assert response_is_csv(response)
    assert csv_has_headers(response, [
        'Timestamp',
        'Date and Time',
        'power MAX, GW',
        'power MIN, GW',
        'power GUESS, GW',
        'annualised consumption MAX, TWh',
        'annualised consumption MIN, TWh',
        'annualised consumption GUESS, TWh',
    ], row_i=1)


@pytest.mark.usefixtures('client')
def test_download_endpoint_v1_1_1_download_monthly_data(client):
    response = client.get('/cbeci/api/v1.1.1/download/data/monthly')  # type: Response
    assert response_is_successful(response)
    assert response_is_csv(response)
    assert csv_has_headers(response, [
        'Month',
        'Monthly consumption, TWh',
        'Cumulative consumption, TWh',
    ], row_i=1)
