# -*- coding: utf-8 -*-
"""
Created on Thu May 16 14:24:16 2019

@author: Anton
"""
from flask import Flask, jsonify, make_response, request, has_request_context, send_from_directory
from flask_cors import CORS
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from logging.handlers import RotatingFileHandler
from schema import SchemaError
from flask_swagger import swagger
from flask_swagger_ui import get_swaggerui_blueprint
from werkzeug.middleware.dispatcher import DispatcherMiddleware
from datetime import datetime
from sentry_sdk.integrations.flask import FlaskIntegration
import flask
import requests
import logging
import time
import psycopg2
import csv
import io
import os
import sentry_sdk
from services.firebase import init_app as init_firebase_app
from dotenv import load_dotenv
from config import config, start_date
from decorators.auth import AuthenticationError
from decorators.cache_control import cache_control
from extensions import cache
from services.realtime_collection import realtime_collections
from helpers import load_typed_hasrates
from forms.feedback_form import FeedbackForm
from components.energy_consumption import EnergyConsumptionServiceFactory

load_dotenv(override=True)

LOG_LEVEL = logging.INFO

SWAGGER_URL = '/api/docs/contribute'
SWAGGER_SPEC_URL = '/api/docs/spec'


def get_limiter_flag():
    val = os.environ.get("LIMITER_ENABLED")

    return val is not None and val.lower() not in ("0", "false", "no")


# loading data in cache of each worker:
def load_data():
    with psycopg2.connect(**config['blockchain_data']) as conn:
        c = conn.cursor()
        c.execute('SELECT * FROM prof_threshold WHERE timestamp >= %s', (start_date.timestamp(),))
        prof_threshold = c.fetchall()
        c.execute('SELECT * FROM hash_rate WHERE timestamp >= %s', (start_date.timestamp(),))
        hash_rate = c.fetchall()
        c.execute('SELECT * FROM energy_consumption_ma WHERE timestamp >= %s ORDER BY timestamp', (start_date.timestamp(),))
        cons = c.fetchall()
    typed_hasrates = load_typed_hasrates()
    with psycopg2.connect(**config['custom_data']) as conn2:
        c2 = conn2.cursor()
        c2.execute('SELECT * FROM miners WHERE is_active is true')
        miners=c2.fetchall()
        c2.execute('SELECT country, code, electricity_consumption, country_flag FROM countries')
        countries=c2.fetchall()
    return prof_threshold, hash_rate, miners, countries, cons, typed_hasrates


def get_hashrate():
    rate = hash_rate[-1][2]
    return int(0 if rate is None else rate)
    # return int(requests.get("https://blockchain.info/q/hashrate", timeout=3).json())


def send_err_to_slack(err, name):
    try:
        headers = {'Content-type': 'application/json',}
        data = {"text":""}
        data["text"] = f"Getting {name} failed. It unexpectedly returned: " + str(err)[0:140]
        requests.post(config['webhook_err'], headers=headers, data=str(data))
    except:
        pass # not the best practice but we want API working even if Slack msg failed for any reason


def get_request_ip():
    return request.headers.get('X-Real-Ip') or get_remote_address()


def limits_exempt_when():
    exempt_ip = os.environ.get("RATELIMIT_EXEMPT_IP")

    return exempt_ip is not None and exempt_ip.lower() not in ("0", "false", "no") and get_request_ip() == exempt_ip


def get_file_handler(filename):
    file_handler = RotatingFileHandler(filename, maxBytes=10 * 1024 * 1024, backupCount=5)  # mb * kb * b
    file_handler.setLevel(logging.INFO)

    class RequestFormatter(logging.Formatter):
        def format(self, record):
            if has_request_context():
                record.url = request.url
                record.remote_addr = get_request_ip()
            else:
                record.url = None
                record.remote_addr = None

            return super().format(record)

    formatter = RequestFormatter(
        '[%(asctime)s] %(remote_addr)s requested %(url)s\n'
        '%(levelname)s in %(module)s: %(message)s'
    )
    file_handler.setFormatter(formatter)

    return file_handler


def create_app():
    app = Flask(__name__)

    if os.environ.get('SENTRY_DSN'):
        sentry_sdk.init(
            dsn=os.environ.get('SENTRY_DSN'),
            integrations=[FlaskIntegration()],
            environment=os.environ.get('SENTRY_ENV', 'production')
        )

    app.wsgi_app = DispatcherMiddleware(app.wsgi_app, {
        '/cbeci': app
    })

    from blueprints import charts, contribute, download, text_pages, reports, sponsors, data, ghg, eth

    app.register_blueprint(charts.bp, url_prefix='/api/charts')
    app.register_blueprint(text_pages.bp, url_prefix='/api/text_pages')
    app.register_blueprint(reports.bp, url_prefix='/api/reports')
    app.register_blueprint(sponsors.bp, url_prefix='/api/sponsors')
    app.register_blueprint(contribute.bp, url_prefix='/api/contribute')
    app.register_blueprint(download.bp, url_prefix='/api/<string:version>/download')
    app.register_blueprint(data.bp, url_prefix='/api/data')
    app.register_blueprint(ghg.bp, url_prefix='/api/ghg')

    app.register_blueprint(eth.pow.charts.bp, url_prefix='/api/eth/pow/charts')
    app.register_blueprint(eth.pow.download.bp, url_prefix='/api/<string:version>/eth/pow/download')

    app.register_blueprint(eth.pos.charts.bp, url_prefix='/api/eth/pos/charts')
    app.register_blueprint(eth.pos.download.bp, url_prefix='/api/<string:version>/eth/pos/download')

    swaggerui_bp = get_swaggerui_blueprint(
        '/cbeci' + SWAGGER_URL,
        '/cbeci' + SWAGGER_SPEC_URL,
        config={
            'app_name': "Cbeci API"
        }
    )
    app.register_blueprint(swaggerui_bp, url_prefix=SWAGGER_URL)

    doc_bp = get_swaggerui_blueprint('/cbeci/api/docs', '/cbeci/api/docs/spec/index.yaml', blueprint_name='docs')
    app.register_blueprint(doc_bp, url_prefix='/api/docs')

    return app


app = create_app()
cache.init_app(app)
app.logger.setLevel(LOG_LEVEL)
app.logger.addHandler(get_file_handler("./logs/errors.log"))
ratelimit_storage_url = os.environ.get("RATELIMIT_STORAGE_URL")
if ratelimit_storage_url:
    app.config["RATELIMIT_STORAGE_URL"] = ratelimit_storage_url

CORS(app)
if get_limiter_flag():
    Limiter(
        app,
        key_func=get_request_ip,
        default_limits=["12000 per day", "300 per 10 minutes", "15 per 10 seconds"],
        default_limits_exempt_when=limits_exempt_when
    )

init_firebase_app(cert=os.path.abspath(f"../storage/firebase/service-account-cert.{os.environ.get('PROJECT_ID')}.json"))
realtime_collections.init()

# initialisation of cache vars:
prof_threshold, hash_rate, miners, countries, cons, typed_hasrates = load_data()
lastupdate = time.time()
lastupdate_power = time.time()
try:
    hashrate = get_hashrate()
except Exception as err:
    hashrate = 0
    logging.exception(str(err))
    send_err_to_slack(err, 'INIT HASHRATE')


@app.errorhandler(429)
def ratelimit_handler(e):
    app.logger.info(f"{str(e)} - IP: {get_request_ip()}")
    return make_response(
        jsonify(error="Too many requests from your IP, try again later")
        , 429
    )


@app.errorhandler(SchemaError)
def bad_request(error):
    return make_response(jsonify(error=str(error)), 422)\



@app.errorhandler(AuthenticationError)
def bad_request(error):
    return make_response(jsonify(error=str(error)), 401)


@app.errorhandler(NotImplementedError)
def bad_request(error):
    return make_response(jsonify(error=str(error)), 404)


@app.before_request
def before_request():
    global lastupdate, lastupdate_power, prof_threshold, hash_rate, miners, countries, cons, hashrate, typed_hasrates
    if time.time() - lastupdate > 3600:
        try:
            prof_threshold, hash_rate, miners, countries, cons, typed_hasrates = load_data()
        except Exception as err:
            app.logger.exception(f"Getting data from DB err: {str(err)}")
            send_err_to_slack(err, 'DB')
        else:
            lastupdate = time.time()
    if time.time() - lastupdate_power > 45:
        try:
            # if executed properly, answer should be int
            hashrate = get_hashrate()
        except Exception as err:
            app.logger.exception(str(err))
            send_err_to_slack(err, 'HASHRATE')
        finally:
            lastupdate_power = time.time()


@app.route("/api/max/<value>")
@app.route("/api/power/max/<value>")
@cache_control()
@cache.memoize()
def recalculate_power_max(value):
    energy_consumption_service = EnergyConsumptionServiceFactory.create()
    try:
        max_power = energy_consumption_service.get_actual_data(float(value))['max_power']
    except:
        max_power = 'mining is not profitable'
    return jsonify(max_power)


@app.route("/api/min/<value>")
@app.route("/api/power/min/<value>")
@cache_control()
@cache.memoize()
def recalculate_power_min(value):
    energy_consumption_service = EnergyConsumptionServiceFactory.create()
    try:
        min_power = energy_consumption_service.get_actual_data(float(value))['min_power']
    except:
        min_power = 'mining is not profitable'
    return jsonify(min_power)


@app.route("/api/guess/<value>")
@app.route("/api/power/guess/<value>")
@cache_control()
@cache.memoize()
def recalculate_power_guess(value):
    energy_consumption_service = EnergyConsumptionServiceFactory.create()
    try:
        guess_power = energy_consumption_service.get_actual_data(float(value))['guess_power']
    except:
        guess_power = 'mining is not profitable'
    return jsonify(guess_power)


@app.route("/api/consumption/max/<value>")
@cache_control()
@cache.memoize()
def recalculate_consumption_max(value):
    energy_consumption_service = EnergyConsumptionServiceFactory.create()
    try:
        max_consumption = energy_consumption_service.get_actual_data(float(value))['max_consumption']
    except:
        max_consumption = 'mining is not profitable'
    return jsonify(max_consumption)


@app.route("/api/consumption/min/<value>")
@cache_control()
@cache.memoize()
def recalculate_consumption_min(value):
    energy_consumption_service = EnergyConsumptionServiceFactory.create()
    try:
        min_consumption = energy_consumption_service.get_actual_data(float(value))['min_consumption']
    except:
        min_consumption = 'mining is not profitable'
    return jsonify(min_consumption)


@app.route("/api/consumption/guess/<value>")
@cache_control()
@cache.memoize()
def recalculate_consumption_guess(value):
    energy_consumption_service = EnergyConsumptionServiceFactory.create()
    try:
        guess_consumption = energy_consumption_service.get_actual_data(float(value))['guess_consumption']
    except:
        guess_consumption = 'mining is not profitable'
    return jsonify(guess_consumption)


@app.route("/api/countries")
@cache_control()
@cache.memoize()
def countries_btc():
    energy_consumption_service = EnergyConsumptionServiceFactory.create()
    actual_guess_consumption = round(energy_consumption_service.get_actual_data(.05)['guess_consumption'], 2)
    tup2dict = {country: [consumption, flag, code] for country, code, consumption, flag in countries}
    tup2dict['Bitcoin'][0] = actual_guess_consumption
    dictsort = sorted(tup2dict.items(), key=lambda i: -1 if i[1][0] is None else i[1][0], reverse=True)

    response = []
    for item in dictsort:
        electricity_consumption = 0 if item[1][0] is None else item[1][0]
        response.append({
            'country': item[0],
            'code': item[1][2],
            'y': electricity_consumption,
            'x': dictsort.index(item)+1,
            'bitcoin_percentage': round(electricity_consumption / actual_guess_consumption * 100, 2),
            'logo': item[1][1]
        })
    for item in response:
        if item['country'] == "Bitcoin":
            item['color'] = "#ffb81c"
    return jsonify(response)


@app.route("/api/feedback", methods=['POST'])
def feedback():
    content = flask.request.json
    form = FeedbackForm(content)
    if not form.valid():
        return jsonify(errors=form.get_errors()), 422
    with psycopg2.connect(**config['custom_data']) as conn:
        c = conn.cursor()
        c.execute("CREATE TABLE IF NOT EXISTS feedback (timestamp INT PRIMARY KEY," 
                  "name TEXT, organisation TEXT, email TEXT, message TEXT);")
        insert_sql = "INSERT INTO feedback (timestamp, name, organisation, email, message) VALUES (%s, %s, %s, %s, %s)"
        name = content['name']
        content['organisation'] = content['organisation'] if content['organisation'] else None
        organisation = content['organisation']
        email = content['email']
        message = content['message']
        timestamp = int(time.time())
        try:
            c.execute(insert_sql, (timestamp, name, organisation, email, message))
        except Exception as error:
            return jsonify(data=content, status="fail", error=error.pgcode)
        finally:
            headers = {'Content-type': 'application/json', }
            sl_d = {
                "attachments": [
                    {
                        "fallback": "CBECI feedback received",
                        "color": "#36a64f",
                        "author_name": name,
                        "author_link": "mailto:" + email,
                        "title": organisation,
                        "text": message,
                        "footer": "cbeci.org",
                        "footer_icon": "https://i.ibb.co/HPhL1xy/favicon.png",
                        "ts": timestamp
                    }
                ]
            }

            date_for_ms = datetime.utcfromtimestamp(timestamp).isoformat()
            facts = []
            if organisation:
                facts.append({'name': 'Organisation:', 'value': organisation})
            facts.append({'name': 'Email:', 'value': email})
            ms_d = {
                "summary": "CBECI feedback received",
                "themeColor": "FFB81C",
                "title": "CBECI feedback received",
                "sections": [
                    {
                        "activityTitle": name,
                        "activitySubtitle": date_for_ms,
                        "activityImage": "https://i.ibb.co/0B6NSnK/user.jpg",
                        "facts": facts,
                        "text": message
                    }
                ]
            }

            sl_d = str(sl_d)
            ms_d = str(ms_d)
            flask.g.slackmsg = (sl_d, headers)
            flask.g.msmsg = (ms_d, headers)
    return jsonify(data=content, status="success", error="")


@app.teardown_request
def teardown_request(_: Exception):
    if hasattr(flask.g, 'slackmsg'):
        try:
            sl_d, headers = flask.g.slackmsg
            requests.post(config['webhook'], headers=headers, data=sl_d)
        except Exception as error:
            app.logger.exception(str(error))
    if hasattr(flask.g, 'msmsg'):
        try:
            ms_d, headers = flask.g.msmsg
            requests.post(config['webhook_ms'], headers=headers, data=ms_d)
        except Exception as error:
            app.logger.exception(str(error))


@app.route('/api/csv', methods=['GET'])
def download_report():
        with psycopg2.connect(**config['blockchain_data']) as conn:
            c = conn.cursor()
            c.execute('SELECT * FROM energy_consumption_ma WHERE timestamp >= %s', (start_date.timestamp(),))
        rows = c.fetchall()
        si = io.StringIO()
        cw = csv.writer(si)
        line = ['Timestamp', 'Date and Time', 'MAX', 'MIN', 'GUESS']
        cw.writerow(line)
        cw.writerows(rows)
        output = make_response(si.getvalue())
        output.headers["Content-Disposition"] = "attachment; filename=export.csv"
        output.headers["Content-type"] = "text/csv"
        return output


@app.route(SWAGGER_SPEC_URL)
def spec():
    swag = swagger(app)
    swag['info']['version'] = "1.0"
    swag['info']['title'] = "Cbeci API Specs"
    swag['info']['description'] = """
## Introduction
The [Cambridge Bitcoin Electricity Consumption Index (CBECI)](http://www.cbeci.org/) is an online tool that tracks the energy expenditure of the Bitcoin network to assess the environmental impact over time. Initially released in 2019, the CBECI is developed and maintained by the [Cambridge Centre for Alternative Finance (CCAF)](http://www.jbs.cam.ac.uk/ccaf) at The University of Cambridge Judge Business School. The tool is provided as a public good to help inform the Bitcoin energy debate through independent analysis based on empirical data.

This API enables participating mining pools to share geolocational data on their average hashrate distribution at both national and provincial levels. This data is then aggregated by the research team to provide regular updates to the CBECI mining map that depicts trends in Bitcoin’s geographical hashrate distribution. In order to use the API, please contact us [here](https://cbeci.org/contact/) to receive a pseudonymous token that enables a secure connection detached from your identity. The documentation below explains the data model in more detail. Please note that we currently target a biweekly frequency for data updates.
    """
    swag['securityDefinitions'] = {
        'Bearer': {
            'type': 'apiKey',
            'name': 'Authorization',
            'in': 'header',
            'description': """
    To authorize, please use the following request header: "Authorization"

    Example:
    "Authorization": "Bearer 1234567890",
    where 1234567890 - is your token
            """
        }
    }
    return jsonify(swag)


@app.route('/api/docs/spec/<path:path>')
def doc(path):
    print(path)
    return send_from_directory('docs', path)


if __name__ == '__main__':
    app.run(host='0.0.0.0', use_reloader=True)
