# -*- coding: utf-8 -*-
"""
Created on Thu May 16 14:24:16 2019

@author: Anton
"""
from flask import Flask, jsonify, make_response, request, has_request_context
from flask_cors import CORS
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from logging.handlers import RotatingFileHandler
from schema import SchemaError
import pandas as pd
from datetime import datetime
import flask
import requests
import logging
import time
import psycopg2
import csv
import io
import os
from dotenv import load_dotenv
from config import config
from decorators.auth import AuthenticationError
from extensions import cache

load_dotenv(override=True)

LOG_LEVEL = logging.INFO

def get_limiter_flag():
    val = os.environ.get("LIMITER_ENABLED")

    return val is not None and val.lower() not in ("0", "false", "no")

# loading data in cache of each worker:
def load_data():
    with psycopg2.connect(**config['blockchain_data']) as conn:
        c = conn.cursor()
        c.execute('SELECT * FROM prof_threshold')
        prof_threshold = c.fetchall()
        prof_threshold = prof_threshold[500:]
        c.execute('SELECT * FROM hash_rate')
        hash_rate = c.fetchall()
        hash_rate = hash_rate[500:]
        c.execute('SELECT * FROM energy_consumption_ma')
        cons = c.fetchall()
        cons = cons[500:]
    with psycopg2.connect(**config['custom_data']) as conn2:   
        c2 = conn2.cursor()
        c2.execute('SELECT * FROM miners')
        miners=c2.fetchall()
        c2.execute('SELECT * FROM countries')
        countries=c2.fetchall()
    return prof_threshold, hash_rate, miners, countries, cons


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

    from blueprints import charts, contribute
    app.register_blueprint(charts.bp, url_prefix='/api/charts')
    app.register_blueprint(contribute.bp, url_prefix='/api/contribute')

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

# initialisation of cache vars:
prof_threshold, hash_rate, miners, countries, cons = load_data()
lastupdate = time.time()
lastupdate_power = time.time()
cache = {}
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

# cache:
@app.before_request
def before_request():
    global lastupdate, lastupdate_power, prof_threshold, hash_rate, miners, countries, cons, hashrate, cache
    if time.time() - lastupdate > 3600:
        try:
            prof_threshold, hash_rate, miners, countries, cons = load_data()
        except Exception as err:
            app.logger.exception(f"Getting data from DB err: {str(err)}")
            send_err_to_slack(err, 'DB')
        else:
            cache = {}
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


@app.route('/api/data')
@app.route('/api/data/<value>')
def recalculate_data(value=None):
    try:
        if value is None:
            value = request.args.get('p')
        price = float(value)
    except:
        return "Welcome to the CBECI API data endpoint. To get bitcoin electricity consumption estimate timeseries, specify electricity price parameter 'p' (in USD), for example /api/data?p=0.05"


    if price in cache:
        return cache[price]

    k = 0.05 / price
    # that is because base calculation in the DB is for the price 0.05 USD/KWth
    # temporary vars:
    prof_eqp = []
    all_prof_eqp = []
    max_all = []
    min_all = []
    ts_all = []
    date_all = []
    guess_all = []
    response = []

    prof_th = pd.DataFrame(prof_threshold)
    prof_th = prof_th.drop(1, axis=1).set_index(0)
    prof_th_ma = prof_th.rolling(window=14, min_periods=1).mean()

    hashra = pd.DataFrame(hash_rate)
    hashra = hashra.drop(1, axis=1).set_index(0)

    for timestamp, row in prof_th_ma.iterrows():
        for miner in miners:
            if timestamp > miner[1] and row[2] * k > miner[2]:
                prof_eqp.append(miner[2])
            # ^^current date miner release date ^^checks if miner is profit. ^^adds miner's efficiency to the list
        all_prof_eqp.append(prof_eqp)
        try:
            max_consumption = max(prof_eqp) * hashra[2][timestamp] * 365.25 * 24 / 1e9 * 1.2
            min_consumption = min(prof_eqp) * hashra[2][timestamp] * 365.25 * 24 / 1e9 * 1.01
            guess_consumption = sum(prof_eqp) / len(prof_eqp) * hashra[2][timestamp] * 365.25 * 24 / 1e9 * 1.1
        except:  # in case if mining is not profitable (it is impossible to find MIN or MAX of empty list)
            max_consumption = max_all[-1]
            min_consumption = min_all[-1]
            guess_consumption = guess_all[-1]
        max_all.append(max_consumption)
        min_all.append(min_consumption)
        guess_all.append(guess_consumption)
        ts_all.append(timestamp)
        date = datetime.utcfromtimestamp(timestamp).isoformat()
        date_all.append(date)
        prof_eqp = []

    energy_df = pd.DataFrame(list(zip(max_all, min_all, guess_all)), index=ts_all, columns=['MAX', 'MIN', 'GUESS'])
    energy_ma = energy_df.rolling(window=7, min_periods=1).mean()
    max_ma = list(energy_ma['MAX'])
    min_ma = list(energy_ma['MIN'])
    guess_ma = list(energy_ma['GUESS'])

    for day in range(0, len(ts_all)):
        response.append({
            'date': date_all[day],
            'guess_consumption': round(guess_ma[day], 2),
            'max_consumption': round(max_ma[day], 2),
            'min_consumption': round(min_ma[day], 2),
            'timestamp': ts_all[day],
        })

    value = jsonify(data=response)
    cache[price] = value
    return value


@app.route("/api/max/<value>")
def recalculate_max(value):
    price = float(value)
    k = 0.05/price  # that is because base calculations in the DB is for the price 0.05 USD/KWth
    prof_eqp = []   # temp var for the list of profitable equipment efficiency at any given moment
    for miner in miners:
        if prof_threshold[-1][0]>miner[1] and prof_threshold[-1][2]*k>miner[2]: prof_eqp.append(miner[2])
        # ^^current date miner release date ^^checks if miner is profit. ^^if yes, adds miner's efficiency to the list
    try:
        max_consumption = max(prof_eqp)*hashrate*1.2/1e9
    except:
        max_consumption = 'mining is not profitable'
    return jsonify(max_consumption)


@app.route("/api/min/<value>")
def recalculate_min(value):
    price = float(value)
    k = 0.05/price  # that is because base calculations in the DB is for the price 0.05 USD/KWth
    prof_eqp = []  # temp var for the list of profitable equipment efficiency at any given moment
    for miner in miners:
        if prof_threshold[-1][0]>miner[1] and prof_threshold[-1][2]*k>miner[2]: prof_eqp.append(miner[2])
        # ^^current date miner release date ^^checks if miner is profit. ^^if yes, adds miner's efficiency to the list
    try:
        min_consumption = min(prof_eqp)*hashrate*1.01/1e9
    except:
        min_consumption = 'mining is not profitable'
    return jsonify(min_consumption)


@app.route("/api/guess/<value>")
def recalculate_guess(value):
    price = float(value)
    k = 0.05/price  # that is because base calculations in the DB is for the price 0.05 USD/KWth
    prof_eqp = []   # temp var for the list of profitable equipment efficiency at any given moment

    for miner in miners:
        if prof_threshold[-1][0]>miner[1] and prof_threshold[-1][2]*k>miner[2]: prof_eqp.append(miner[2])
        # ^^current date miner release date ^^checks if miner is profit. ^^if yes, adds miner's efficiency to the list
    try:
        guess_consumption = sum(prof_eqp)/len(prof_eqp)*hashrate*1.10/1e9
    except:
        guess_consumption = 'mining is not profitable'
    return jsonify(guess_consumption)
# =============================================================================
# @app.route("/api/countries", methods=['GET','POST'])
# def countries_old():
#     jsonify(data=countries)
# =============================================================================

@app.route("/api/countries")
def countries_btc():
    tup2dict = {a:[c,d,b] for a,b,c,d,e,f in countries}
    tup2dict['Bitcoin'][0] = round(cons[-1][4],2)
    dictsort = sorted(tup2dict.items(), key = lambda i: i[1][0], reverse=True)
    response = []
    for item in dictsort:
         response.append({
            'country': item[0],
            'code': item[1][2],
            'y': item[1][0],        
            'x': dictsort.index(item)+1,
            'bitcoin_percentage': round(item[1][0]/round(cons[-1][4], 2)*100, 2),
            'logo': item[1][1]
            })
    for item in response:
        if item['country'] == "Bitcoin":
            item['color'] = "#ffb81c"
    return jsonify(response)


@app.route("/api/feedback", methods=['POST'])
def feedback():
    content = flask.request.json
    with psycopg2.connect(**config['custom_data']) as conn:
        c = conn.cursor()
        c.execute("CREATE TABLE IF NOT EXISTS feedback (timestamp INT PRIMARY KEY," 
                  "name TEXT, organisation TEXT, email TEXT, message TEXT);")
        insert_sql = "INSERT INTO feedback (timestamp, name, organisation, email, message) VALUES (%s, %s, %s, %s, %s)"
        name = content['name']
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
            ms_d = {
                "summary": "CBECI feedback received",
                "themeColor": "FFB81C",
                "title": "CBECI feedback received",
                "sections": [
                    {
                        "activityTitle": name,
                        "activitySubtitle": date_for_ms,
                        "activityImage": "https://i.ibb.co/0B6NSnK/user.jpg",
                        "facts": [
                            {
                                "name": "Organisation:",
                                "value": organisation
                            },
                            {
                                "name": "Email:",
                                "value": email
                            }
                        ],
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
            c.execute('SELECT * FROM energy_consumption_ma')
        rows = c.fetchall()
        rows = rows[500:]
        si = io.StringIO()
        cw = csv.writer(si)
        line = ['Timestamp', 'Date and Time', 'MAX', 'MIN', 'GUESS']
        cw.writerow(line)
        cw.writerows(rows)
        output = make_response(si.getvalue())
        output.headers["Content-Disposition"] = "attachment; filename=export.csv"
        output.headers["Content-type"] = "text/csv"
        return output
# =============================================================================
# # ====== test endpoints ahead =========================================
# @app.route('/api/new/data/<value>')
# def recalculate_data_new(value):
#     price = float(value)
# 
# #    if price in cache:
# #        return cache[price]
# 
#     k = 0.05 / price
#     # that is because base calculation in the DB is for the price 0.05 USD/KWth
#     # temporary vars:
#     prof_eqp = []
#     all_prof_eqp = []
#     max_all = []
#     min_all = []
#     ts_all = []
#     date_all = []
#     guess_all = []
#     response = []
# 
#     for i in range(0, len(prof_threshold)):
#         for miner in miners:
#             if prof_threshold[i][0] > miner[1] and prof_threshold[i][2] * k > miner[2]:
#                 prof_eqp.append(miner[2])
#             # ^^current date miner release date ^^checks if miner is profit. ^^adds miner's efficiency to the list
#         all_prof_eqp.append(prof_eqp)
#         try:
#             max_consumption = max(prof_eqp) * hash_rate[i][2] * 365.25 * 24 / 1e9 * 1.2
#             min_consumption = min(prof_eqp) * hash_rate[i][2] * 365.25 * 24 / 1e9 * 1.01
#             guess_consumption = sum(prof_eqp) / len(prof_eqp) * hash_rate[i][2] * 365.25 * 24 / 1e9 * 1.1
#         except:  # in case if mining is not profitable (it is impossible to find MIN or MAX of empty list)
#             max_consumption = max_all[-1]
#             min_consumption = min_all[-1]
#             guess_consumption = guess_all[-1]
#         max_all.append(max_consumption)
#         min_all.append(min_consumption)
#         guess_all.append(guess_consumption)
#         timestamp = prof_threshold[i][0]
#         ts_all.append(timestamp)
#         date = prof_threshold[i][1]
#         date_all.append(date)
#         prof_eqp = []
# 
#     energy_df = pd.DataFrame(list(zip(max_all, min_all, guess_all)), index=ts_all, columns=['MAX', 'MIN', 'GUESS'])
#     energy_ma = energy_df.rolling(window=7, min_periods=1).mean()
#     max_ma = list(energy_ma['MAX'])
#     min_ma = list(energy_ma['MIN'])
#     guess_ma = list(energy_ma['GUESS'])
# 
#     for day in range(0, len(ts_all)):
#         response.append({
#             'g': round(guess_ma[day], 2),
#             'x': round(max_ma[day], 2),
#             'n': round(min_ma[day], 2),
#             't': ts_all[day],
#         })
# 
# #    value = jsonify(data=response)
# #    cache[price] = value
# #    return value
#     return jsonify(data=response)
# 
# 
# @app.route("/api/new/countries", methods=['GET', 'POST'])
# def countries_btc_new():
#     tup2dict = {a: [c, d, b] for a, b, c, d in countries}
#     tup2dict['Bitcoin'][0] = round(cons[-1][4],2)
#     dictsort = sorted(tup2dict.items(), key=lambda i: i[1][0], reverse=True)
#     response = []
#     for item in dictsort:
#          response.append({
#             'c': item[0],
#             'y': item[1][0],
#             'x': dictsort.index(item)+1,
#             'p': round(item[1][0]/round(cons[-1][4], 2)*100, 2),
#             'l': item[1][2]
#             })
#     for item in response:
#         if item['c'] == "Bitcoin":
#             item['color'] = "#ffb81c"
#     return jsonify(response)
# 
# =============================================================================

if __name__ == '__main__':
    app.run(host='0.0.0.0', use_reloader=True)
