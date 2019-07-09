# -*- coding: utf-8 -*-
"""
Created on Thu May 16 14:24:16 2019

@author: Anton
"""
from flask import Flask, jsonify
import pandas as pd
import flask
import requests
import time
from flask_cors import CORS
import psycopg2
import yaml

config_path = '../CONFIG.yml'
if config_path:
    with open(config_path) as fp:
        config = yaml.load(fp, yaml.FullLoader)
else:
    config = {}


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


app = Flask(__name__)
CORS(app)
app.config["DEBUG"] = False

# initialisation of cache vars:
prof_threshold, hash_rate, miners, countries, cons = load_data()
lastupdate = time.time()
hashrate = int(requests.get("https://blockchain.info/q/hashrate").json())
lastupdate_power = time.time()
cache = {}


def send_err_to_slack(err, name):
    try: 
        headers = {'Content-type': 'application/json',}
        data = {"text":""}
        data["text"] = f"Getting {name} failed. It unexpectedly returned: " + str(err)[0:140]
        requests.post(config['webhook_err'], headers=headers, data=str(data))
    except:
        pass


# cache:
@app.before_request
def before_request():
    global lastupdate, lastupdate_power, prof_threshold, hash_rate, miners, countries, cons, hashrate, cache
    if time.time() - lastupdate > 3600:
        try:
            prof_threshold, hash_rate, miners, countries, cons = load_data()
            cache = {}
            lastupdate = time.time()
        except Exception as err:
            app.logger.exception(f"Getting data from DB err: {str(err)}")
            send_err_to_slack(err,'DB')
    if time.time() - lastupdate_power > 45:
        try:
            # if executed properly, answer should be int
            hashrate = int(requests.get("https://blockchain.info/q/hashrate").json())
            lastupdate_power = time.time()
        except Exception:
            try:
                # in case it is not int, for example str saying us something:
                err = requests.get("https://blockchain.info/q/hashrate").json()
                app.logger.error(f"Fetching HASHRATE failed. It unexpectedly returned {str(err)[0:140]}")
                send_err_to_slack(err, 'HASHRATE')
            except Exception as err:
                # in all other cases, e.g. unreachable endpoint:
                app.logger.exception(str(err))
                send_err_to_slack(err, 'HASHRATE')
            
            
@app.route('/api/data/<value>')
def recalculate_data(value):
        
    price = float(value)

    if price in cache:
        return cache[price]

    k = 0.05/price 
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
    
    for i in range(0, len(prof_threshold)):
        for miner in miners:
            if prof_threshold[i][0]>miner[1] and prof_threshold[i][2]*k > miner[2]:
                prof_eqp.append(miner[2])
            # ^^current date miner release date ^^checks if miner is profit. ^^adds miner's efficiency to the list
        all_prof_eqp.append(prof_eqp)
        try:
            max_consumption = max(prof_eqp)*hash_rate[i][2]*365.25*24/1e9*1.2
            min_consumption = min(prof_eqp)*hash_rate[i][2]*365.25*24/1e9*1.01
            guess_consumption = sum(prof_eqp)/len(prof_eqp)*hash_rate[i][2]*365.25*24/1e9*1.1
        except:  # in case if mining is not profitable (it is impossible to find MIN or MAX of empty list)
            max_consumption = max_all[-1]
            min_consumption = min_all[-1]
            guess_consumption = guess_all[-1]
        max_all.append(max_consumption)
        min_all.append(min_consumption)
        guess_all.append(guess_consumption)
        timestamp = prof_threshold[i][0]
        ts_all.append(timestamp)
        date = prof_threshold[i][1]
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


@app.route("/api/countries", methods=['GET', 'POST'])
def countries_btc():
    tup2dict = {a:[c,d] for a,b,c,d in countries}
    tup2dict['Bitcoin'][0] = round(cons[-1][4],2)
    dictsort = sorted(tup2dict.items(), key = lambda i: i[1][0], reverse=True)
    response = []
    for item in dictsort:
         response.append({
            'country': item[0],
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
                        "fallback": "CBECI feedback recieved",
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
            sl_d = str(sl_d)
            flask.g.slackmsg = (sl_d, headers)
    return jsonify(data=content, status="success", error="")


@app.teardown_request
def teardown_request(_: Exception):
    if hasattr(flask.g, 'slackmsg'):
        try:
            sl_d, headers = flask.g.slackmsg
            requests.post(config['webhook'], headers=headers, data=sl_d)
        except Exception as error:
            app.logger.exception(str(error))


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
