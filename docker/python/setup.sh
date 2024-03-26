#!/bin/bash
set -eu
cd /var/app/python
poetry install
# uncomment following line if pm2.config.js file existing in python/src dir and contain instructions
# to run worker processes. Or replace with required bash terminal instructions
#pm2 start pm2.config.js
cd /var/app/python/api
poetry shell
flask run --host=0.0.0.0
