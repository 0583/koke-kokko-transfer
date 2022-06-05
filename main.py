import requests
from sys import argv
from flask import Flask, request


try:
    port = int(argv[-1])
    assert 0 <= port < 65536
except Exception:
    port = 8080

import koke_kokko_pb2

from VERSION import *

endpoint = "http://202.120.40.82:11233"
appName = KOKKO_APP_NAME
appID = KOKKO_APP_ID
app = Flask(__name__)


@app.route('/app', methods=['POST'])
def register_app():
    return requests.post(f"{endpoint}/app?appName={request.args['appName']}").json()


@app.route('/app', methods=['DELETE'])
def deregister_app():
    return requests.delete(f"{endpoint}/app?appName={request.args['appName']}&appID={request.args['appID']}").json()
