import json
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


@app.route('/schema', methods=['PUT'])
def upload_schema():
    return requests.put(f"{endpoint}/schema?appID={request.args['appID']}&fileName={request.args['fileName']}&version={request.args['version']}", data=request.data).text


@app.route('/schema', methods=['POST'])
def update_schema():
    return requests.post(f"{endpoint}/schema?appID={request.args['appID']}&version={request.args['version']}", data=request.data).text


entity_type_mapper = {
    'csdi.User': koke_kokko_pb2.User,
    'csdi.Article': koke_kokko_pb2.Article,
    'csdi.Tag': koke_kokko_pb2.Tag
}


def construct_entity(entity_type: type, attrs: dict):
    entity = entity_type()
    for k, v in attrs.items():
        if type(v) != list:
            entity.__setattr__(k, v)
        else:
            getattr(entity, k)[:] = v
    return entity


@app.route('/record', methods=['POST'])
def update_record():
    entity_type = entity_type_mapper[request.args['schemaName']]
    entity_data = construct_entity(
        entity_type, request.json).SerializeToString()
    response = requests.post(
        f"{endpoint}/record?appID={request.args['appID']}&schemaName={request.args['schemaName']}", data=entity_data)

    response['record_value'] = entity_type().ParseFromString(response.content)
    return response


@app.route('/record', methods=['DELETE'])
def delete_record():
    return requests.delete(
        f"{endpoint}/record?appID={request.args['appID']}&schemaName={request.args['schemaName']}&recordKey={request.args['recordKey']}").json()


@app.route('/record', methods=['GET'])
def get_record():
    entity_type = entity_type_mapper[request.args['schemaName']]
    if 'beginKey' in request.args:
        # range query
        response = requests.get(
            f"{endpoint}/record?appID={request.args['appID']}&schemaName={request.args['schemaName']}&beginKey={request.args['beginKey']}&endKey={request.args['endKey']}&iteration={request.args['iteration']}")
        return entity_type().ParseFromString(response.content).__dict__()
    else:
        # normal query
        response = requests.get(
            f"{endpoint}/record?appID={request.args['appID']}&schemaName={request.args['schemaName']}&recordKey={request.args['recordKey']}")
        return entity_type().ParseFromString(response.content).__dict__()


app.run('0.0.0.0', port, debug=True)
