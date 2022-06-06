from base64 import b64encode
import json
from uuid import uuid4
import requests
from sys import argv
from flask import Flask, request, send_file


try:
    port = int(argv[-1])
    assert 0 <= port < 65536
except Exception:
    port = 8080

import koke_kokko_pb2
from google.protobuf.json_format import MessageToDict

from VERSION import *

endpoint = "http://202.120.40.82:11233"
app = Flask(__name__)


@app.route('/image', methods=['POST'])
def post_image():
    unique_id = str(uuid4())
    request.files['image'].save('images/' + unique_id)
    return {
        'status': 'ok',
        'uuid': unique_id
    }


@app.route('/image', methods=['GET'])
def get_image():
    return send_file('images/' + request.args['uuid'])


@app.route('/app', methods=['POST'])
def register_app():
    return requests.post(f"{endpoint}/app?appName={request.args['appName']}").json()


@app.route('/app', methods=['DELETE'])
def deregister_app():
    return requests.delete(f"{endpoint}/app?appName={request.args['appName']}&appID={request.args['appID']}", headers=request.headers).json()


@app.route('/schema', methods=['PUT'])
def upload_schema():
    print(request.data)
    return requests.put(
        f"{endpoint}/schema?appID={request.args['appID']}&fileName={request.args['fileName']}&version={request.args['version']}", headers={
            'Content-Type': 'text-plain'
        }, data=request.data).text


@app.route('/schema', methods=['POST'])
def update_schema():
    return requests.post(
        f"{endpoint}/schema?appID={request.args['appID']}&version={request.args['version']}").text


entity_type_mapper = {
    'csdi.User': koke_kokko_pb2.User,
    'csdi.Article': koke_kokko_pb2.Article,
    'csdi.Tag': koke_kokko_pb2.Tag
}


def deserialize_entity(entity_type: type, attrs: dict):
    entity = entity_type()
    for k, v in attrs.items():
        if type(v) != list:
            entity.__setattr__(k, v)
        else:
            getattr(entity, k)[:] = v
    return entity


def serialize_entity(entity) -> dict:
    return MessageToDict(entity, including_default_value_fields=True, preserving_proto_field_name=True)


@app.route('/record', methods=['POST'])
def update_record():
    entity_type = entity_type_mapper[request.args['schemaName']]
    entity_data = deserialize_entity(
        entity_type, request.json).SerializeToString()
    print(entity_data)
    ret = requests.post(
        f"{endpoint}/record?appID={request.args['appID']}&schemaName={request.args['schemaName']}", data=entity_data, headers={
            'Content-Type': 'octet-stream'
        }).text

    print(ret)
    return ret


@app.route('/record', methods=['DELETE'])
def delete_record():
    return requests.delete(
        f"{endpoint}/record?appID={request.args['appID']}&schemaName={request.args['schemaName']}&recordKey={request.args['recordKey']}").json()


@app.route('/query', methods=['GET'])
def get_record():
    entity_type = entity_type_mapper[request.args['schemaName']]
    if 'beginKey' in request.args:
        # range query
        response = requests.get(
            f"{endpoint}/query?range=true&appID={request.args['appID']}&schemaName={request.args['schemaName']}&beginKey={request.args['beginKey']}&endKey={request.args['endKey']}&iteration={request.args['iteration']}")
        print(response.content)

        bts = bytearray(response.content)
        more = bts.pop(0)

        entities = []
        while bts:
            size = int.from_bytes(bts[:2], byteorder='big')
            entity = entity_type()
            entity.ParseFromString(bts[2:2 + size])
            entities.append(entity)
            bts = bts[2 + size:]

        return {
            'more': more,
            'entities': [serialize_entity(e) for e in entities]
        }
    else:
        # normal query
        response = requests.get(
            f"{endpoint}/query?appID={request.args['appID']}&schemaName={request.args['schemaName']}&recordKey={request.args['recordKey']}")
        print(response.content)
        entity = entity_type()
        entity.ParseFromString(response.content)
        return serialize_entity(entity)


app.run('0.0.0.0', port, debug=True)
