import flask.json
from typing import Dict
from app import g
from flask import json


def send(person: Dict):
    data = json.dumps(person).encode()
    g.producer.send("persons", data)
    #g.producer.flush()
    print('producer complete')
