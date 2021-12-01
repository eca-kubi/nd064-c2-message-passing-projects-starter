import app.udaconnect.services as s
import flask.json as json
from kafka import KafkaConsumer
from app import create_app

import logging

logging.basicConfig(level=logging.DEBUG)

app = create_app()
app.app_context().push()

if __name__ == "__main__":

    TOPIC_NAME = 'persons'
    consumer = KafkaConsumer(TOPIC_NAME)
    for message in consumer:
        # decode message and write to postgres db
        print('inside consumer')
        print(message)

        person = json.loads(message.value)
        print(person)
        s.PersonService.create(person)
