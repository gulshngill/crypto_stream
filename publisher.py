"""
Retrive BTC-USD,ETH-USD ticker data from Coinbase and publish to Cloud Pub/Sub

python publisher.py \
--project=PROJECT_NAME \
--topic=TOPIC_NAME
"""

import json
import ssl
import argparse
import logging
import websocket

from google.cloud import pubsub

TOPIC = 'cryptostream'
SUBSCRIBE_PARAMS = {"type": "subscribe",
                    "channels": [{"name": "ticker", "product_ids": ["BTC-USD", "ETH-USD"]}]}

futures = dict()


def get_callback(f, message):
    def callback(f):
        try:
            print(f.result())
            futures.pop(message)  # pop message from dutures dictionary
            logging.info(f'Published {message}')
        except:
            # TODO: does it publish log messages that fail but are successful after retry?
            logging.error(f'PUBLISH ERROR:{f.exception()}\nData: {message}')

    return callback


def on_message(ws, message):

    future = publisher.publish(event_type, message.encode("utf-8"))  # data must be encoded in bytestring
    futures[message] = future

    # Publish failures shall be handled in the callback function.
    future.add_done_callback(get_callback(future, message))


def on_error(ws, error):
    print(error)


def on_close(ws):
    print("### stream closed ###")


def on_open(ws):
    # send subscription params to server
    print("opened")
    ws.send(json.dumps(SUBSCRIBE_PARAMS))


if __name__ == "__main__":
    # parse arguments from cmd line
    parser = argparse.ArgumentParser(description='Send data to Cloud Pub/Sub')
    parser.add_argument('--project', help='Example: --project $DEVSHELL_PROJECT_ID', required=True)
    parser.add_argument('--topic', help='Example: --topic crypto', required=True)
    args = parser.parse_args()

    # create Pub/Sub notification topic
    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
    publisher = pubsub.PublisherClient()
    event_type = publisher.topic_path(args.project, args.topic)
    print(event_type)

    # check if topic exists in pubsub, else create new one (service account my require additional role to create topics)
    try:
        publisher.get_topic(event_type)
        logging.info(f'Using pub/sub topic {args.topic}')
    except:
        # publisher.create_topic(event_type)
        logging.info(f'Pub/sub topic {args.topic} does not exist')

    # enable websocket feed
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp("wss://ws-feed.pro.coinbase.com",
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)

    ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})  # no ssl cert
