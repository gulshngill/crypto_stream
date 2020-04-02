import websocket
import json
import socket
import ssl
import argparse
import logging
import sys
import time
from google.cloud import pubsub

TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
TOPIC = 'cryptostream'
PARAMS = {"type": "subscribe", "product_ids": ["BTC-USD"],
"channels": [{"name": "ticker", "product_ids": ["BTC-USD"]}]}

try:
    import thread
except ImportError:
    import _thread as thread



def on_message(ws, message):
    logging.info('Publishing {}'.format(message))
    print(message)
    #publisher.publish(event_type, message.encode("utf-8")) #data must be encoded in bytestring


def on_error(ws, error):
    print(error)

def on_close(ws):
    print("### stream closed ###")

def on_open(ws):
    #send subscription params to server
    ws.send(json.dumps(PARAMS))

    def run(*args):
        print("run")
        #result = json.loads(ws.recv())


    thread.start_new_thread(run, ())


if __name__ == "__main__":
    #parse arguments from cmd line
    # parser = argparse.ArgumentParser(description='Send data to Cloud Pub/Sub')
    # parser.add_argument('--project', help='Example: --project $DEVSHELL_PROJECT_ID', required=True)
    # parser.add_argument('--topic', help='Example: --topic crypto', required=True)
    # args = parser.parse_args()


    # create Pub/Sub notification topic
    # logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
    # publisher = pubsub.PublisherClient()
    # event_type = publisher.topic_path(args.project,args.topic)
    # print(event_type)

    #check if topic exists in pubsub, else create new one (service account my require additional role to create topics)
    # try:
    #     publisher.get_topic(event_type)
    #     logging.info('Using pub/sub topic {}'.format(args.topic))
    # except:
    #     #publisher.create_topic(event_type)
    #     logging.info('Pub/sub topic {} does not exist'.format(args.topic))



    #enable websocket feed
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp("wss://ws-feed.pro.coinbase.com",
    on_message = on_message,
    on_error = on_error,
    on_close = on_close)

    ws.on_open = on_open

    ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE}) #no ssl cert
