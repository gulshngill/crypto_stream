import websocket
import json
import socket
import ssl

#subscription parameters
#TODO: update params
params = {"type": "subscribe", "product_ids": ["BTC-USD"],
"channels": [{"name": "ticker", "product_ids": ["BTC-USD"]}]}

try:
    import thread
except ImportError:
    import _thread as thread
import time

def on_message(ws, message):
    print(message)

def on_error(ws, error):
    print(error)

def on_close(ws):
    print("### closed ###")

def on_open(ws):

    #send subscription params to server
    ws.send(json.dumps(params))

    def run(*args):

        #TODO: forward data to pub/sub

        #response from server
        result = json.loads(ws.recv())
        print(result)

    thread.start_new_thread(run, ())



if __name__ == "__main__":
  websocket.enableTrace(True)
  ws = websocket.WebSocketApp("wss://ws-feed.pro.coinbase.com",
   on_message = on_message,
   on_error = on_error,
   on_close = on_close)

  ws.on_open = on_open

  ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})
  #ws.run_forever()
