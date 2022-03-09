import os
#os.environ['PYTHONASYNCIODEBUG'] = '1'

import requests
import json
import asyncio
import websockets
import logging

import time
import sys


MAX_RETRIES = 5

class TickSpreadAPI:
    def __init__(self, logger=logging.getLogger(), id_multiple=100, env="staging"):
        self.next_id = int(time.time()*id_multiple)
        self.logger = logger
        #self.host = 'api.tickspread.com'
        if env == "dev":
            self.http_host = 'http://localhost:4000'
            self.ws_host = 'ws://localhost:4000'
        elif env == "staging":
            self.http_host = "http://stag.api.tickspread.com"
            self.ws_host = "ws://stag.api.tickspread.com"
        else:
            self.http_host = 'https://api.tickspread.com'
            self.ws_host = 'wss://api.tickspread.com'
    
    def login(self, username, password):
        payload = {"username": username, "password_hash": password}
        url = '%s/v1/accounts/login' % self.http_host
        print(url, payload)
        
        try:
            r = requests.post(url, json=payload, timeout=5.0)
        except requests.exceptions.ReadTimeout:
            self.logger.error("Login timeout")
            return False
        except Exception as e:
            self.logger.error(e)
            return False
        
        try:
            data = json.loads(r.text)
            self.token = data["token"]
            self.callbacks = []
            self.websocket = None
        except:
            self.logger.error(r.text)
            return False
        
        return True
    
    def register(self, username, password):
        payload = {"type": "email_pass", "email": username, "password": password}
        url = '%s/v1/accounts' % self.http_host
        
        try:
            requests.post(url, json=payload, timeout=5.0)
        except requests.exceptions.ReadTimeout:
            self.logger.error("Register timeout")
        except Exception as e:
            self.logger.error(e)
            return False
        
        return True
    
    def get_next_clordid(self):
        return self.next_id
        
    def create_order_sync(self, client_order_id, amount, price, leverage, symbol, side, type):

        order = {"client_order_id": client_order_id, "amount": str(amount), "price": str(price), "leverage": str(leverage), "market": symbol, "side": side, "type": type}
        
        url = '%s/v2/orders' % self.http_host
        try:
            self.logger.info(order)
            r = requests.post(url, headers={"authorization": (
                "Bearer %s" % self.token)}, json=order, timeout=5.0)
        except Exception as e:
            print(e, flush=True)
            self.logger.error(e)
            logging.shutdown()
            sys.exit(1)
        
        try:
            text_json = json.loads(r.text)
            client_order_id = text_json["client_order_id"]
        except Exception as e:
            print(r.text)
            print(e, flush=True)
            self.logger.error(r.text[:500])
            logging.shutdown()
            sys.exit(1)
        
        return client_order_id

    def create_order(self, *, client_order_id=0, amount, price, leverage, symbol="ETH", side, type="limit", asynchronous=False):
        if (client_order_id == 0):
            client_order_id = self.next_id
            self.next_id += 1
        if (asynchronous==False):    
            return self.create_order_sync(client_order_id,amount,price,leverage,symbol,side,type)
        else:
            #print("%f: ASYNC NEW" % time.time())
            loop = asyncio.get_event_loop()
            loop.run_in_executor(
            None,
            self.create_order_sync,
            client_order_id,amount,price,leverage,symbol,side,type)
            
            #print("%f: ASYNC NEW END" % time.time())
            return "OK"

    def delete_order_sync(self, client_order_id, symbol="ETH"):
        url = '%s/v2/orders' % (self.http_host)
        counter = 0
        r = None
        order = {"client_order_id": client_order_id, "market": symbol}
        while counter < MAX_RETRIES:
            try:
                counter += 1
                r = requests.delete(url, headers={"authorization": ("Bearer %s" % self.token)}, json=order, timeout=5.0)
                json_response = json.loads(r.text)
                if (r.status_code == 200):
                    break
            except Exception as e:
                if (r): self.logger.error(r.text)
                else: self.logger.error(e)
                #logging.shutdown()
                #sys.exit(1)
        return json_response

    def delete_order(self, client_order_id, asynchronous=False):
        if (asynchronous==False):
            return self.delete_order_sync(client_order_id)
        else:
            loop = asyncio.get_event_loop()
            loop.run_in_executor(
            None,
            self.delete_order_sync,
            client_order_id)
            return "OK"
    
    async def connect(self):
        self.websocket = await websockets.connect("%s/realtime" % self.ws_host, ping_interval=None)
        asyncio.get_event_loop().create_task(self.loop(self.websocket))

    async def subscribe(self, topic, arguments):
        data = {
            "topic": topic,
            "event": "subscribe",
            "payload": arguments,
            "authorization": "Bearer %s" % self.token
        }
        
        try:
            await self.websocket.send(json.dumps(data))
        except Exception as e:
            self.logger.error(e)
            logging.shutdown()
            sys.exit(1)

    def on_message(self, callback):
        self.callbacks.append(callback)

    async def loop(self, websocket):
        rc = 0
        while rc == 0:
            try:
                print("wait")
                message = await websocket.recv()
                print("received")
            except Exception as e:
                self.logger.error(e)
                logging.shutdown()
                sys.exit(1)
            for callback in self.callbacks:
                rc = callback('tickspread', message)
        asyncio.get_event_loop().stop()  #exit the process

async def main():
    logging.basicConfig(level=logging.INFO, filename="test.log")
    
    api = TickSpreadAPI()
    print(api.register("test@tickspread.com", "tick"))
    login_status = api.login("matthewericfisher@yahoo.com", "tick")
    
    if (not login_status):
        print("Login failed")
        asyncio.get_event_loop().stop()
        return 1
    
    await api.connect()
    await api.subscribe("market_data", {"symbol": "ETH"})
    await api.subscribe("user_data", {"symbol": "ETH"})
    api.on_message(lambda source, data: logging.info(data))

if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        loop.create_task(main())
        loop.run_forever()
    except (Exception, KeyboardInterrupt) as e:
        print('ERROR', str(e))
        exit()
