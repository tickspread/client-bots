from decimal import Decimal
import sys
from time import sleep
import asyncio
import requests
import time

from python_loopring.loopring_v3_client import LoopringV3Client

{
    "market": "ETH-USDC",
    "baseTokenId": 0,
    "quoteTokenId": 6,
    "precisionForPrice": 2,
    "orderbookAggLevels": 2,
    "enabled": True
}

MAX_RETRIES = 5

class TickSpreadDex:
    def __init__(self, env="dev", id_multiple=100):
        self.next_id = int(time.time()*id_multiple)
        self.loopring_exported_account = {
            "name": "UAT Account 1",
            "chainId": 5,
            "exchangeName": "Loopring Exchange v2",
            "exchangeAddress": "0x2e76EBd1c7c0C8e7c2B875b6d505a260C525d25e",
            "accountAddress": "0xc82f080efCED38eF76646cf09Ee77513aA6fEBbB",
            "accountId": 10484,
            "apiKey": "ziuttGoJlmMWE7Zl9cvDbTB4HUUr70YfpOOem1H500TrOp92NNfULdYPkgPzRj1n",
            "publicKeyX": "0x138821284787c1fb72af5f434b0355b4252cd96e3bd5bff061f253e44956c5dc",
            "publicKeyY": "0x1d53c18acb36a71a18beb9332776e80104e72e062973891a820ca7b152b84114",
            "privateKey": "0x218dbda1a7f1576921e56a0bde0cd82d03de8fff4fe141dc54cc09a6e1410a5",
            "ecdsaKey": "3bf94b5932cf77e00fa2d0d8dcd37522a44897fba52c17aa46945a0739c924bc",
            "whitelisted": False
        }

        self.loopring_rest = LoopringV3Client()
        self.loopring_rest.connect(self.loopring_exported_account)\
        self.url = "http://stag.api.tickspread.com:4000"
        self.api_key = self.loopring_rest.api_key

    def get_next_clordid(self):
        return self.next_id

    def create_order(self, amount, price, leverage, symbol, side, asynchronous=True, client_order_id=0):
        symbol = "ETH-USDT"
        buy = True if side == "bid" else False
        if (client_order_id == 0):
            client_order_id = self.next_id
            self.next_id += 1
            print("client_order_id", client_order_id)
        if (asynchronous == False):
            return self.create_order_sync(client_order_id, amount, price, leverage, symbol, buy)
        else:
            # return self.create_order_sync(client_order_id, amount, price, leverage, symbol, buy)
            loop = asyncio.get_event_loop()
            loop.run_in_executor(
                None,
                self.create_order_sync,
                client_order_id, amount, price, leverage, symbol, buy)

            return "OK"

    def create_order_sync(self, client_order_id, amount, price, leverage, symbol, buy):
        if symbol == "ETH-USDT":
            asset1 = "ETH"
            asset2 = "USDT"

        print("%f: ORDER NEW" % time.time())
        order = self.loopring_rest.mount_send_order(
            client_order_id, asset1, asset2, buy, price, amount)

        r = requests.post("%s/dexapi/loopring/v2/orders" % self.url,
                          json=order,
                          timeout=5.0,
                          headers={
                              "Content-Type": "application/json",
                              "Accept": "application/json",
                              "X-API-KEY": self.api_key,
                          })
        print("%f: ORDER NEW END" % time.time())

    def delete_order(self, client_order_id, symbol="ETH-USDT", asynchronous=True):
        if (asynchronous == False):
            return self.delete_order_sync(client_order_id, symbol)
        else:
            #print("%f: ASYNC NEW" % time.time())
            loop = asyncio.get_event_loop()
            loop.run_in_executor(
                None,
                self.delete_order_sync,
                client_order_id, symbol)

            #print("%f: ASYNC NEW END" % time.time())
            return "OK"

    def delete_order_sync(self, client_order_id, symbol):
        if symbol == "ETH-USDT":
            asset1 = "ETH"
            asset2 = "USDT"
        delete_params = self.loopring_rest.mount_cancel_order(clientOrderId=client_order_id)
        
        signature = self.loopring_rest.mount_cancel_signature(clientOrderId=client_order_id)

        counter = 0
        r = None
        order = {"client_order_id": client_order_id, "market": symbol}
        while counter < MAX_RETRIES:
            try:
                counter += 1
                r = requests.delete("%s/dexapi/loopring/v2/orders" % self.url,
                                    timeout=5.0,
                                    params=delete_params,
                                    headers={
                                        "Content-Type": "application/json",
                                        "Accept": "application/json",
                                        "X-API-KEY": self.api_key,
                                        "X-API-SIG": signature
                                    })
                json_response = json.loads(r.text)
                if (r.status_code == 200):
                    break
            except Exception as e:
                pass
        return json_response


# tickspread = TickSpreadDex()
# tickspread.create_order_sync(
#             clientOrderId, 1.0, 1, 1, "ETH-USDT", False)
# tickspread.delete_order_sync(clientOrderId, "ETH-USDT")
