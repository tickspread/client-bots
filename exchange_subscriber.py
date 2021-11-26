from collections import ABC, abstractmethod
import json
import asyncio
import websockets
import logging

class WebsocketExchangeMonitor(ABC):

    @abstractmethod
    def __init__(self, *args):
        ...

    @abstractmethod
    def subscription_url(self, marketUrl):
        """
            for example, "wss://www.bitmex.com/realtime?subscribe=trade:XBTUSD"
        """

    @abstractmethod
    def subscription_data(self):
        """
            for example, {'op': 'subscribe', 'channel': topic, 'market': 'ETH-PERP'}
        """

    async def connect(self):
        self.websocket = await websockets.connect(self.subscription_url())
        asyncio.get_event_loop().create_task(self.loop(self.websocket))

    async def loop(self, websocket):
        while True:
            message = await websocket.recv()






class FTXAPI:
    def __init__(self, logger=logging.getLogger()):
        self.host = "wss://www.bitmex.com/realtime?subscribe=trade:XBTUSD"
        self.logger = logger
        self.callbacks = []

    async def connect(self):
        self.websocket = await websockets.connect(self.host)
        asyncio.get_event_loop().create_task(self.loop(self.websocket))

    async def subscribe(self, topic):
        data = {'op': 'subscribe', 'channel': topic, 'market': 'ETH-PERP'}
        await self.websocket.send(json.dumps(data))

    def on_message(self, callback):
        self.callbacks.append(callback)

    async def loop(self, websocket):
        while True:
            message = await websocket.recv()
            #message = json.loads(message)
            for callback in self.callbacks:
                callback("bitmex", message)