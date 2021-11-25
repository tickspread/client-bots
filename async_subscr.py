
import json
import asyncio
import websockets
from functools import partial

async def call_subscription(subscrMessage, subscrUrl):
    async with websockets.connect(subscrUrl):
        await websockets.send(subscrMessage) 
        while True:
            response = await websockets.recv()
            yield response
    

async def gather_all(params):
    gather_funcs = [ call_subscription(**p) for p in params ] 
    await asyncio.gather( *gather_funcs )

async def main(subscrMessage, subscrUrl):
    subscr_gen = call_subscription(subscrMessage, subscrUrl)
    async for resp in subscr_gen:
        print( resp )


if __name__ == "__main__":
    params = [
               {
                   'subscrMessage' : "{'op': 'subscribe', 'channel': 'trades', 'market': 'ETH-PERP'}",
                   'subscrUrl' : "wss://ftx.com/ws/",
               },
               {
                   'subscrMessage' : "{'op': 'subscribe', 'channel': 'trades', 'market': 'BTC-PERP'}",
                   'subscrUrl' : "wss://ftx.com/ws/",               
                },
             ]



    asyncio.run( main(**params[0]) )

