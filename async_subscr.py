
import json
import asyncio
import websockets
from functools import partial

async def call_subscription(subscrMessage, subscrUrl):
    async with websockets.connect(subscrUrl, compression=None) as ws:
        await ws.send(json.dumps(subscrMessage)) 
        while True:
            yield (response := await ws.recv())
    
async def gather_all(params):
    gather_funcs = [ call_subscription(**p) for p in params ] 
    await asyncio.gather( *gather_funcs )

async def subscr1(subscrMessage, subscrUrl):
    subscr_gen = call_subscription(subscrMessage, subscrUrl)
    print( "using next:")
    x = await subscr_gen.__anext__()
    print(x)
    async for resp in subscr_gen:
        print ("using for")
        print( resp )
        break


if __name__ == "__main__":
    params = [
               {
                   'subscrMessage' : {'op': 'subscribe', 'channel': 'trades', 'market': 'ETH-PERP'},
                   'subscrUrl' : "wss://ftx.com/ws/",
               },
               {
                   'subscrMessage' : {'op': 'subscribe', 'channel': 'trades', 'market': 'BTC-PERP'},
                   'subscrUrl' : "wss://ftx.com/ws/",               
                },
             ]



    asyncio.run( subscr1(**params[0]) )

