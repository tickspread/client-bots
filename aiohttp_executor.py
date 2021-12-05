import asyncio
import aiohttp

async def execute_order(url, order, callback):
    async with aiohttp.ClientSession() as session:
        async with session.post(url, order) as response:
            response_text = await response.text()
            callback(response_text)

async def execute_order_on_session(session, url, order, callback):
    async with session.post(url, order) as response:
        response_text = await response.text()
        callback(response_text)


def execute_orders(url, orders, callback):
    asyncio.gather( *[execute_order(url, order, callback) for order in orders] )

