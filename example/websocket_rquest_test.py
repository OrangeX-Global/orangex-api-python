import asyncio
import os
import unittest

from orangex_api.websocket_async import WebsocketAsync

ws_async = WebsocketAsync(client_id='28e91afa', client_secret='***************')


class OrangexTest(unittest.TestCase):

    async def sub_private_trade(self):
        symbols = ['BNB-USDT-PERPETUAL']
        asyncio.create_task(ws_async.subscribe_private_trades(symbols))
        await asyncio.sleep(100)

    def test_private_trade(self):
        asyncio.run(self.sub_private_trade())

    async def sub_depth(self):
        symbols = ['BTC-USDT-PERPETUAL']
        asyncio.create_task(ws_async.subscribe_depth(symbols))
        await asyncio.sleep(100)

    def test_sub_depth(self):
        asyncio.run(self.sub_private_trade())


if __name__ == '__main__':
    test_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)))

    discover = unittest.defaultTestLoader.discover(test_dir, pattern="test*.py", top_level_dir=None)
    runner = unittest.TextTestRunner()
    runner.run(discover)
