import asyncio
import json
import logging
import traceback
from asyncio import Queue

import websockets
from sortedcontainers import SortedDict

from orangex_api.call import Call
from orangex_api.urls import Urls


class WebsocketClient(object):
    def __init__(self, client_id: str, client_secret: str):
        self.client_key = client_id
        self.client_secret = client_secret
        self._call = Call(client_id, client_secret)
        self.ws = None
        self.depths = dict()
        self.__queues = dict()
        self.__full_data_needs = dict()
        self.__id = 1

    async def auth(self, wait_time=0, websocket_connection=None):
        assert self.client_key and self.client_secret, 'please set client_id and client_secret'
        if wait_time > 0:
            await asyncio.sleep(wait_time)
        m = {
            "jsonrpc": "2.0",
            "id": str(self.__id),
            "method": "/public/auth",
            "params": {
                "grant_type": "client_credentials",
                "client_id": self.client_key,
                "client_secret": self.client_secret,
            }
        }
        self.__id += 1
        await websocket_connection.send(json.dumps(m))

    async def subscribe(self, method: str, topics: list):
        while self.ws is None:
            await asyncio.sleep(0.5)
        if self.ws is not None:
            m = {
                "jsonrpc": "2.0",
                "id": self.__id,
                "method": method,
                "params": {
                    "channels": topics,
                }
            }
            self.__id += 1
            await self.send(json.dumps(m))

    async def run_subscribe(self):
        while True:
            try:
                await self.receive_ws_message()
            except Exception:
                logging.error(traceback.format_exc())
                await asyncio.sleep(5)

    async def receive_ws_message(self):
        logging.info('start call receive_private_ws_message')
        async with websockets.connect(Urls.ws_base) as websocket:
            await self.auth(wait_time=2, websocket_connection=websocket)
            asyncio.create_task(self.ping())
            self.ws = websocket
            while True:
                response = await self.ws.recv()
                await self.handle(response)

    async def handle(self, message):
        if message == 'PONG':
            return
        m = json.loads(message)
        method = m.get('method')
        if method != 'subscription':
            return
        channel = m.get('params', {}).get('channel', '')
        if channel[:4] == 'book':
            # Just for reference how to construct an order book
            await self.build_order_book(m.get('params', {}))
        pass

    """ How to build an order book
    
    1. Open a WebSocket connection to  wss://api.orangex.com/ws/api/v1, 
       then subscribe to the OrderBook data stream through the subscription channel book.{instrumentName}.raw, 
       buffer the events received from the stream. Note the change_id of the first event you received, mark this as fisrtChangeId.
    
    2. Get the orderbook snapshot by http endpoint /public/get_order_book.  
       If the version in the snapshot is strictly less than the fisrtChangeId - 1, retry taking the snapshot until version is greater than or equal to fisrtChangeId -1, mark the version field in the last snapshot data obtained as lastVersion.
    
    3. Set your local order book to the snapshot. Its update ID is lastVersion. In the buffered events, discard any event where change_id is <= lastVersion of the snapshot. 
    
    4.  Apply the update procedure below to all buffered events, and then to all subsequent events received.
       1. If the event change_id is < lastVersion of your local order book, ignore the event.
       2. If the event change_id > lastVersion + 1 of your local order book, something went wrong. Discard your local order book and restart the process from the beginning.
       3. For each price level in bids and asks, set the new quantity in the order book:
          * If the price level does not exist in the order book, insert it with new quantity.
          * If the quantity is zero, remove the price level from the order book.
       4. Set the order book lastVersion to the change_id in the processed event.
    """

    async def build_order_book(self, params):
        """Building an order book"""
        data = params.get('data', {})
        if not data:
            return
        symbol = data.get('instrument_name')
        if not self.__queues.get(symbol):
            self.__queues[symbol] = Queue(1000)
        full_data_need = self.__full_data_needs.get(symbol)
        if not full_data_need:
            self.__full_data_needs[symbol] = True
            asyncio.create_task(self.start_build_depth(symbol))
        await self.produce_depth(symbol, data)

    @staticmethod
    def reverse_order(p):
        return -float(p)

    @staticmethod
    def nature_order(p):
        return float(p)

    def init_symbol_depth(self, symbol):
        self.depths[symbol] = dict()
        self.depths[symbol]['asks'] = SortedDict(WebsocketClient.nature_order)
        self.depths[symbol]['bids'] = SortedDict(WebsocketClient.reverse_order)

    def __update_depth(self, symbol, asks, bids, ts):
        self.update_depth(symbol, asks, 'asks')
        self.update_depth(symbol, bids, 'bids')
        self.depths[symbol]['ts'] = ts

    def update_depth(self, instrument_symbol, depth: list, side):
        if instrument_symbol not in self.depths:
            logging.info(f'****************{instrument_symbol}尚未收到初始消息************')
            return
        current_depth: SortedDict = self.depths[instrument_symbol].get(side)
        for a in depth:
            if len(a) == 2:
                current_depth[float(a[0])] = float(a[1])
            else:
                if a[0] == 'delete':
                    current_depth.pop(float(a[1]), 0)
                else:
                    current_depth[float(a[1])] = float(a[2])
        return current_depth

    async def start_build_depth(self, symbol):
        try:
            self.init_symbol_depth(symbol)
            q: Queue = self.__queues[symbol]
            full = await self.depth(symbol)
            full_version = full.get('version')
            self.__update_depth(symbol, full.get('asks', []), full.get('bids', []), full['timestamp'])
            last_version = full_version
            first_version = None
            while True:
                event = await q.get()
                q.task_done()
                change_id = event.get('change_id')
                if first_version is None:
                    first_version = change_id
                if first_version > full_version:
                    while True:
                        full = await self.depth(symbol)
                        full_version = full.get('version')
                        if full_version >= first_version:
                            last_version = full_version
                            break
                if change_id <= full_version:
                    continue
                if last_version != 0 and last_version + 1 != change_id:
                    break
                last_version = change_id
                asks, bids = event.get('asks', []), event.get('bids', [])
                self.__update_depth(symbol, asks, bids, event.get('timestamp'))
                current_depth = self.depths.get(symbol)
                print(
                    f"Updated depth for {symbol}, asks:{current_depth['asks'].items()[0:5]}, bids:{current_depth['bids'].items()[0:5]}")
        except Exception:
            print(traceback.format_exc())
        await asyncio.sleep(0.1)
        self.__full_data_needs[symbol] = False

    async def produce_depth(self, ch, data: dict):
        q: Queue = self.__queues[ch]
        await q.put(data)

    async def ping(self):
        while True:
            await self.send('PING')
            await asyncio.sleep(5)

    async def send(self, message):
        await self.ws.send(message)

    async def depth(self, symbol):
        url = Urls.depth
        params = dict(instrument_name=symbol, depth=20)
        res = await self._call.get(url=url, params=params)
        return res
