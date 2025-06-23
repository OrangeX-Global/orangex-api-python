class Path(object):
    def __init__(self, base, path):
        self.path = path
        self.base = base

    @property
    def url(self):
        return self.base + self.path


class Urls(object):
    base = 'https://api.orangex.com/api/v1'
    ws_base = 'wss://api.orangex.com/ws/api/v1'

    token = Path(base, '/public/auth')
    depth = Path(base, '/public/get_order_book')
    wallet_transfer = Path(base, '/private/submit_transfer')
    withdraw = Path(base, '/private/withdraw')
    leverage = Path(base, '/private/adjust_perpetual_leverage')
    adjust_perpetual_margin_type = Path(base, '/private/adjust_perpetual_margin_type')
