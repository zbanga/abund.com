
class Broker(object):
    
    name = '' # name of broker
    oid = 1   # order id
    cid = 1   # connection id

    def __init__(self):
        pass
    def connect(self):
        pass
    def disconnect(self):
        pass
    def is_connected(self):
        pass
    def get_contract_by_sid(self):
        pass
    def get_market_order(self):
        pass
    def place_order(self, contract, order):
        pass
    def order(self, sid, amt, limit_price=None, stop_price=None):
        pass

