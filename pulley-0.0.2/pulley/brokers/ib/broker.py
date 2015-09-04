"""
2015-04-01 | Created by Jimmie Goode (jimmie@goodeanalytics.com)
"""

import datetime
import pytz
import numpy as np
import pandas as pd

from swigibpy import EWrapper, EPosixClientSocket, Contract, Order, TagValue, TagValueList

from pulley.brokers.broker import Broker

'''
Class for an Interactive Brokers TWS connection. Each instance has it's own EWrapper class
for catching events via callback functions. If no EWrapper is provided, a default is
created with WrapperDefault().
'''
class IBBroker(Broker):

    cid = 0 # connection id
    oid = 0 # order id
    tid = 0 # tick id (for fetching quotes)
    tws = None # Trader WorkStation 
    wrapper = None # instance of EWrapper
    sid_to_tid = {} # map of security id to tick id
    
    def __init__(self, wrapper=None):
        Broker.__init__(self)
        
        # initialize a default wrapper
        if wrapper:
            self.wrapper = wrapper
        else:
            self.wrapper = WrapperDefault()

        # initialize the wrapper's portfolio object
        self.wrapper.portfolio = IBPortfolio()
            
    # get next order id
    def get_next_oid(self):
        IBBroker.oid += 1
        return IBBroker.oid

    # get next connection id
    def get_next_cid(self):
        IBBroker.cid += 1
        return IBBroker.cid

    # get next tick request id (for getting quotes)
    def get_next_tid(self):
        self.tid += 1
        return self.tid
    
    # connect to TWS
    def connect(self, port=7496):
        if self.is_connected():
            self.disconnect()
        cid = self.get_next_cid()
        self.tws = EPosixClientSocket(self.wrapper)
        self.tws.eConnect('', port, cid)

    # disconnect from TWS
    def disconnect(self):
        self.tws.eDisconnect()

    # check if TWS is connected
    def is_connected(self):
        if self.tws is None:
            return False
        return self.tws.isConnected()

    # Convert Zipline order signs into IB action strings
    def order_action(self, iSign):
        if iSign > 0:
            return 'BUY'
        elif iSign < 0:
            return 'SELL'
        raise Exception('Order of zero shares has no IB side: %i' % iSign)

    # get an IB contract by ticker
    def get_contract_by_sid(self, sid):
        contract = Contract()
        contract.symbol = sid
        contract.secType = 'STK'
        contract.exchange = 'SMART'
        contract.currency = 'USD'
        return contract
    
    # get a default IB market order
    def get_market_order(self, sid, amt):
        order = Order();
        order.action = self.order_action(amt)
        order.totalQuantity = abs(amt)
        order.orderType = 'MKT'
        order.tif = 'DAY'
        order.outsideRth = False
        return order

    # get a default IB limit order
    def get_limit_order(self, sid, amt, lmtPrice):
        order = Order();
        order.action = self.order_action(amt)
        order.totalQuantity = abs(amt)
        order.orderType = 'LMT'
        order.tif = 'DAY'
        order.outsideRth = False
        order.lmtPrice = lmtPrice
        return order

    # send the IB (contract, order) order to TWS
    def place_order(self, contract, order):
        oid = self.get_next_oid()
        self.tws.placeOrder(oid, contract, order)
        return oid
    
    # send order with Zipline style order arguments
    # <TODO> stop_price is not implemented
    def order(self, sid, amt, limit_price=None, stop_price=None):
        contract = self.get_contract_by_sid(sid)
        amt = int(amt)
        if limit_price is None:
            order = self.get_market_order(sid, amt)
        else:
            order = self.get_limit_order(sid, amt, limit_price)
        return self.place_order(contract, order)

    # subscribe to market data ticks
    def subscribe(self, sid):
        tid = self.get_next_tid()
        self.sid_to_tid[sid] = tid
        contract = self.get_contract_by_sid(sid)
        self.tws.reqMktData(tid, contract, '', False)
        return tid

    # subscribe to market data ticks for a list of tickers
    def subscribe_list(self, tickers):
        for tkr in tickers:
            self.subscribe(tkr)

    # cancel a market data subscription
    def unsubscribe(self, sid):
        if sid not in self.sid_to_tid.keys():
            return
        tid = self.sid_to_tid[sid]
        self.tws.cancelMktData(tid)

    # cancel all market data subscriptions
    def unsubscribe_all(self):
        sids = self.sid_to_tid.keys()
        for sid in sids:
            self.unsubscribe(sid)

    # fetch a quote by ticker id tid
    def get_quote_by_tid(self, tid):
        return self.wrapper.tid_to_price[tid]

    # fetch a quote by ticker sid
    def get_quote(self, sid):
        if sid not in self.sid_to_tid:
            self.subscribe(sid)
            return (None, None)
        tid = self.sid_to_tid[sid]
        if tid not in self.wrapper.tid_to_price:
            price = None
        else:
            price = self.wrapper.tid_to_price[tid]
        if tid not in self.wrapper.tid_to_size:
            size = None
        else:
            size = self.wrapper.tid_to_size[tid]
        return (price, size)

    # fetch a price by ticker sid
    def get_price(self, sid):
        if sid not in self.sid_to_tid:
            self.subscribe(sid)
            return None

        tid = self.sid_to_tid[sid]
        if tid not in self.wrapper.tid_to_price:
            return None
        else:
            price_dict = self.wrapper.tid_to_price[tid]
            if 'price' in price_dict:
                return price_dict['price']
        return None

    # get a Pandas DataFrame of current positions
    def get_positions_frame(self):
        ib_dict = {}
        for sid, position in self.wrapper.portfolio.sid_to_position.iteritems():
            # <TODO> don't use vars here
            #ib_dict[sid] = vars(position)
            ib_dict[sid] = {'marketValue': position.marketValue,
                            'realizedPNL': position.realizedPNL,
                            'marketPrice': position.marketPrice,
                            'unrealizedPNL': position.unrealizedPNL,
                            'accountName': position.accountName,
                            'averageCost': position.averageCost,
                            'sid': position.sid,
                            'position': position.position}
            
        return pd.DataFrame.from_dict(ib_dict, orient='index')
    
'''
EWrapper class to catch all IB events.
'''
class WrapperDefault(EWrapper):

    # these two dicts are required for keeping track of quote requests
    tid_to_price = {}
    tid_to_size = {}
    portfolio = None # object of type IBPortfolio()

    def accountDownloadEnd(self, accountName):
	pass

    def bondContractDetails(self, reqId, contractDetails):
        pass

    def commissionReport(self, commissionReport):
        pass
    
    def contractDetails(self, reqId, contractDetails):
        pass

    def contractDetailsEnd(self, reqId):
        pass

    def currentTime(self, time):
        pass

    def fundamentalData(self, reqId, data):
        pass

    def historicalData(self, reqId, date, open, high, low, close, volume, count, WAP, hasGaps):
        pass

    def managedAccounts(self, accountsList):
        pass
    
    def nextValidId(self, orderId):
        IBBroker.oid = orderId
        print '----> next valid orderId = %i' % orderId

    def realtimeBar(self, reqId, time, open, high, low, close, volume, wap, count):
        pass

    def receiveFA(self, faDataType, xml):
        pass

    def scannerData(self, reqId, rank, contractDetails, distance,  benchmark, projection, legsStr):
        pass

    def scannerDataEnd(self, reqId):
        pass
	
    def scannerParameters(self, xml):
        pass

    def tickEFP(self, tickerId, tickType, basisPoints,
                formattedBasisPoints, impliedFuture, holdDays,
                futureExpiry, dividendImpact, dividendsToExpiry):
        pass

    def tickGeneric(self, tickerId, tickType, value):
        pass

    def tickOptionComputation(self, tickerId, field, impliedVol, delta, optPrice,
                              pvDividend, gamma, vega, theta, undPrice):
        pass
	
    def tickPrice(self, tickerId, field, price, canExecuteAuto):
        self.tid_to_price[tickerId] = {'tickerID': tickerId,
                                       'field': field,
                                       'price': price,
                                       'canExecuteAuto': canExecuteAuto}
        
    def tickSize(self, tickerId, field, size):
        self.tid_to_size[tickerId] = {'tickerID': tickerId,
                                       'field': field,
                                       'size': size}
	
    def tickString(self, tickerId, tickType, value):
        pass

    def tickSnapshotEnd(self, tickerId):
        pass

    def updateAccountTime(self, timeStamp):
        pass

    def updateAccountValue(self, key, value, currency, accountName):
        pass

    def updateMktDepth(self, tickerId, position, operation, side, price, size):
        pass

    def updateMktDepthL2(self, tickerId, position, marketMaker, operation, side, price, size):
        pass
    
    def updateNewsBulletin(self, msgId, msgType, message, origExchange):
        pass

    
    def updatePortfolio(self, contract, position, marketPrice, marketValue, averageCost,
                        unrealizedPNL, realizedPNL, accountName):

        self.portfolio.update(contract, position, marketPrice, marketValue, averageCost,
                              unrealizedPNL, realizedPNL, accountName)

    def connectionClosed(self):
        pass
        
    def error(self, id, errorCode, errorMsg):
        print '----> ib_wrapper: error( %i, %s, %s )' % (id, errorCode, errorMsg)

    def openOrder(self, orderID, contract, order, orderState):
        pass

    def openOrderEnd(self):
        pass

    def orderStatus(self, id, status, filled, remaining, avgFillPrice, permId,
                    parentId, lastFilledPrice, clientId, whyHeld):
        pass


    def deltaNeutralValidation(self, reqId, underComp):
        pass

    def execDetails(self, reqId, contract, execution):
        pass

    def execDetailsEnd(self, reqId):
        pass
    
'''
IB contract objects don't hash properly when used as dict keys.
Thus we use sid as the identifier (which is the IB tkr).
'''
class IBPosition(object):

    sid = None
    
    def __init__(self, sid):
        self.sid = sid

    def update(self, position, marketPrice, marketValue, averageCost, unrealizedPNL, realizedPNL, accountName):

        if self.sid is None:
            raise Exception('No position object exists for this contract.')

        self.position = position;
        self.marketPrice = marketPrice;
        self.marketValue = marketValue;
        self.averageCost = averageCost;
        self.unrealizedPNL = unrealizedPNL;
        self.realizedPNL = realizedPNL;
        self.accountName = accountName;

'''
Mainly just a dict that maps each IB contract to an IBPosition object.
'''
class IBPortfolio(object):

    def __init__(self):
        self.sid_to_position = {}

    def update(self, contract, position, marketPrice, marketValue, averageCost,
                        unrealizedPNL, realizedPNL, accountName):

        sid = contract.symbol
        
        # get or create an IBPosition for this contract        
        if sid in self.sid_to_position.keys():
            ib_position = self.sid_to_position[sid]
        else:
            ib_position = IBPosition(sid)

        # update the position with new info
        ib_position.update(position, marketPrice, marketValue, averageCost,
                        unrealizedPNL, realizedPNL, accountName)
        
        self.sid_to_position[sid] = ib_position
            
    def get_positions_by_sid(self, sid):
        net_pos = 0
        ib_positions = []
        for sid_key, ib_position in self.sid_to_position.iteritems():
            if sid_key == sid:
                ib_positions.append(ib_position)
                net_pos += ib_position.position
        return ib_positions, net_pos
        
