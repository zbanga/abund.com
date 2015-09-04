import sys
import logbook
import datetime
import numpy as np
import pandas as pd
import pytz
import math

from zipline.algorithm import TradingAlgorithm
from zipline.protocol import DATASOURCE_TYPE
from zipline.finance.blotter import ORDER_STATUS
from zipline.utils.tradingcalendar import get_trading_days

# Order types for each event, keys for event_orders
ENTRY = 'ENTRY'
PT = 'PT'
SL = 'SL'
DC = 'DC'
EXIT_DESC = {PT: 'Profit target filled',
             SL: 'Stop loss filled',
             DC: 'Open day count'}

DEBUG = False

class EventTrader(TradingAlgorithm):

    def initialize(self,
                   exit_after_days = None,
                   profit_target = None,
                   stop_loss = None,
                   alloc_per_signal = None,
                   side = None):

        self.exit_after_days = int(exit_after_days)
        self.profit_target = float(profit_target)
        self.stop_loss = float(stop_loss)
        self.alloc_per_signal = float(alloc_per_signal)
        self.side = int(side)

        assert exit_after_days > 0
        assert profit_target > 0
        assert stop_loss > 0
        assert alloc_per_signal > 0
        assert side == 1 or side == -1

        self.side = float(self.side)
        self.event_orders = {}      # dict of dict of order IDs, keyed by event
        self.event_orders_old = {}  #
        self.tickers_wo_prices = [] # list of tickers that don't have price data
        self.skipped_events = []

    def print_data(self, data):
        for tkr in data.keys():
            if 'price' in data[tkr]:
                if 'event_date' in data[tkr]:
                    print '%s\t%s\t%5.2f\tHYBRID-EVENT' % \
                        (str(data[tkr]['dt']),
                         data[tkr]['sid'],
                         data[tkr]['price'])
                else:
                    print '%s\t%s\t%5.2f\tTRADE-EVENT' % \
                        (str(data[tkr]['dt']),
                         data[tkr]['sid'],
                         data[tkr]['price'])

    '''
    Returns a unique identifier for an event.    
    <TODO> hash this
    '''
    def get_event_id(self, event_date, event_sym):
        return '%s_%s' % (event_sym, event_date.strftime('%Y-%m-%d_%H:%M:%S'))
    
    def handle_data(self, data):

        # Cancel any stop loss (profit target) order
        # if its profit target (stop loss) has been filled.
        
        keys_to_del = []
        current_dt = self.get_datetime()
        new_pos_value = 0.0
        
        for event_id in self.event_orders:
            oid_en = None
            oid_sl = None
            oid_pt = None
            oid_dc = None

            event_orders = self.event_orders[event_id]

            # Double check that the entry is filled or
            # waiting to be filled with NO exit orders.
            oid_en = event_orders[ENTRY]
            order_en = self.get_order(oid_en)
            keys = event_orders.keys()

            # <TODO> Why does this happen?
            if order_en is None:
                if DEBUG:
                    print '----> Entry order is None, event_id = %s' % event_id
                continue
            
            # Make sure no exit orders have been sent if the entry is not yet filled
            if order_en.status != ORDER_STATUS.FILLED:
                assert SL not in keys and PT not in keys and DC not in keys
            
            if SL in keys:
                oid_sl = event_orders[SL]
                order_sl = self.get_order(oid_sl)
            if PT in keys:
                oid_pt = event_orders[PT]
                order_pt = self.get_order(oid_pt)
            if DC in keys:
                oid_dc = event_orders[DC]
                order_dc = self.get_order(oid_dc)

            # Case when PT and SL have both been sent.
            if oid_sl and oid_pt:
                # Case when SL is filled and PT is open
                if order_sl.status == ORDER_STATUS.FILLED and order_pt.status == ORDER_STATUS.OPEN:
                    self.cancel_order(oid_pt)
                    if DEBUG:
                        print "--------> PT cancelled"
                # Case when SL is open and PT is filled
                elif order_sl.status == ORDER_STATUS.OPEN and order_pt.status == ORDER_STATUS.FILLED:
                    self.cancel_order(oid_sl)
                    if DEBUG:
                        print "--------> SL cancelled"
                # Case when SL and PT are either both filled or both canceled,
                # with the canceled case being due to a DC exit being sent.
                elif order_sl.status != ORDER_STATUS.OPEN and order_pt.status != ORDER_STATUS.OPEN:
                    # case when DC is either filled, canceled, or was never sent.
                    if (oid_dc and order_dc.status != ORDER_STATUS.OPEN) or not oid_dc:
                        keys_to_del.append(event_id)
                        continue

            # send PT order
            if not oid_pt:
                position = self.portfolio.positions[order_en.sid]
                limit_price = position.cost_basis * (1. + self.profit_target*self.side)
                oid = self.order(order_en.sid, -1*position.amount, limit_price)
                event_orders[PT] = oid
                self.event_orders[event_id] = event_orders

                if DEBUG:
                    print "--------> PT sent | Order(%s, %s, %.2f)" \
                        % (order_en.sid, -1*position.amount, limit_price)
                    
            # send SL order
            if not oid_sl:
                position = self.portfolio.positions[order_en.sid]
                stop_price = position.cost_basis * (1. - self.stop_loss*self.side)
                oid = self.order(order_en.sid, -1*position.amount, None, stop_price)
                event_orders[SL] = oid
                self.event_orders[event_id] = event_orders

                if DEBUG:
                    print "--------> SL sent | Order(%s, %s, %.2f)" \
                        % (order_en.sid, -1*position.amount, stop_price)

            # send DC order conditional upon number of days being exceeded
            if not oid_dc:
                position = self.portfolio.positions[order_en.sid]
                #open_days =  (current_dt - order_en.created).days # calendar days open
                open_days = get_trading_days_num(order_en.created, current_dt) # busdays open
                
                # Case when open day counter has been reached.
                # Cancel SL and PT, then exit position at market price.
                if open_days >= self.exit_after_days:
                    self.cancel_order(oid_pt)
                    self.cancel_order(oid_sl)
                    
                    oid = self.order(order_en.sid, -1*position.amount)
                    event_orders[DC] = oid
                    self.event_orders[event_id] = event_orders

                    if DEBUG:
                        print "--------> DC sent | Order(%s, %s, %i)" \
                            % (order_en.sid, -1*position.amount, open_days)

        # move finished entries to the old dict for speed
        for key in keys_to_del:
            self.event_orders_old[key] = self.event_orders[key]
            del self.event_orders[key]
                
        for tkr in data.keys():
            if tkr in self.tickers_wo_prices:
                continue

            has_event_date = 'event_date' in data[tkr]
            shares_open = self.portfolio.positions[tkr].amount
            
            # case when an event occurs for tkr and we have no exisitng open position
            if has_event_date and shares_open == 0:
                
                # Case when there is not price data for this scan match,
                # append to missed signals.
                if 'price' not in data[tkr]:
                    self.tickers_wo_prices.append(tkr)
                    continue
                
                event_sym = data[tkr]['sid']
                event_date = data[tkr]['event_date']
                event_id = self.get_event_id(event_date, event_sym)

                # <TODO> remove
                assert tkr == event_sym
                
                # Case when we've already sent an entry order for this scan event.
                # Required b/c older non-contemporaneous events get carried forward to handle_data.
                if event_id in self.event_orders.keys():
                    continue

                # Case when we've already skipped this event due to a position being open
                if event_id in self.skipped_events:
                    continue

                # Case when we've used all of our available equity
                positions_value = self.portfolio.positions_value
                capital = self.portfolio.starting_cash
                
                # Check if our positions are worth more than our base capital
                if abs(positions_value) + abs(new_pos_value) >= capital:
                    # print "----> %s | CAPITAL_BASE_EXCEEDED | base = %.2f | pos_val = %.2f | new_pos_value = %.2f" \
                    #     % (str(data[tkr]['dt']), capital, positions_value, new_pos_value)
                    break

                # <ALLOCATION>
                price = float(data[tkr]['price'])
                shares = int(round(self.side*(self.alloc_per_signal/price)))

                oid = self.order(event_sym, shares)
                self.event_orders[event_id] = {ENTRY: oid}

                # increment the value of new positions to be opened on the next bar
                new_pos_value += -1.0 * float(shares) * float(price)
                
                if DEBUG:
                    print "----> %s: Entry | Order(%s, %i) | %s | %s" \
                        % (data[tkr]['dt'], event_sym, shares, event_id, oid)
                    
            elif has_event_date and shares_open != 0:
                                
                event_id  = self.get_event_id(data[tkr]['event_date'], data[tkr]['sid'])
                self.skipped_events.append(event_id)


'''
Uses the zipline trading calendar to get number of business days between two dates.
'''
def get_trading_days_num(start_date, end_date):
    return len(get_trading_days(start_date, end_date))


'''
Trades as a dictionary keyed by order id.
'''
def transactions_to_dict(transactions):
    d = {}
    for index in transactions.index:
        elems_day = transactions[index]
        if len(elems_day) == 0:
            continue
        for elem_dict in elems_day:
            order_id = elem_dict['order_id']# + '_' + str(index)
            if order_id in d.keys():
                raise Exception('----> Duplicate order IDs in zipline transactions: %s' % order_id)
            else:
                d[order_id] = elem_dict
    return d
   

'''
Makes a list of table for each event's round-trips. The input 'transactions'
is from the final Pandas zp_results.transactions.
'''
def event_trip_table(transactions, event_orders_all):

    # keyed by order ID, with all fill prices for each transaction
    trans_dict = transactions_to_dict(transactions)

    # <TODO> include others
    # event_orders_all = self.event_orders_old

    tbl = []
    for event_id, event_orders in event_orders_all.iteritems():
        keys = event_orders.keys()
        oid_n = event_orders[ENTRY]
        try:
            trans_n = trans_dict[oid_n]
        except KeyError:
            if DEBUG:
                print '-----> Missing entry order in zp_transactions: %s' % oid_n
            continue

        filled_order_types = []
        for order_type in (SL, PT, DC):
            # if this order_type has a filled transaction
            if order_type in event_orders and event_orders[order_type] in trans_dict:
                trans_x = trans_dict[event_orders[order_type]]
                filled_order_types.append(order_type)

        if len(filled_order_types) != 1:
            print "----> Multiple exit order types filled for event: %s" % event_id
            #raise Exception("Multiple exit order types filled for event: %s" % event_id)

        # check that entry and exit orders match
        #assert trans_n['sid'] == trans_x['sid']

        if trans_n['amount'] != -trans_x['amount']:
            print "----> Amounts for entry and exit positions don't match: %i vs %i " % \
                (trans_n['amount'], -trans_x['amount'])
            continue

        sid = trans_n['sid']
        Tn = trans_n['dt']
        Tx = trans_x['dt']
        days_open = get_trading_days_num(Tn, Tx)
        Vn = trans_n['amount']
        Pn = trans_n['price']
        Px = trans_x['price']
        pnl = float(Vn)*(Px - Pn)
        exit_desc = EXIT_DESC[filled_order_types[0]]

        tbl.append([sid, Tn, Tx, days_open, Vn, Pn, Px, pnl, exit_desc, event_id])

    return tbl
