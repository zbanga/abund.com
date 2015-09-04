import os
import datetime
import pytz
import tzlocal
import time

import numpy as np
import pandas as pd

from celery import shared_task

from IPython.parallel import Client

from pulley.brokers.ib import broker, tws
from pulley.zp.finance.commission import PerShareWithMin
from pulley.zp.sources import redshift, yahoo, quant_quote, csi
from pulley.calendar import date_utils

from zipline.finance.trading import SimulationParameters
from zipline.finance.slippage import FixedSlippage
from zipline.utils.tradingcalendar import get_trading_days

eastern = pytz.timezone('US/Eastern')

class Runner(object):
    
    def __init__(self, tickers=[], capital_base=None, mlab=None, tws_port=7496):

        assert capital_base is not None and capital_base >= 0
        assert tickers is not None and len(tickers) > 0
        
        self.algo = None
        self.sim_params = None
        self.tickers = tickers
        self.capital_base = capital_base
        self.mlab = mlab        
        self.rows_prices = None
        self.bench_price_utc = None
        self.run_time = 0.0
        self.tws_port = tws_port
        
    def run(self, algo,
            tBeg=None, tEnd=None,
            commission=None,
            slippage=None,
            warn=False,
            bar_source='yahoo',
            adjusted=True,
            data_frequency='daily',
            include_open=True,
            csi_port='ETFs'):

        # Set a default algo if none is provided
        if not algo:
            raise Exception('No algo provided')
        self.algo = algo

        # set commission model
        if commission:
           self.algo.set_commission(commission)
        else:
            self.algo.set_commission(PerShareWithMin(comm_per_share=0.01, comm_min=1.0))

        # set slippage model
        if slippage:
            self.algo.set_slippage(slippage)
        else:
            self.algo.set_slippage(FixedSlippage(spread=0.0))

        # guess starting and ending dates if none are provided
        if not tEnd:
            tEnd = get_end_date()
        if not tBeg:
            tBeg = get_start_date(self.algo.iL, tNow=tEnd)

        tBeg = pytz.utc.localize(tBeg)
        tEnd = pytz.utc.localize(tEnd)
            
        self.sim_params = SimulationParameters(tBeg, tEnd,
                                               data_frequency=data_frequency,
                                               capital_base=self.capital_base,
                                               make_new_environment=True,
                                               extra_dates=[])
        # print self.sim_params

        source = self.get_bar_source(tBeg, tEnd, bar_source,
                                     adjusted=adjusted,
                                     include_open=include_open,
                                     csi_port=csi_port)

        if bar_source == 'redshift':
            bench_source, self.bench_price_utc = redshift.get_bench_source(tBeg, tEnd)
        else:
            bench_source = None

        sources = [source]

        if not warn:
            # turn off warnings
            import warnings
            warnings.filterwarnings('ignore')

        self.run_time = time.time()
        self.results = self.algo.run(sources, sim_params=self.sim_params, benchmark_return_source=bench_source)
        self.run_time = time.time() - self.run_time

    '''
    Get a list of Zipline events for each bar in our bar data source.
    '''
    def get_bar_source(self, tBeg, tEnd, bar_source, adjusted=True, include_open=True, csi_port='ETFs'):
        
        self.rows_prices = None

        if bar_source == 'yahoo':
            panel = yahoo.fetch(self.tickers, tBeg, tEnd, adjusted=adjusted)
            self.rows_prices = yahoo.flatten_panel(panel, include_open=include_open)
        elif bar_source == 'redshift':
            self.rows_prices = redshift.get_data(self.tickers, tBeg, tEnd, adjusted=adjusted)
        elif bar_source == 'quantquote':
            self.rows_prices = quant_quote.get_data(self.tickers, tBeg, tEnd)
        elif bar_source == 'csi':
            self.rows_prices = csi.get_data(self.tickers, tBeg, tEnd,
                                            include_open=include_open,
                                            portfolio=csi_port)
        else:
            raise Exception('Unknown bar_source: %s' % bar_source)
        
        return redshift.get_price_events(self.rows_prices)   
        
    def check_up_to_date(self):

        iS = len(self.tickers)
        data_recent = np.array(self.rows_prices[-iS:])

        # check that all tickers have a common most recent update date
        dt_unique = np.unique(data_recent[:,0])
        if len(dt_unique) != 1:
            raise Exception('All tickers do not have a common recent update date.')        
        dt_last_quote = dt_unique[0]

        # check that this common update date matches the most recent close
        dt_now = datetime.datetime.now()
        iNow = date_utils.datetime2iDate(dt_now)                
        iBeg = date_utils.datetime2iDate(dt_now - datetime.timedelta(days=5))
        iEnd = date_utils.datetime2iDate(dt_now)
        viBusdays = date_utils.nyseDates(iBeg, iEnd)
        if iNow != viBusdays[-1]:
            print viBusdays
            raise Exception('iNow = %i but most recent business day is %i' % (iNow, viBusdays[-1]))
        
        '''
        if eastern.localize(dt_last_quote) != self.algo.current_dt.astimezone(eastern):
            raise Exception('Current datetime of algorithm does not match that of the most recent quote. %s vs. %s',
                            (str(eastern.localize(dt_last_quote)), str(self.algo.current_dt.astimezone(eastern))))
        if dt_last_quote.date() != now_local().date():
            msg = 'Current date does not match that of the most recent quote. %s vs. %s' % \
                (str(dt_last_quote.date()), str(now_local().date()))
            raise Exception(msg)
        return True
        '''

    # broker connection and setup
    def ib_connect(self):
        self.ib = broker.IBBroker()
        self.ib.connect(port=self.tws_port)
        self.ib.tws.reqAccountUpdates(True, '')
        #self.ib.subscribe_list(self.tickers)
        if not self.ib.is_connected():
            raise Exception('Failed to connect with IB.')

    def ib_switch_port(self, new_tws_port):
        self.ib.disconnect()
        self.tws_port = new_tws_port
        self.ib_connect()
        
    def get_positions_frame(self):
        ib_df = self.ib.get_positions_frame()
        sync_dict = {}
        
        for tkr in self.tickers:
            shares_theo = 0
            if tkr in self.algo.portfolio.positions:
                shares_theo = self.algo.portfolio.positions[tkr].amount

            shares_actual = 0
            if 'position' in ib_df.keys() and tkr in ib_df.ix[:, 'position']:
                shares_actual = ib_df.ix[tkr, 'position']
                
            shares_pending = 0
            if tkr in self.algo.blotter.open_orders:
                orders = self.algo.blotter.open_orders[tkr]
                for order in orders:
                    shares_pending += order.amount
                    
            # based on algo.portfolio
            shares_diff1 = shares_theo - shares_actual
            # considers algo.blotter.open_orders as well
            shares_diff2 = (shares_theo + shares_pending) - shares_actual
            
            sync_dict[tkr] = {'shares_actual': shares_actual, 
                              'shares_theo': shares_theo,
                              'shares_pending': shares_pending,
                              'shares_diff1': shares_diff1,
                              'shares_diff2': shares_diff2,
                              }
        return pd.DataFrame.from_dict(sync_dict, orient='index')
    
    # shares_diff2
    def sync(self, dry=False, use_limit=False):
        # self.ib.order('SPY', 1)
        # return        
        pos_df = self.get_positions_frame()
        if pos_df is None:
            return
        print pos_df
        
        for tkr in self.tickers:
            amt = pos_df.shares_diff2[tkr]
            if amt != 0:
                limit_price = self.ib.get_price(tkr)
                # print '%s\t%i\t%s' % (tkr, int(amt), str(limit_price))
                if not dry:
                    if use_limit:
                        self.ib.order(tkr, amt, limit_price=limit_price)
                    else:
                        self.ib.order(tkr, amt)
        print '----> sync complete'

    # get a single quote
    def get_quote(self, tkr):
        return self.ib.get_quote(tkr)
    
    # gets quotes
    def get_quotes(self):
        quote_dict = {}
        prices_ok = True
        for tkr in self.tickers:
            price, size = self.get_quote(tkr)
            if not price:
                prices_ok = False
            quote_dict[tkr] = {'price': price, 'size': size}
        return quote_dict, prices_ok

    # update the rows of database-IB quote hybrid prices
    def update(self, dry=False):        
        dt = datetime.datetime.now()
        quotes, prices_ok = self.get_quotes()
        if not prices_ok:
            raise Exception('Bad prices in bar data.')
        for tkr, quote in quotes.iteritems():
            # IB returns quote size of zero sometimes.
            # Fake a big enough quote size to not trigger VolumeShareSlippage. 
            self.rows_prices.append((dt, tkr, quote['price'], 1000000)) #quote['size']

    # get ready for market open
    def pre_open(self, algo, dry=False, check_dates=True, bar_source='yahoo', adjusted=True, csi_port='ETFs'):

        # run with a minimum window of history
        self.run(algo, bar_source=bar_source, adjusted=adjusted, csi_port=csi_port)

        # check that algo is aware of newest data
        if check_dates:
            self.check_up_to_date()

        # connect to IB
        self.ib_connect()

        # sleep while IB initializes
        print '----> Sleeping for 5 seconds...'
        time.sleep(5)

        # send any new orders
        print '----> Synchronizing...'
        self.sync(dry=dry)

        print '----> Sleeping for 5 seconds...'
        time.sleep(5)

'''
Get the current time based on local system clock
'''
def now_local():
    return datetime.datetime.now()

'''
Gets a stopping date for the Zipline simulation.
'''
def get_end_date():
    return (datetime.datetime.now() + datetime.timedelta(days=1)).replace(\
        hour=0, minute=0, second=0, microsecond=0, tzinfo=None)

'''
Gets a starting date for the Zipline simulation assuming a lookbook of iL days.
'''
def get_start_date(iLag, tNow=None):
    if tNow == None:
        tNow = now_local().date()

    # convert business lag into calendar lag, 365/252 = 1.45
    # divide by 2 b/c we're using open prices and closing prices
    iLagDays = int(iLag*1.5/2.0) + 2
    
    trading_days = get_trading_days(tNow - datetime.timedelta(days=iLagDays), tNow)

    # print '----> tNow = %s' % tNow
    # print '----> iLagDays = %i' % iLagDays
    # print '----> len(trading_days): %i ' % len(trading_days)
    
    return trading_days[0].replace(tzinfo=None)
