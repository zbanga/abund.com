import sys
import datetime
import pytz
import logbook

import numpy as np
import pandas as pd

from zipline.algorithm import TradingAlgorithm

SPDRs =   [ 'XLY',  # XLY Consumer Discrectionary SPDR Fund   
            'XLF',  # XLF Financial SPDR Fund  
            'XLK',  # XLK Technology SPDR Fund  
            'XLE',  # XLE Energy SPDR Fund  
            'XLV',  # XLV Health Care SPRD Fund  
            'XLI',  # XLI Industrial SPDR Fund  
            'XLP',  # XLP Consumer Staples SPDR Fund   
            'XLB',  # XLB Materials SPDR Fund  
            'XLU' ] # XLU Utilities SPRD Fund
  
class OLMAR(TradingAlgorithm):
  
    def initialize(self,
                   tkrs = SPDRs,
                   eps = 1,
                   iL = 5):

        assert iL > 0
        assert len(tkrs) > 0
        
        self.tkrs = tkrs
        self.iS = len(self.tkrs)
        self.iL = int(iL)
        self.eps = float(eps)
        self.b_t = np.ones(self.iS)/float(self.iS)

        self.mnPrices = None
        self.vnDates = None
        self.current_dt = None
        self.current_data = None
        self.last_price = {}
        self.iHandles = 0
        self.iRebalances = 0

            
    def handle_data(self, data, debug=False):
        current_dt = self.get_datetime()
        self.current_dt = current_dt
        self.current_data = data
        self.iHandles += 1
        
        # yearly print of status
        if current_dt is not None and (current_dt.year != self.current_dt.year):

            pnl = self.portfolio.pnl/self.portfolio.starting_cash * 100.
            
            print '-----> current_dt: %s, len(data) = %i, num_pos = %i, pnl = %.2f' % \
                    (current_dt.strftime('%Y-%m-%d %H:%M:%S'),
                     len(data.keys()), self.num_open(), pnl)
        
        for tkr, tkr_data in data.iteritems():
            if 'price' in tkr_data:
                self.last_price[tkr] = tkr_data['price']
            
        # fill price array
        if self.mnPrices is None:
            self.init_price_array()

        iS = self.iS
        vnP = np.zeros((iS,))
        for i, tkr in enumerate(self.tkrs):
            vnP[i] = self.last_price[tkr]

        # MATLAB indexing 1:end-1 <- 2:end
        self.mnPrices[:-1, :] = self.mnPrices[1:, :]
        self.mnPrices[-1, :] = vnP

        if len(self.vnDates) == self.iL+1:
            self.vnDates.pop(0)
        self.vnDates.append(self.current_dt)
        
        if self.iHandles < self.iL + 1:
            return

        # do not trade at the open
        if self.current_dt.minute == 30:
            return
                
        x_tilde = np.zeros(self.iS)
        b = np.zeros(self.iS)

        # find relative moving average price for each security
        for i, tkr in enumerate(self.tkrs):
            x_tilde[i] = np.mean(self.mnPrices[:,i]) / self.mnPrices[-1,i]
        
        # market relative deviation
        x_bar = x_tilde.mean()            
        mark_rel_dev = x_tilde - x_bar

        # Expected return with current portfolio
        exp_return = np.dot(self.b_t, x_tilde)
        weight = self.eps - exp_return
        variability = (np.linalg.norm(mark_rel_dev))**2

        # test for divide-by-zero case
        if variability == 0.0:
            step_size = 0
        else:
            step_size = max(0, weight/variability)

        b = self.b_t + step_size*mark_rel_dev
        b_norm = OLMAR.simplex_projection(b)
        np.testing.assert_almost_equal(b_norm.sum(), 1)
        self.rebalance_portfolio(data, b_norm)

        # update portfolio
        self.b_t = b_norm
        
    def rebalance_portfolio(self, data, desired_port):
        self.iRebalances += 1
        desired_amount = np.zeros_like(desired_port)
        current_amount = np.zeros_like(desired_port)
        prices = np.zeros_like(desired_port)

        # constant cash for testing purposes (profits not re-invested)
        positions_value = self.portfolio.starting_cash
        #positions_value = self.portfolio.positions_value + self.portfolio.cash
        
        for i, stock in enumerate(self.tkrs):
            current_amount[i] = self.portfolio.positions[stock].amount
            prices[i] = data[stock].price

        desired_amount = np.round(desired_port * positions_value / prices)

        diff_amount = desired_amount - current_amount

        for i, stock in enumerate(self.tkrs):
            if diff_amount[i] == 0:
                continue
            self.order(stock, diff_amount[i])

            
    def init_price_array(self):
        self.mnPrices = np.zeros((self.iL+1, self.iS))
        self.vnDates = []

    def num_open(self):
        count = 0
        for tkr, position in self.portfolio.positions.iteritems():
            if position.amount != 0:
                count += 1
        return count

    @staticmethod
    def simplex_projection(v, b=1):
        v = np.asarray(v)
        p = len(v)
        
        # Sort v into u in descending order
        v = (v > 0) * v
        u = np.sort(v)[::-1]
        sv = np.cumsum(u)

        rho = np.where(u > (sv - b) / np.arange(1, p+1))[0][-1]
        theta = np.max([0, (sv[rho] - b) / (rho+1)])
        w = (v - theta)
        w[w < 0] = 0
        return w
