import numpy as np
import pandas as pd

from zipline.finance import trading
from zipline.finance.blotter import ORDER_STATUS

class Report(object):
    def __init__(self,
                 sim_params,
                 results,
                 MAR=0.0,
                 periods_per_year=252,
                 ret_riskless=None):
        self.sim_params = sim_params
        self.results = results
        self.MAR = MAR
        self.periods_per_year = periods_per_year
        self.ret_riskless = ret_riskless
        
    def build(self):
        # algo returns
        ret_algo = pd.Series(name='ret_algo')
        for dt, ret in (self.results.portfolio_value.diff() / self.sim_params.capital_base).iterkv():
            ret_algo = ret_algo.set_value(dt.date(), ret)
        #ret_algo = self.results.portfolio_value.diff() / self.sim_params.capital_base

        # benchmark returns
        ret_bench = pd.Series(name='ret_bench')
        for dt, ret in trading.environment.benchmark_returns.iterkv():
            if dt.date() >= self.sim_params.period_start.date() \
                    and dt.date() <= self.sim_params.period_end.date():
                ret_bench = ret_bench.set_value(dt, ret)

        # Zipline results
        positions = self.results.positions
        transactions = self.results.transactions

        # remove any leading period with no trading
        idx_first_trade = -1
        for i, dt in enumerate(transactions.index):
            if len(transactions[dt]) > 0:
                idx_first_trade = i-1
                break
        if idx_first_trade > 0:
            ret_bench = ret_bench[ idx_first_trade: ]
            ret_algo = ret_algo[ idx_first_trade: ]
            transactions = transactions[ idx_first_trade: ]
            positions = positions[ idx_first_trade: ]
                
        # intersect the two return series
        ret_join = pd.concat([ret_algo, ret_bench], join='inner', axis=1)
        self.ret_join = ret_join
        self.ret_algo = ret_join['ret_algo']
        self.ret_bench = ret_join['ret_bench']

        self.sum_transactions(transactions)
        self.sum_positions(positions)

    # Get amounts transacted on both the long and short side
    def sum_transactions(self, transactions):
        if transactions is None:
            raise Exception('Zipline transactions not set.')
         # Get amounts transacted on both the long and short side
        zero = np.zeros(len(transactions))
        transaction_sum = pd.DataFrame(data={'Bought': zero, 'Sold': zero},
                                       index=transactions.index)
        iTotalTrans = 0 # num trades
        iTotalShares = 0 # num shares traded (absolute)
        nTotalComms = 0.0 # total commissions
        for day in transactions.index:
            transDay = transactions[day]
            for tran in transDay:
                if not isinstance(tran, dict):
                    print tran[0]
                    raise Exception('Not a dict: ' + str(type(tran)))
                amt = tran['amount']
                price = tran['price']
                iTotalTrans += 1
                iTotalShares += abs(amt)
                nTotalComms += tran['commission']
                if tran['amount'] > 0:
                    transaction_sum['Bought'][day] += amt*price
                elif tran['amount'] < 0:
                    transaction_sum['Sold'][day] += amt*price
                else:
                    raise Exception('Number of shares transacted was zero.')
            #transaction_sum['Net'][day] = transaction_sum['Bought'][day] + transaction_sum['Sold'][day]
        self.transaction_sum = transaction_sum
        self.iTotalTrans = iTotalTrans
        self.iTotalShares = iTotalShares
        self.nTotalComms = nTotalComms

    # Get long counts and short counts for open positions
    # Get equity for long and short positions
    def sum_positions(self, positions):

        zero = np.zeros(len(positions))
        side_counts = pd.DataFrame(data={'Long': zero, 'Short': zero}, index=positions.index)

        '''
        Note: The open_equity calculated here is EXACTLY the value of algo.portfolio.poritions_value,
        which is availiable in the algo. It is stored as zp_results.ending_value in the final
        Zipline output. On the chart these are one bar apart.
        '''
        # 'Net':zero},
        open_equity = pd.DataFrame(data={'Long':zero, 'Short':zero}, index=positions.index)

        total_comms = {'Long':0.0, 'Short':0.0}

        # Loop over each day's positions
        for day in positions.index:
            posDay = positions[day]
            for pos in posDay:
                amt = pos['amount']
                price = pos['last_sale_price']

                if amt > 0:
                    side_counts['Long'][day] += 1
                    open_equity['Long'][day] += amt*price
                elif amt < 0:
                    side_counts['Short'][day] += 1
                    open_equity['Short'][day] += amt*price
                else:
                    raise Exception('Number of shares for position was zero.')

            #open_equity['Net'][day] = open_equity['Short'][day] + open_equity['Long'][day]

        self.open_equity = open_equity
        self.side_counts = side_counts

    def plot(self):

        import matplotlib.pyplot as plt
        from pylab import *

        plt.figure(figsize=(15, 20))

        ax1 = plt.subplot(421)
        (self.ret_join.cumsum()*100.0).plot(ax=ax1, color=('g','b'), rot=30)
        plt.ylabel('Return (%)')
        plt.title('Cummulative Return (%)')

        # Histograms
        ax2 = plt.subplot(422)

        '''
        n = len(ret_algo)
        ks_algo = gaussian_kde(ret_algo) # KS density fn
        ks_spx  = gaussian_kde(ret_spx) # KS density fn
        x = np.linspace(-0.10, 0.10, n)
        ax2.plot(x, ks_algo(x), 'g')
        ax2.plot(x, ks_spx(x), 'b')
        '''
        BINS = 15
        hist(self.ret_algo.dropna().values*100.0, alpha=.4, bins=BINS, color='g', label='Algo')
        hist(self.ret_bench.dropna().values*100.0,  alpha=.4, bins=BINS, color='b', label='S&P 500')
        plt.xlabel('Return (%)')
        plt.ylabel('Count')
        plt.title('Daily return histogram')

        ax3 = plt.subplot(423, sharex=ax1)
        self.transaction_sum.plot(ax=ax3, sharex=ax1, color=('g','r'))
        plt.ylabel('Amount (USD)')
        plt.title('Daily value transacted')

        ax4 = plt.subplot(424)
        hist(self.transaction_sum['Bought'].values, alpha=0.55, bins=BINS, color='g', label='Bought')
        hist(self.transaction_sum['Sold'].values,  alpha=0.55, bins=BINS, color='r', label='Sold')
        plt.xlabel('Value Transacted (USD)')
        plt.title('Daily value transacted histogram')

        ax5 = plt.subplot(425, sharex=ax1)
        self.open_equity.plot(ax=ax5, sharex=ax1, color=('g','r','b'))
        plt.ylabel('Amount (USD)')
        plt.title('Open Equity')

        ax6 = plt.subplot(426)
        hist(self.open_equity['Long'].values, alpha=0.55, bins=BINS, color='g', label='Long')
        hist(self.open_equity['Short'].values,  alpha=0.55, bins=BINS, color='r', label='Short')
        plt.xlabel('Open Equity (USD)')
        plt.title('Open equity histogram')

        ax7 = plt.subplot(427, sharex=ax1)
        self.side_counts.plot(ax=ax7, sharex=ax1, color=('g','r'))
        plt.title('Number of open positions')
        plt.xlabel('Number of open positions')

        ax8 = plt.subplot(428)
        hist(self.side_counts['Long'].values, alpha=0.55, bins=BINS, color='g', label='Long')
        hist(self.side_counts['Short'].values, alpha=0.55, bins=BINS, color='r', label='Short')
        plt.title('Number of open positions histogram')
        plt.xlabel('Number of open positions')
        
        ##
        ## Format x and y axes for each subplot
        ##

        format_money  = matplotlib.ticker.FuncFormatter(format_money_fn)
        format_return = matplotlib.ticker.FuncFormatter(format_return_fn)

        box = ax1.get_position()
        ax1.set_position([box.x0, box.y0, box.width * 0.8, box.height])
        ax1.legend(loc='center left', bbox_to_anchor=(1, 0.5))
        ax1.xaxis.grid(color='gray', linestyle='dashed')
        ax1.yaxis.grid(color='gray', linestyle='dashed')
        ax1.yaxis.set_major_formatter(format_return)

        box = ax2.get_position()
        ax2.set_position([box.x0, box.y0, box.width * 0.8, box.height])
        #ax2.legend(loc='center left', bbox_to_anchor=(1, 0.5))
        ax2.xaxis.grid(color='gray', linestyle='dashed')
        ax2.yaxis.grid(color='gray', linestyle='dashed')
        ax2.xaxis.set_major_formatter(format_return)

        box = ax3.get_position()
        ax3.set_position([box.x0, box.y0, box.width * 0.8, box.height])
        ax3.legend(loc='center left', bbox_to_anchor=(1, 0.5))
        ax3.xaxis.grid(color='gray', linestyle='dashed')
        ax3.yaxis.grid(color='gray', linestyle='dashed')
        ax3.yaxis.set_major_formatter(format_money)

        box = ax4.get_position()
        ax4.set_position([box.x0, box.y0, box.width * 0.8, box.height])
        #ax4.legend(loc='center left', bbox_to_anchor=(1, 0.5))
        ax4.xaxis.grid(color='gray', linestyle='dashed')
        ax4.yaxis.grid(color='gray', linestyle='dashed')
        ax4.xaxis.set_major_formatter(format_money)

        box = ax5.get_position()
        ax5.set_position([box.x0, box.y0, box.width * 0.8, box.height])
        ax5.legend(loc='center left', bbox_to_anchor=(1, 0.5))
        ax5.xaxis.grid(color='gray', linestyle='dashed')
        ax5.yaxis.grid(color='gray', linestyle='dashed')
        ax5.yaxis.set_major_formatter(format_money)

        box = ax6.get_position()
        ax6.set_position([box.x0, box.y0, box.width * 0.8, box.height])
        #ax6.legend(loc='center left', bbox_to_anchor=(1, 0.5))
        ax6.xaxis.grid(color='gray', linestyle='dashed')
        ax6.yaxis.grid(color='gray', linestyle='dashed')
        ax6.xaxis.set_major_formatter(format_money)

        box = ax7.get_position()
        ax7.set_position([box.x0, box.y0, box.width * 0.8, box.height])
        ax7.legend(loc='center left', bbox_to_anchor=(1, 0.5))
        ax7.xaxis.grid(color='gray', linestyle='dashed')
        ax7.yaxis.grid(color='gray', linestyle='dashed')

        box = ax8.get_position()
        ax8.set_position([box.x0, box.y0, box.width * 0.8, box.height])
        # ax8.legend(loc='center left', bbox_to_anchor=(1, 0.5))
        ax8.xaxis.grid(color='gray', linestyle='dashed')
        ax8.yaxis.grid(color='gray', linestyle='dashed')

        '''
        plt.setp(ax1.get_xticklabels(), visible=False) # remove tick labels
        plt.setp(ax2.get_xticklabels(), visible=False) # remove tick labels
        plt.setp(ax3.get_xticklabels(), visible=False) # remove tick labels
        plt.setp(ax4.get_xticklabels(), visible=False) # remove tick labels
        plt.setp(ax5.get_xticklabels(), visible=False) # remove tick labels
        '''

        plt.tight_layout(pad=2, h_pad=5, w_pad=15)
        plt.show()

        
# Function for conversion of numbers to money style strings.
def format_money_fn(x, pos):
    return '${:,.0f}'.format(x)

# Function for conversion of numbers to percent styles.
def format_return_fn(x, pos):
    return '{0:.1f}%'.format(x)

def zp_order_status_to_str(status):
    if status == ORDER_STATUS.CANCELLED:
        return 'CANCELLED'
    if status == ORDER_STATUS.FILLED:
        return 'FILLED'
    if status == ORDER_STATUS.OPEN:
        return 'OPEN'
    return None

'''
Convert Zipline Pandas TimeSeries of list of dicts to a nested list,
ignoring empty entries. Keeps the Pandas index in the last column.
'''
def zp_pd_series_to_list(ts, key_list):
    table_list = []
    for t_index in ts.index:
        elems_day = ts[t_index] 
        if len(elems_day) == 0:
            continue
        for elem_dict in elems_day:
            row_list = []
            for key in key_list:
                row_list.append(elem_dict[key])
            row_list.append(t_index)
            table_list.append(row_list)

    # add an entry for the included pandas index datetime
    #cols_list = key_list.append('dt_for_day')
    return table_list


# Names for zipline results tables
COLS_TRANS_ZP = ['sid', 'dt', 'amount', 'price', 'commission', 'order_id']
COLS_TRANS    = ['Ticker', 'Date (UTC)', 'Shares', 'Price', 'Commissions', 'Value', 'Order ID']

COLS_POS_ZP = ['sid', 'amount', 'cost_basis', 'last_sale_price']
COLS_POS   = ['Ticker', 'Date (UTC)', 'Shares', 'Cost Basis', 'Last Price', 'P&L']

COLS_ORDERS_ZP = ['sid', 'created', 'dt', 'amount', 'filled',
                  'limit', 'limit_reached',
                  'stop', 'stop_reached',
                  'status', 'id']
COLS_ORDERS = COLS_ORDERS_ZP

# columns for the event_trip_table method
EVENT_TRIP_COLS = ['Ticker', 'Entry Date (UTC)', 'Exit Date (UTC)', 'Days Open', 'Shares',
                   'Adj.<br>Entry Price', 'Adj.<br>Exit Price', 'PNL', 'Exit Reason', 'Event ID']
