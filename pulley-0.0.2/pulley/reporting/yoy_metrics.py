import traceback
import numpy as np
import pandas as pd
import pickle
import simplejson as json
import time
import pytz

from zipline.finance.risk import RiskMetricsPeriod, RiskReport
from zipline.utils.factory import create_returns_from_list
from zipline.finance.blotter import ORDER_STATUS

'''
Compute YOY metrics for Zipline results.
'''
def compute(results, sim_params):
    
    trading_dates = results.index
    idx = np.where(trading_dates.year[0:-1] != trading_dates.year[1:])[0]

    # include the last element for the most recent year
    idx_last = trading_dates.shape[0] - 1
    if idx_last not in idx:
        idx = np.append(idx, idx_last) 

    returns_risk = create_returns_from_list([ret for ret in results.returns], sim_params)

    dt_ranges = []
    yoy_metrics = []
    for i in range(len(idx)):
        if i == 0:
            tBeg = trading_dates[0].tz_localize('UTC')
        else:
            tBeg = trading_dates[idx[i-1]].tz_localize('UTC')
        tEnd = trading_dates[idx[i]].tz_localize('UTC')
        dt_ranges.append([tBeg, tEnd, tEnd.year])

    # append full range
    dt_ranges.append([trading_dates[0].tz_localize('UTC'),
                      trading_dates[-1].tz_localize('UTC'),
                      'All'])

    for rng in dt_ranges:
        (tBeg, tEnd, year) = rng
        metrics = None
        try:
            metrics = RiskMetricsPeriod(tBeg, tEnd, returns_risk)
            dMetrics = metrics.to_dict()
            dMetrics['year'] = year
            yoy_metrics.append(dMetrics)
        except Exception, e:
            print e
            yoy_metrics.append({'year': year})
            
    return yoy_metrics

def pretty_print(yoy_metrics):

    # these are not verified
    black_list = ['sharpe', 'sortino', 'alpha', 'beta', 'information', 'period_label']
    
    # Make a printable DataFrame from the YOY metrics
    df = pd.DataFrame(yoy_metrics)

    df.drop(black_list, inplace=True, axis=1)
    
    # make long column headers wrap
    new_cols = []
    for col in df.columns.values:
        new_cols.append(col.replace('_', ' '))
    df.columns = new_cols
    return df
    
'''
Compute Zipline metrics for full period.
'''
def compute_full(results, sim_params):
    
    returns_risk = create_returns_from_list([ret for ret in results.returns], sim_params)
    
    # Compute metrics for the entire backtesting period
    riskMetrics = RiskMetricsPeriod(sim_params.first_open, sim_params.last_close, returns_risk)

    # Compute RiskMetricsBase on a rolling basis for several terms.
    riskReport = RiskReport(returns_risk, sim_params)

    return riskReport, riskMetrics
