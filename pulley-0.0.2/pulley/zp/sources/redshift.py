import os
import pytz
import numpy as np
import pandas as pd
import psycopg2

from pulley.zp.sources.query_source import QuerySource

from zipline.protocol import DATASOURCE_TYPE

## To get these credentials for database access, see http://goodeanalytics.com/clusters/aws-redshift-clusters/
redshift_user = os.getenv('AWS_REDSHIFT_USER', '')
redshift_pwd = os.getenv('AWS_REDSHIFT_PWD', '')
redshift_ip = os.getenv('AWS_REDSHIFT_IP', '')

EQUITY_BAR_TABLE = 'csi_adj'
SQL_FORMAT = '%Y-%m-%d %H:%M:%S'
PRINT_QUERIES = False

'''
Returns true if credentials are set and false otherwise.
'''
def has_creds():
    if redshift_user != '' and redshift_pwd != '' and redshift_ip != '':
        return True
    return False

'''
Get a PostgreSQL connection to an AWS Redshift DB:
'''
def get_redshift_con():
    return psycopg2.connect("dbname=db1 user=%s password=%s host=%s port=5439" % \
                               (redshift_user, redshift_pwd, redshift_ip))

'''
Get a Zipline DataSource generator with rows fetched from a Python DBAPI.
'''
def get_price_events(rows_prices):    
    return QuerySource(rows_prices, ['dt', 'sid', 'price', 'volume'], DATASOURCE_TYPE.TRADE)

'''
Get a Zipline DataSource generator with rows fetched from a Python DB API.
'''
def get_match_events(rows_matches):
    return QuerySource(rows_matches, ['dt', 'sid', 'event_date'], DATASOURCE_TYPE.CUSTOM)

'''
Execute a query, with optional printing of that query.
'''
def cur_execute(cur, query):
    if PRINT_QUERIES:
        print query
    cur.execute(query)

'''
Check if a PostgreSQL cursor has a table called table_name.
'''
def is_table(cur, table_name):
    cur.execute("select * from information_schema.tables where table_name=%s", (table_name,))
    return bool(cur.rowcount)

'''
Get data to run an algo. Used in pulley.trading exclusively.
'''
def get_data(tickers, tBeg, tEnd, adjusted=True):

    if not has_creds():
        raise Exception('No environment variables set for AWS Redshift.')

    con = get_redshift_con()
    cur = con.cursor()
    
    event_table = 'event_table' # table to copy the query results into
    eastern = pytz.timezone('US/Eastern')
    
    sBeg = tBeg.strftime(SQL_FORMAT)            # for use with yyyy-mm-dd HH-MM-SS' format (DeltaNeutral)
    sEnd = tEnd.strftime(SQL_FORMAT)            #
    sBegDate = tBeg.date().strftime(SQL_FORMAT) # for use with 'yyyy-mm-dd 00:00:00' format (CSI)
    sEndDate = tEnd.date().strftime(SQL_FORMAT) #

    # check if event_table already exists
    if is_table(cur, event_table):
        query_drop = "DROP TABLE %s" % event_table
        cur_execute(cur, query_drop)

    ## Make a table for tickers
    query = 'SELECT * INTO %s FROM (\n' % event_table 
    for i, tkr in enumerate(tickers):
        if i == 0:
            query += "SELECT '%s' as event_sym" % tkr
        else:
            query += "SELECT '%s'" % tkr
        if i != len(tickers) - 1:
            query += ' UNION ALL\n'            
    query += ')'

    cur_execute(cur, query)
    con.commit()

    if adjusted:
        bar_table = 'csi_adj'
    else:
        bar_table = 'csi_unadj'
    
    ##
    ## Equity quote data (open and close prices with corresponding times)
    ##

    # The volu*100 is necessary b/c our the equity quote provider divides volumes by 100.
    # We add 16 hours to the TIMESTAMP b/c this corresponds to the time of lastu
    query = """\
SELECT * FROM (
    SELECT obs_date+interval '16 hour' as dt, symu as sid, lastu as price, volu*100.0 as volume
    FROM %(BAR_TABLE)s
    WHERE symu in (SELECT DISTINCT event_sym from %(UNI_TABLE)s)
    AND obs_date >= '%(T_BEG)s'
    AND obs_date <= '%(T_END)s'

    UNION ALL

    SELECT obs_date+interval '9 hour'+interval '30 minutes' as dt, symu as sid, openu as price, volu*100.0 as volume
    FROM %(BAR_TABLE)s
    WHERE symu in (SELECT DISTINCT event_sym from %(UNI_TABLE)s)
    AND obs_date >= '%(T_BEG)s'
    AND obs_date <= '%(T_END)s' 
)
ORDER BY dt, sid
"""
    query = query % {'BAR_TABLE': bar_table ,
                     'UNI_TABLE': event_table,
                     'T_BEG': sBegDate,
                     'T_END': sEndDate}
    
    cur_execute(cur, query)
    rows_prices = cur.fetchall()

    cur.close()
    con.close()

    return rows_prices

'''
<TODO> Check for usages (I think it's not used anywhere)
'''
def get_bench_source(tBeg, tEnd):

    con = get_redshift_con()
    cur = con.cursor()
    
    eastern = pytz.timezone('US/Eastern')
    
    sBeg = tBeg.strftime(SQL_FORMAT)            # for use with yyyy-mm-dd HH-MM-SS' format (DeltaNeutral)
    sEnd = tEnd.strftime(SQL_FORMAT)            #
    sBegDate = tBeg.date().strftime(SQL_FORMAT) # for use with 'yyyy-mm-dd 00:00:00' format (CSI)
    sEndDate = tEnd.date().strftime(SQL_FORMAT) #

    query = """\
    SELECT obs_date+interval '16 hour' as dt, lastu
    FROM %s
    WHERE symu = 'SPY'
    AND obs_date >= '%s'
    AND obs_date <= '%s'
    ORDER BY dt
    """ % (EQUITY_BAR_TABLE, sBegDate, sEndDate)

    cur_execute(cur, query)
    rows = cur.fetchall()

    bench_price_utc = pd.Series([float(row[1]) for row in rows],
                            index=[eastern.localize(row[0]).astimezone(pytz.utc) for row in rows])

    bench_source = []
    for i, index in enumerate(bench_price_utc.index):
        bench_source.append({'dt': index,
                             'returns': bench_price_utc[i]/bench_price_utc[i-1] - 1.0,
                             'type': DATASOURCE_TYPE.BENCHMARK,
                             'source_id': 'ga_benchmark'})
    cur.close()
    con.close()

    return bench_source, bench_price_utc

'''
Replacement for Zipline's Yahoo dependency. Fetches SPY returns from database.
'''
def get_bench_returns():
    
    con = get_redshift_con()
    cur = con.cursor()

    query = """\
    SELECT obs_date as dt, lastu
    FROM %s
    WHERE symu = 'SPY'
    ORDER BY dt
    """ % (EQUITY_BAR_TABLE)

    cur_execute(cur, query)
    data = cur.fetchall()
    dates = [datum[0] for datum in data]
    del dates[0]
    prices = np.array([float(datum[1]) for datum in data])

    # <NOTE> These are not log returns.
    benchmark_returns = pd.Series(prices[1:]/prices[:-1] - 1.0, index=dates)
    benchmark_returns = benchmark_returns.tz_localize('UTC')

    cur.close()
    con.close()

    return benchmark_returns
