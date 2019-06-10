# -*- coding: utf-8 -*-

import numpy as np
import talib
from builddbpro import *


if __name__ == "__main__":
    dbname = 'C:\\good\\mypanel\\tudfdb2.db'
    string_for_engine = 'sqlite:///' + dbname
    engine = create_engine(string_for_engine)

    stockbasics = read_stock_basic_fromdb(engine)

    codelist = stockbasics['ts_code']
    for ts_code in codelist:
        stockname = stockbasics[stockbasics['ts_code']
                                == ts_code].iloc[0]['name']
        #stockdf=read_onestock_to_df(engine, ts_code,100)
        stockdf = read_onestock_to_df_by_date(
            engine, ts_code, '20190101', '20190610')

        close = [float(x) for x in stockdf['close']]

        #print stockdf
        try:
            stockdf['RSI'] = talib.RSI(np.array(close), timeperiod=12)
            stockdf['MA10_talib'] = talib.MA(np.array(close), timeperiod=10)

            # trsilist=stockdf[stockdf['trade_date']=='20190606']['RSI']
            trsi = stockdf[stockdf['trade_date'] == '20190606']['RSI'].iloc[-1]
            # trsilist=stockdf[stockdf['trade_date']=='20190606']['RSI']
            #print trsilist
            # trsi=trsilist[-1]
            print trsi
        except BaseException:

            continue

        if(trsi < 30):
            print "RSI %d %s %s\n" % (trsi, ts_code, stockname)
