# -*- coding: utf-8 -*-
import tushare as ts
import sqlite3 as lite
from sqlalchemy import create_engine

import time
import datetime
import sys


def getallstockcodes_pro(engine):
    ts.set_token('0000000000000000000000000000000000000000000000000000')

    pro = ts.pro_api()
    data = pro.query('stock_basic', exchange='', list_status='L',
                     fields='ts_code,symbol,name,area,industry,list_date')
    data.to_sql('stock_basic', engine, if_exists='replace')

    lista = data.ts_code
    listb = data.symbol
    length = len(lista)
    mapdict = {}
    for j in range(0, length):
        mapdict[lista[j]] = listb[j]

    return lista, mapdict


def updateallstocks_bydataframe(daynums, dbname):
    string_for_engine = 'sqlite:///'+dbname
    engine = create_engine(string_for_engine)

    for i in range(0, 10):
        try:
            codelist, mapdict = getallstockcodes_pro(engine)
        except Exception as e:
            print(e)

    now_time = datetime.datetime.now()
    yes_time = now_time + datetime.timedelta(days=-daynums)
    endstr = now_time.strftime('%Y%m%d')
    startstr = yes_time.strftime('%Y%m%d')
    stocknum = 0

    for item in codelist:
        try:
            print "stocknum=%d code=%s" % (stocknum, item)
            pro = ts.pro_api()
            df = pro.daily(ts_code=item, start_date=startstr, end_date=endstr)

            # 存入数据库
            # df.to_sql('tick_data',engine)

            # 追加数据到现有表
            df.to_sql('daily_data', engine, if_exists='append')
            stocknum += 1

        except Exception as e:
            print "save to db error"
            print(e)


if __name__ == "__main__":

    dbname = 'C:\\good\\mypanel\\tudfdb.db'
    updateallstocks_bydataframe(4000, dbname)
