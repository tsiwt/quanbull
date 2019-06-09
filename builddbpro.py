# -*- coding: utf-8 -*-
import tushare as ts
import sqlite3 as lite
import pandas as pd
from sqlalchemy import create_engine, text


import time
import datetime
import sys


def getallstockcodes_pro(engine):
    ts.set_token('6c6eb6d7fb037c74f999dc78a9bdcca6a961b52fcb06ced3c746ae1f')
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



def read_stock_basic_fromdb(engine ):
    stockbasics = pd.read_sql_table('stock_basic', engine)
    return stockbasics


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


"""
sqlite> .schema daily_data
CREATE TABLE daily_data (
        "index" BIGINT,
        ts_code TEXT,
        trade_date TEXT,
        open FLOAT,
        high FLOAT,
        low FLOAT,
        close FLOAT,
        pre_close FLOAT,
        change FLOAT,
        pct_chg FLOAT,
        vol FLOAT,
        amount FLOAT
);
CREATE INDEX ix_daily_data_index ON daily_data ("index");



sqlite> .schema stock_basic
CREATE TABLE stock_basic (
        "index" BIGINT,
        ts_code TEXT,
        symbol TEXT,
        name TEXT,
        area TEXT,
        industry TEXT,
        list_date TEXT
);
CREATE INDEX ix_stock_basic_index ON stock_basic ("index");

"""
def  read_onestock_to_df(engine, ts_code,daynum):
     querystr=" select * from daily_data where ts_code='000001.SZ' order by trade_date asc limit 800 "
     querystr2=" select * from stock_basic where ts_code='000001.SZ'"

     querystr=querystr.replace("000001.SZ", ts_code)
     querystr=querystr.replace("800", str(daynum))


     #connection = engine.connect()
     querystr2=querystr2.replace("000001.SZ", ts_code)
         
     print querystr
     stockdf = pd.read_sql(querystr, engine )
     return stockdf

    
def  read_onestock_to_df_by_date(engine, ts_code,start_date, end_date):
     querystr=" select * from daily_data where ts_code='000001.SZ' and trade_date >='aaaaaaaa' and trade_date<='bbbbbbbb' order by trade_date asc "
     querystr2=" select * from stock_basic where ts_code='000001.SZ'"

     querystr=querystr.replace("000001.SZ", ts_code)
     querystr=querystr.replace("aaaaaaaa", start_date)
     querystr=querystr.replace("bbbbbbbb", end_date)



     #connection = engine.connect()
     querystr2=querystr2.replace("000001.SZ", ts_code)
         
     print querystr
     stockdf = pd.read_sql(querystr, engine )
     return stockdf





def   readallstocks_todataframe(daynums, dbname):
    string_for_engine = 'sqlite:///'+dbname
    engine = create_engine(string_for_engine)

    stockbasics=read_stock_basic_fromdb(engine )
    now_time = datetime.datetime.now()
    yes_time = now_time + datetime.timedelta(days=-daynums)
    endstr = now_time.strftime('%Y%m%d')
    startstr = yes_time.strftime('%Y%m%d')

    codelist=stockbasics['ts_code']
    for ts_code in codelist:
         stockdf=read_onestock_to_df(engine, ts_code,100)
         print stockdf




def build_db( ):
    dbname = 'C:\\good\\mypanel\\tudfdb.db'
    updateallstocks_bydataframe(4000, dbname)





if __name__ == "__main__":
    dbname = 'C:\\good\\mypanel\\tudfdb.db'
    readallstocks_todataframe(800, dbname)



   
