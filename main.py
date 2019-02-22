# -*- coding: utf-8 -*-
"""
Created on Thu Feb 21 15:06:03 2019

@author: Administrator
"""

import re
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from datetime import timedelta, time
import pymssql
from WindPy import w
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import NVARCHAR, Float, Integer, Unicode
import time as ttime
import talib as ta

# import optionfunction_Wind as ofw
# w.start()

conn = pymssql.connect(
    server='10.0.0.51',  # 不用改
    port=1433,  # 不用改
    user='Seminar',  # 讨论班专用账号
    password='seminar',  # 密码
    database='rawdata',  # 要对哪个数据库进行操作
    charset='utf8'
)
conn2 = pymssql.connect(
    server='10.0.0.51',  # 不用改
    port=1433,  # 不用改
    user='Seminar',  # 讨论班专用账号
    password='seminar',  # 密码
    database='StrategyOutput',  # 要对哪个数据库进行操作
    charset='utf8'
)


def input_SOdata(DBarea, DBname, table_name, df, order='fail', chinesecol=[]):
    """
    DBarea==0,本地数据库 DBarea==1，阿里云数据库
    DBname为数据库名称，字符串格式，可选‘rawdata','StrategyOutput'.....
    table_name为想要保存的数据表名称，字符串格式
    df为DataFrame格式，为想要导入的表名
    order,控制导入方式，取值说明
     'fail'：如果表格存在则报错
     'replace'：替换已经存在表格(rawdata库中小心使用)
     'append'：向已经存在的表格后新增数据
    chinesecol:当表内容(不是列名和表名)含中文时,将中文列列名标出，格式为list，如['银行类型']，或者['银行类型','公司名称']
    """
    if DBarea == 0:
        engine = create_engine('mssql+pymssql://sa:abc123@10.0.0.51:1433/%s' % DBname)  # 数据库类型+驱动://用户名:密码@IP:端口/数据库名称
    elif DBarea == 1 and DBname == 'StrategyOutput':
        engine = create_engine(
            'mssql+pymssql://ruihui:abc123@106.14.118.247:1433/%s' % DBname)  # 数据库类型+驱动://用户名:密码@IP:端口/数据库名称
    else:
        return '请输入正确的DBarea'

    #
    if chinesecol != []:
        typelist = [NVARCHAR(255) for i in chinesecol]
        dtypedict = dict(zip(chinesecol, typelist))
    else:
        dtypedict = {}

    print(dtypedict)

    if order == 'fail':
        tablelist = view_table(DBarea, DBname)
        if table_name in list(tablelist.iloc[:, 0]):
            return '表名重复'
        else:
            df.to_sql(table_name, engine, if_exists=order, chunksize=1000, index=False, dtype=dtypedict)
            return 'succeed!'
    #    elif order=='replace' and DBname.lower()=='rawdata':
    #        print('you cannot replace rawdata')
    #        return 'try to delete it'
    else:
        df.to_sql(table_name, engine, if_exists=order, chunksize=1000, index=False, dtype=dtypedict)
        return 'succeed!'


def view_table(DBarea, DBname='StrategyOutput'):
    """
    DBarea==0,本地数据库 DBarea==1，阿里云数据库
    DBname为数据库名称，字符串格式，可选‘rawdata','StrategyOutput'....
    """
    if DBarea == 0:
        engine = create_engine('mssql+pymssql://sa:abc123@10.0.0.51:1433/%s' % DBname)  # 数据库类型+驱动://用户名:密码@IP:端口/数据库名称
    elif DBarea == 1 and DBname == 'StrategyOutput':
        engine = create_engine(
            'mssql+pymssql://ruihui:abc123@106.14.118.247:1433/%s' % DBname)  # 数据库类型+驱动://用户名:密码@IP:端口/数据库名称
    else:
        return '请输入正确的DBarea'
    sql = "select * from sysobjects  where xtype='U';"
    res = engine.execute(sql);
    table_list = pd.DataFrame(res.fetchall(), columns=res.keys())
    return table_list


def upload_exresult(data, sname, snamecn, exname, exnamecn):
    df = pd.DataFrame(columns=['Date', 'Strategy', 'StrategyName', 'code', 'codename', 'QTY', 'NAV'])
    df['Date'] = data.ddate
    df['Strategy'] = sname
    df['StrategyName'] = snamecn
    df['code'] = exname
    df['codename'] = exnamecn
    df['QTY'] = data['pos']
    df['NAV'] = data['sum_profit']
    input_SOdata(0, 'StrategyOutput', 'FX_strategy_pnl', df, order='append', chinesecol=['StrategyName', 'codename'])
    input_SOdata(1, 'StrategyOutput', 'FX_strategy_pnl', df, order='append', chinesecol=['StrategyName', 'codename'])
    return 0




exlist = pd.read_excel('code2name.xlsx')

