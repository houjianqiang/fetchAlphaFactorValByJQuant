# -*- coding: utf-8 -*-
# @File  : run_tech_analysis.py
# @Author: hoke
# @Date  : 2018/4/19
# @Desc  : 获取技术分析因子数据
import threading
import time
from datetime import datetime, timedelta
import pandas as pd
from pandas import DataFrame
import pymysql
import jqdatasdk as jq
from six import StringIO
import json
# from jqlib import technical_analysis

'''
用来获取 技术分析因子数据的
'''
host = '192.168.8.178'
port = 3306
user = 'root'
passwd = '6wKAbnj4'
db = 'test_factor'
charset = 'utf8'

exc_str_list = ['ACCER(security_list, check_date, N = 8)',
                'ADTM(security_list, check_date, N = 23, M = 8)',
                'ATR(security_list, check_date, timeperiod=14)',
                'BIAS(security_list,check_date, N1=6, N2=12, N3=24)',
                'BIAS_QL(security_list, check_date, N = 6, M = 6)',
                'BIAS_36(security_list, check_date, M = 6)',
                'CCI(security_list, check_date, N=14)',
                'CYF(security_list, check_date, N = 21)',
                'DKX(security_list, check_date, M = 10)',
                'KD(security_list, check_date, N = 9, M1 = 3, M2 = 3)',
                'KDJ(security_list, check_date, N =9, M1=3, M2=3) ',
                'SKDJ(security_list, check_date, N = 9, M = 3)',
                'MFI(security_list, check_date, timeperiod=14)',
                'MTM(security_list, check_date, timeperiod=14)',
                'ROC(security_list, check_date, timeperiod=12)',
                'RSI(security_list, check_date, N1=6)',
                'MARSI(security_list, check_date, M1 = 10, M2 = 6)',
                'OSC(security_list, check_date, N = 20, M = 6)',
                'UDL(security_list, check_date, N1 = 3, N2 = 5, N3 = 10, N4 = 20, M = 6)',
                'WR(security_list, check_date, N = 10, N1 = 6)',
                'LWR(security_list, check_date, N = 9, M1 = 3, M2 = 3)',
                'TAPI(\'000300.XSHE\', security_list, check_date, M=6)',
                'FSL(security_list, check_date)',
                'CHO(security_list, check_date, N1 = 10, N2 = 20, M = 6)',
                'CYE(security_list, check_date)',
                'DBQR(\'000300.XSHE\', security_list, check_date, N = 5, M1 = 10, M2 = 20, M3 = 60)',
                'DMA(security_list, check_date, N1 = 10, N2 = 50, M = 10)',
                'DMI(security_list, check_date, N=14,  MM = 6):',
                'DPO(security_list, check_date, N=20,  M = 6):',
                'EMV(security_list, check_date, N = 14, M = 9)',
                'GDX(security_list, check_date, N = 30, M = 9)',
                'JLHB(security_list, check_date, N = 7, M = 5)',
                'JS(security_list, check_date, N = 5, M1 = 5, M2 = 10, M3 = 20)',
                'MACD(security_list, check_date, SHORT = 12, LONG = 26, MID = 9)',
                'QACD(security_list, check_date, N1 = 12, N2 = 26, M = 9)',
                'QR(\'000300.XSHE\', security_list, check_date, N = 21)',
                'TRIX(security_list, check_date, N = 12, M = 9)',
                'UOS(security_list, check_date, N1 = 7, N2 = 14, N3 = 28, M = 6)',
                'VMACD(security_list, check_date, SHORT = 12, LONG = 26, MID = 9)',
                'VPT(security_list, check_date, N = 51, M = 6)',
                'WVAD(security_list, check_date, N = 24, M = 6)',
                'BRAR(security_list, check_date, N=26)',
                'CR(security_list, check_date, N=26, M1=10, M2=20, M3=40, M4=62)',
                'CYR(security_list, check_date, N = 13, M = 5)',
                'MASS(security_list, check_date, N1=9, N2=25, M=6)',
                'PCNT(security_list, check_date, M = 5)',
                'PSY(security_list, check_date, timeperiod=12)',
                'VR(security_list, check_date, N=26, M=6)',
                'AMO(security_list, check_date, M1 = 5, M2 = 10)',
                'CCL(futures_list, check_date, M=5)',
                'DBLB(\'000300.XSHE\', security_list, check_date, N=5, M=5)',
                'DBQRV(\'000300.XSHE\', security_list, check_date, N = 5)',
                'HSL(security_list, check_date, N = 5)',
                'OBV(security_list, check_date, timeperiod=30)',
                'VOL(security_list, check_date, M1=5, M2=10)',
                'VRSI(security_list, check_date, N1=6, N2=12, N3=24)',
                'AMV(security_list, check_date, timeperiod = 13)',
                'ALLIGAT(security_list, check_date, timeperiod = 21)',
                'BBI(security_list, check_date, timeperiod1=3, timeperiod2=6, timeperiod3=12, timeperiod4=24)',
                'EXPMA(security_list, check_date, timeperiod = 12)',
                'BBIBOLL(security_list, check_date, N = 11, M = 6)',
                'MA(security_list, check_date, timeperiod=5)',
                'HMA(security_list, check_date, timeperiod = 12)',
                'LMA(security_list, check_date, timeperiod = 12)',
                'VMA(security_list, check_date, timeperiod = 12)',
                'Bollinger_Bands(security_list, check_date, timeperiod=20, nbdevup=2, nbdevdn=2)',
                'ENE(security_list,check_date,N=25,M1=6,M2=6):',
                'MIKE(security_list, check_date, timeperiod = 10)',
                'PBX(security_list, check_date, timeperiod = 9):',
                'XS(security_list, check_date, timeperiod = 13)',
                'XS2(security_list, check_date, N = 102, M = 7)',
                'EMA(security_list, check_date, timeperiod=30)',
                'SMA(security_list, check_date, N = 7, M = 1)',
                'BDZX(security_list, check_date, timeperiod = 40)',
                'CDP_STD(security_list, check_date, timeperiod = 2)',
                'CJDX(security_list, check_date, timeperiod = 16)',
                'CYHT(security_list, check_date, timeperiod = 60)',
                'JAX(security_list, check_date, timeperiod = 30)',
                'JFZX(security_list, check_date, timeperiod = 30)',
                'JYJL(security_list, check_date, N = 120, M = 5)',
                'LHXJ(security_list, check_date, timeperiod = 100)',
                'LYJH(security_list, check_date, M = 80, M1 = 50)',
                'TBP_STD(security_list, check_date, timeperiod=30)',
                'ZBCD(security_list, check_date, timeperiod = 10)',
                'SG_SMX(\'000300.XSHE\', security_list, check_date, N = 50)',
                'SG_LB(\'000300.XSHE\', security_list, check_date)',
                'SG_PF(\'000300.XSHE\', security_list, check_date)',
                'XDT(\'000300.XSHE\',security_list, check_date, P1 = 5, P2 = 10)',
                'ZLMM(security_list, check_date)',
                'RAD(\'000300.XSHE\', security_list, check_date, D=3, S=30, M=30)',
                'SHT(security_list, check_date, N=5)',
                'CYW(security_list, check_date)',
                'CYS(security_list, check_date)',
                'AROON(security_list, check_date, N = 25)',
                'CFJT(security_list, check_date, MM = 200)',
                'ZSDB(\'000300.XSHE\', check_date)',
                'ZX(security_list, check_date)',
                'PUCU(security_list, check_date, N=24)', ]


def get_data_base1():
    # li = jq.get_all_securities(types=['stock'])
    li = pd.read_csv("D:\\df.csv")
    return li


def deal_code(df, e_time):
    bt = pd.Timestamp(e_time)
    df[['start_date', 'end_date']] = df[['start_date', 'end_date']].apply(pd.to_datetime)
    df = df[df.start_date < bt]
    if not df.empty:
        df = df[df.end_date > bt]
    return df


def bulk_insert_series(data, factor_origin, factor_name, record_date):
    conn = pymysql.connect(host=host, port=port, user=user, passwd=passwd, db=db, charset='utf8')
    cur = pymysql.cursors.Cursor(conn)
    for it in data.items():
        
        stock_code = it[0]
        factor_values = it[1]
        
        exc_str = "insert into factor_values (record_date, stock_code, factor_origin, factor_name, facotr_values) VALUES('%s','%s','%s','%s',%f)" % (
            record_date, stock_code, factor_origin, factor_name, factor_values)
        
        cur.execute(exc_str)
    
    conn.commit()
    cur.close()
    conn.close()


def cc_alpha191_data(datelist):
    try:
        jq.auth("18675594612", "Cmfchina123")
        # jq.auth("18845210680", "xa255389")
    except Exception as e:
        print(e)
    days_range = datelist
    code = get_data_base1()
    for day in days_range:
        check_date = day
        code = deal_code(code, check_date)
        if not code.empty:
            security_list = code['code'].tolist()
            try:
                for method in exc_str_list:
                    exc_str = method
                    data = eval(exc_str)
                    if data:
                        # # data : 股票代码  aplpha factor_values ,还需要date，origin，factor_name,
                        factor_origin = "alpha101"
                        factor_name = exc_str.split("(")[0]
                        record_date = str(check_date)
                
                        bulk_insert_series(data, factor_origin, factor_name, record_date)
            except Exception as e:
                print(e)


import numpy as np

import multiprocessing

if __name__ == '__main__':
    
    date_list = np.loadtxt("daterange.npy", dtype=str, delimiter=',').tolist()
    try:
        thread_list = []
        for i in range(0, 731, 3):
            if i >= 729:
                th = threading.Thread(target=cc_alpha191_data, args=(date_list[i:731],))
                th.start()
                thread_list.append(th)
            else:
                th = threading.Thread(target=cc_alpha191_data, args=(date_list[i:i + 73],))
                th.start()
                thread_list.append(th)
        for t in thread_list:
            t.join()
    except Exception as e:
        print(e)
    print("end this")
