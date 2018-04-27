# # # -*- coding: utf-8 -*-
# # # @File  : test.py
# # # @Author: hoke
# # # @Date  : 2018/4/16
# # # @Desc  :
# # # 导入函数库
# # '''
# # import time
# # import datetime
# # # 导入 mysql 连接信息
# # import pymysql
# # import jqdatasdk as jq
# # try:
# #     jq.auth("18675594612", "Cmfchina123")
# #
# #     jq.get_price("000001.XSHE")
# # except Exception as e:
# #     print(e)
# #
# #
# # host = '192.168.8.178'
# # port = 3306
# # user = 'root'
# # passwd = '6wKAbnj4'
# # db = 'test_factor'
# # charset = 'utf8'
# #
# #
# # # 创建连接
# # def create_conn():
# #     conn = pymysql.connect(host=host, port=port, user=user, passwd=passwd, db=db, charset='utf8')
# #     return conn
# #
# #
# # # 创建游标
# # def create_cursor(conn):
# #     cursor = conn.cursor()
# #     return cursor
# #
# #
# # def find_one():
# #     conn = create_conn()
# #     cursor = create_cursor(conn)
# #
# #     cursor.execute("select * from factor_values")
# #     row_1 = cursor.fetchone()
# #     print(row_1)
# #
# #     conn.commit()
# #     cursor.close()
# #     conn.close()
# #
# #
# # def save_data(data):
# #     pass
# #
# #
# # # 获得时间范围
# # def getBetweenDay(begin_date):
# #     date_list = []
# #     begin_date = datetime.datetime.strptime(begin_date, "%Y-%m-%d")
# #     end_date = datetime.datetime.strptime(time.strftime('%Y-%m-%d', time.localtime(time.time())), "%Y-%m-%d")
# #     while begin_date <= end_date:
# #         date_str = begin_date.strftime("%Y-%m-%d")
# #         date_list.append(date_str)
# #         begin_date += datetime.timedelta(days=1)
# #     return date_list
# #
# #
# # days_range = getBetweenDay("2017-09-09")
# # print(len(days_range))
# # '''
# # '''
# # '''
# # N = 10 ** 7
# #
# #
# # async def agen():
# #     for i in range(N):
# #         yield i
# #
# #
# # class AIter:
# #     def __init__(self):
# #         self.i = 0
# #
# #     def __aiter__(self):
# #         return self
# #
# #     async def __anext__(self):
# #         i = self.i
# #         if i >= N:
# #             raise StopAsyncIteration
# #         self.i += 1
# #         return i
# #
# #
# # i = agen()
# #
# #
# # async def g1():
# #     yield 1
# #     yield 2
# #
# #
# # async def g2():
# #     async for v in g1():
# #         yield v
# #
# #
# # # @async_context_manager
# # # async def ctx():
# # #     await open()
# # #     try:
# # #         yield
# # #     finally:
# # #         await close()
# # #
# # # async with ctx():
# # #     await ...
# #
# # '''
# # Example: print numbers from 0 to 9 with one second delay
# # '''
# # # import asyncio
# # # async def ticker(delay,to):
# # #     for i in range(to):
# # #         yield i
# # #         await  asyncio.sleep(delay)
# # #
# # # async def run():
# # #     async for i in ticker(1,10):
# # #         print(i)
# # #
# # # loop = asyncio.get_event_loop()
# # # try:
# # #     loop.run_until_complete(run())
# # # finally:
# # #     loop.close()
# #
# # # import asyncio
# # #
# # # @asyncio.coroutine
# # # def hello():
# # #     print("Hello world ")
# # #     r = yield from asyncio.sleep(1)
# # #     print("Hello again!")
# # #
# # # loop = asyncio.get_event_loop()
# # # loop.run_until_complete(hello())
# # # loop.close()
# #
# # # import asyncio
# # # @asyncio.coroutine
# # # def wget(host):
# # #     print('wget %s...'% host)
# # #     connect = asyncio.open_connection(host,80)
# # #     reader ,writer = yield from connect
# # #     header = 'GET / HTTP/1.0\r\nHost: %s \r\n\r\n' % host
# # #     writer.write(header.encode('utf-8'))
# # #     yield from writer.drain()
# # #     while True:
# # #         line = yield from reader.readline()
# # #         if line == b'\r\n':
# # #             break
# # #         print('%s header > %s'%(host,line.decode('utf-8').rstrip()))
# # #     writer.close()
# # #
# # # loop = asyncio.get_event_loop()
# # # tasks = [wget(host) for host in ['www.sina.com','www.sohu.com','www.163.com']]
# # # loop.run_until_complete(asyncio.wait(tasks))
# # # loop.close()
# #
# # # @prettyprint.register(Set)
# #
# #
# # import pandas
# # from pandas import DataFrame, Series
# # import pandas as pd
# #
# # df = DataFrame(
# #     {'stock_code': ['001', '002'], 'start_date': ['1991/4/3', '1999/4/5'], 'end_date': ['2200/1/1', '2200/1/1']}, \
# #     index=['a', 'b'])
# # from pprint import pprint
# #
# # # pprint(df)
# # '''
# # df 的数据结构
# #    end_date start_date stock_code
# # a  2200/1/1   1991/4/3        001
# # b  2200/1/1   1991/4/5        002
# # '''
# # from datetime import datetime
# #
# #
# # def str_to_date(strs):
# #     return datetime.strptime(strs, "%Y/%m/%d")
# #
# #
# # def date_to_str(date_time):
# #     return date_time.strftime("%Y-%m-%d")
# #
# #
# # def test01(df):
# #     for ind, row in df.iterrows():
# #         print(ind, type(ind))
# #         print(row, type(row))
# #
# #
# # # test01(df)
# # bt = str_to_date('1992/01/01')
# # et = str_to_date('2019/01/01')
# # wei = pd.to_datetime(df['start_date'])
# #
# # # df[map(lambda x:datetime.date(x.year-1,x.month,x.day),df['report_date'])==data['date_1y_ago']]
# # df[['start_date', 'end_date']] = df[['start_date', 'end_date']].apply(pd.to_datetime)
# # df = df[df.start_date > bt]
# # df = df[df.end_date > et]
# # print(df)
# #
# #
# # # pprint(df['stock_code'][(df.start_date < '1992-01-01' < df.end_date)])
# # def deal_(df, bt):
# #     data = df
# #     for i in df.values:
# #         print("*" * 10, i[-1], "*" * 10)
# #         start_date = str_to_date(i[1])
# #         end_date = str_to_date(i[0])
# #         print("*" * 10, i[-1], "*" * 10)
# #         #########################
# #         # 对股票代码判断，上市时间start_date < code['start_date'],start_date > code['end_date']
# #         if not (start_date < bt < end_date):
# #             pass
# #
# #     return start_date, end_date
# #
# #
# # if __name__ == '__main__':
# #     # bt = str_to_date('1992/01/01')
# #     # et = str_to_date('2019/01/01')
# #     # start_date, end_date = deal_(df,bt)
# #     # print(start_date, end_date)
# #     pass
# from run03 import *
#
# # try:
# #     jq.auth("18675594612", "Cmfchina123")
# #     # jq.auth("18845210680", "xa255389")
# #
# #     jq.get_price("000001.XSHE")
# # except Exception as e:
# #     print(e)
# # date_list = getBetweenDay("2015-04-17",'2018-04-13')
# # print(len(date_list))
# import numpy as np
# dlis =[]
# sum = 0
# while sum <= 731:
#     dlis.append(sum)
#     sum+=1
#
# count = 0
# for i in range(0,731,73):
#     print(i,i+73)
#     count +=1
#
# print(count)
#
#
# li = pd.read_csv("D:\\df.csv")
# print(li)
# print(li['code'])
# print(li['code'].tolist())
#
#
# from jqdatasdk.alpha191 import  *
#
# alpha_003
#
# import threading
# threading.Thread

#
# from multiprocessing import Process
# import threading
# import time
# def fire_on(*args):
#     print("*"*50,args[0],"*"*50)
#     time.sleep(5)
#     print(time.time())
#     print("1231213123123")
#     print("*"*50)
#     print()
#
# def run_thread_(*args):
#     time.sleep(2)
#     print("hello %s"%args[0])
#     t = threading.Thread(target=fire_on,args=args)
#
#
# Pool = []
# for i in range(10):
#     p = Process(target=run_thread_,args=("Progress %s"%i,))
#     Pool.append(p)
#     p.start()

import multiprocessing
import time, threading
import numpy as np


def create_thread(time_list,thread_list):
    for task in time_list:
        th = threading.Thread(target=cc_alpha191_data, args=([task, ]))
        th.start()
        thread_list.append(th)
    


if __name__ == '__main__':
    
    date_list = np.loadtxt("daterange.npy", dtype=str, delimiter=',').tolist()
    try:
        for dl in range(0, 731, 100):
            process_list = []
            thread_list = []
            if dl >= 700:
                per_task = date_list[dl:731]
            else:
                per_task = date_list[dl:dl + 100]
            if len(per_task):
                extra = len(per_task) % 4
                per_v = len(per_task) // 4
                p1 = multiprocessing.Process(target=create_thread, args=(per_task[0:per_v],thread_list))
                p2 = multiprocessing.Process(target=create_thread, args=(per_task[per_v:2*per_v],thread_list))
                p3 = multiprocessing.Process(target=create_thread, args=(per_task[2*per_v:3*per_v],thread_list))
                p4 = multiprocessing.Process(target=create_thread, args=(per_task[3*per_v:4*per_v+extra],thread_list))
                process_list = [p1,p2,p3,p4]
                for ptt in process_list:
                    ptt.start()
                for thr in thread_list:
                    thr.join()
                for pp in process_list:
                    pp.join()
    except Exception as e:
        print(e)
    
    print("end this")
    while True:
        try:
            for i in range(0, 731, 100):
                thread_list = []
                if i >= 700:
                    for task in date_list[i:731]:
                        th = threading.Thread(target=cc_alpha191_data, args=([task, ],))
                        th.start()
                        thread_list.append(th)
                else:
                    for task in date_list[i:i + 100]:
                        th = threading.Thread(target=cc_alpha191_data, args=([task, ],))
                        th.start()
                        thread_list.append(th)
                for t in thread_list:
                    t.join()
        except Exception as e:
            print(e)
        break
    print("end this")

# import pandas as pd
#
# index = [0.236667,
# 0.083333,
# -0.123333,
# 0.130000,
# 0.346667,
# -0.126667,
# 0.313333,
# 0.280000,
# -0.033333,
# -0.036667,
# -0.130000,
# -0.013333,
# -0.133333,
# 0.470000,
# -0.13666,]
#
#
#
# values = ['00002.XSHE ',
# '000008.XSHE',
# '000009.XSHE',
# '000027.XSHE',
# '000039.XSHE',
# '000060.XSHE',
# '000061.XSHE',
# '000063.XSHE',
# '000069.XSHE',
# '000100.XSHE',
# '000156.XSHE',
# '000157.XSHE',
# '000166.XSHE',
# '000333.XSHE',
# '000338.XSHE',]
#
#
# ss = pd.Series(values,index=index)
# print(ss)
#
# # def gen(series):
#     for i in range():

str = "ACCER(security_list, check_date, N = 8)"

print(str.split("(")[0])
