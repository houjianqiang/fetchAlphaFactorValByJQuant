# -*- coding: utf-8 -*-
# @File  : juQuantClientRun.py
# @Author: hoke
# @Date  : 2018/4/16
# @Desc  :
# coding: utf8

import jqdata
# from jqboson.api.settings import set_benchmark, set_option
# from jqboson.core.utils import run_daily
#
#
# def initialize(context):
#     set_benchmark('000300.XSHG')
#     set_option('use_real_price', True)
#     log.info('initialize run only once')
#     run_daily(market_open, time='open', reference_security='000300.XSHG')
#
#
# def market_open(context):
#     # 输出开盘时间
#     log.info('(market_open):' + str(context.current_dt.time()))
#     pass
#
#
# # 下方代码为 IDE 运行必备代码
# if __name__ == '__main__':
#     import jqsdk
#
#     params = {
#         'token': 'e99e7b7ed254352ffe06aa3453dc2432',
#         'algorithmId': 6,
#         'baseCapital': 1000000,
#         'frequency': 'day',
#         'startTime': '2017-3-1',
#         'endTime': '2017-9-1',
#         'name': "Test1",
#     }
#     jqsdk.run(params)
#
# '''
#     token：用户凭证，在系统设置中可以查看到              e99e7b7ed254352ffe06aa3453dc2432
#     algorithmId： 策略ID，我的策略中可以查看到
#     baseCapital：初始资金
#     frequency：运行频率，day/minute
#     startTime：回测开始日期，如’2017-06-01’
#     endTime：回测结束日期，如’2017-08-01’
#     name：回测名称
# '''
