# -- coding: utf8 -
import threading
import jqdatasdk
from jqdatasdk.alpha101 import *
from models import model
from log.log import Logger

logger = Logger(logname='log.txt', loglevel=1, logger="fox").getlog()

# 用来跑 alpha101因子数据的


lis = ['alpha_001',
       'alpha_002',
       'alpha_003',
       'alpha_004',
       'alpha_005',
       'alpha_006',
       'alpha_007',
       'alpha_008',
       'alpha_009',
       'alpha_010',
       'alpha_011',
       'alpha_012',
       'alpha_013',
       'alpha_014',
       'alpha_015',
       'alpha_016',
       'alpha_017',
       'alpha_018',
       'alpha_019',
       'alpha_020',
       'alpha_021',
       'alpha_022',
       'alpha_023',
       'alpha_024',
       'alpha_025',
       'alpha_026',
       'alpha_027',
       'alpha_028',
       'alpha_029',
       'alpha_031',
       'alpha_032',
       'alpha_033',
       'alpha_034',
       'alpha_035',
       'alpha_036',
       'alpha_037',
       'alpha_038',
       'alpha_039',
       'alpha_040',
       'alpha_041',
       'alpha_042',
       'alpha_043',
       'alpha_044',
       'alpha_045',
       'alpha_046',
       'alpha_047',
       'alpha_049',
       'alpha_050',
       'alpha_051',
       'alpha_052',
       'alpha_053',
       'alpha_054',
       'alpha_055',
       'alpha_056',
       'alpha_057',
       'alpha_060',
       'alpha_061',
       'alpha_062',
       'alpha_064',
       'alpha_065',
       'alpha_066',
       'alpha_068',
       'alpha_071',
       'alpha_072',
       'alpha_073',
       'alpha_074',
       'alpha_075',
       'alpha_077',
       'alpha_078',
       'alpha_083',
       'alpha_084',
       'alpha_085',
       'alpha_086',
       'alpha_088',
       'alpha_092',
       'alpha_094',
       'alpha_095',
       'alpha_096',
       'alpha_098',
       'alpha_099',
       'alpha_101', ]

from models.model import get_data_base1, deal_code


def cc_alpha101_data(datelist):
    logger.info(msg=u'进入接口>>cc_alpha101_data')
    days_range = datelist
    for day in days_range:
        end_date = day
        try:
            for method in lis:
                logger.info(u"进行jquant认证")
                try:
                    jqdatasdk.auth("18675594612", "Cmfchina123")
                    # jqdatasdk.auth("18845210680", "xa255389")
                except Exception as e:
                    logger.error(msg=u'发生异常>>>>%s' % e)
                logger.info(msg=u"开始执行alpha101因子方法:{},计算时间为:{},index:{}" .format(method,end_date,u'all') )
                exc_str = method + r"('%s','%s')" % (end_date, 'all')

                data = eval(exc_str)
                logger.info(u"alpha101因子方法:{}执行成功,参数为：计算时间为:{},index:{}".format(method,end_date,u'all'))
                if not data.empty:
                    # # data : 股票代码  aplpha factor_values ,还需要date，origin，factor_name,
                    factor_origin = r"alpha101"
                    factor_name = method
                    logger.info(
                        msg="开始数据插入操作，参数：数据集:data,因子来源:{},因子名称:{},计算日期{}".format(factor_origin, factor_name, end_date))
                    model.bulk_insert_series(data, factor_origin, factor_name, end_date)
        except Exception as e:
            logger.error(msg=u'发生异常>>>>%s' % e)
        try:
            logger.info(msg="开始记录完成天数，参数：计算日期{},status：{}".format(end_date, status=0))
            model.mark_record(end_date, status=0)
        except Exception as e:
            logger.error(msg=u'发生异常>>>>%s' % e)


import numpy as np

process_list = []
thread_list = []


def open_single_thread(*args):
    th = threading.Thread(target=cc_alpha101_data, args=args)
    thread_list.append(th)
    th.start()


def verify_stop_status(date_list, *args):
    # 首先查records_status表，查询已经完成的数据

    recorded_date_list = model.find_record(args)
    condition = lambda t: t not in recorded_date_list
    date_list = list(filter(condition, date_list))
    return date_list


import multiprocessing

if __name__ == '__main__':
    logger.info(msg=u"》》》》》开始进程《《《《《")
    date_list = np.loadtxt("daterange.npy", dtype=str, delimiter=',').tolist()
    # 筛掉完成的数据
    try:
        date_list = verify_stop_status(date_list, 0)
        # 从未完成的方法开始调用

        model.find_nofinish(lis, 'alpha101')
    except Exception as e:
        logger.error(msg=u'发生异常>>>>%s' % e)

    # cc_alpha101_data(date_list)
    len_datelist = len(date_list)
    per = len_datelist // 100
    try:
        logger.info(msg=u"》》》》》》开始多线程《《《《《《")
        logger.info(u"开始执行循环完成指定时间区间的alpha因子值计算")
        for i in range(0, len_datelist, 100):
            thread_list = []
            if i >= 100 * per:
                logger.info(u"时间范围为{}-{}".format(date_list[i], date_list[len_datelist]))
                for task in date_list[i:len_datelist]:
                    th = threading.Thread(target=cc_alpha101_data, args=([task, ],))
                    th.start()
                    thread_list.append(th)

            else:
                logger.info(u"时间范围为{}-{}".format(date_list[i], date_list[i + 100]))
                for task in date_list[i:i + 100]:
                    th = threading.Thread(target=cc_alpha101_data, args=([task, ],))
                    th.start()
                    thread_list.append(th)
            for t in thread_list:
                t.join()
                logger.info(msg=u"线程{}完成".format(t.getName()))
            logger.info(u"当前100条线程执行完成")

    except Exception as e:
        logger.error(msg=u'发生异常>>>>%s' % e)
    logger.info(u"完成任务")
    # 多进程访问
# import multiprocessing
# import time, threading
# import numpy as np
#
#
# def create_thread(time_list, thread_list):
#     for task in time_list:
#         th = threading.Thread(target=cc_alpha191_data, args=([task, ]))
#         th.start()
#         thread_list.append(th)
#
#
# if __name__ == '__main__':
#
#     date_list = np.loadtxt("daterange.npy", dtype=str, delimiter=',').tolist()
#     while True:
#         try:
#             for dl in range(0, 731, 100):
#                 process_list = []
#                 thread_list = []
#                 if dl >= 700:
#                     per_task = date_list[dl:731]
#                 else:
#                     per_task = date_list[dl:dl + 100]
#                 if len(per_task):
#                     extra = len(per_task) % 4
#                     per_v = len(per_task) // 4
#                     p1 = multiprocessing.Process(target=create_thread, args=(per_task[0:per_v], thread_list))
#                     p2 = multiprocessing.Process(target=create_thread, args=(per_task[per_v:2 * per_v], thread_list))
#                     p3 = multiprocessing.Process(target=create_thread, args=(per_task[2 * per_v:3 * per_v], thread_list))
#                     p4 = multiprocessing.Process(target=create_thread,
#                                                  args=(per_task[3 * per_v:4 * per_v + extra], thread_list))
#                     process_list = [p1, p2, p3, p4]
#                     for ptt in process_list:
#                         ptt.start()
#                     for thr in thread_list:
#                         thr.join()
#                     for pp in process_list:
#                         pp.join()
#         except Exception as e:
#             print(e)
#         break
#     print("end this")

# if __name__ == '__main__':
#     try:
#         jqdatasdk.auth("18675594612", "Cmfchina123")
#         # jqdatasdk.auth("18845210680", "xa255389")
#     except Exception as e:
#         print(e)
#     method = 'alpha_001'
#     end_date = '2018-04-04'
#     alpha_001
#     exc_str = method + "('%s','000300.XSHG')" % end_date
#     data = eval(exc_str)
#     print(data)
