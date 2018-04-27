# coding=utf-8
import threading
import pandas as pd
import jqdatasdk as jq
from jqdatasdk.alpha191 import *
from models.model import deal_code
from models import model


def get_data_base1():
    li = pd.read_csv("df.csv")
    return li


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
       'alpha_048',
       'alpha_049',
       'alpha_050',
       'alpha_051',
       'alpha_052',
       'alpha_053',
       'alpha_054',
       'alpha_055',
       'alpha_056',
       'alpha_057',
       'alpha_058',
       'alpha_059',
       'alpha_060',
       'alpha_061',
       'alpha_062',
       'alpha_063',
       'alpha_064',
       'alpha_065',
       'alpha_066',
       'alpha_067',
       'alpha_068',
       'alpha_069',
       'alpha_070',
       'alpha_071',
       'alpha_072',
       'alpha_073',
       'alpha_074',
       'alpha_075',
       'alpha_076',
       'alpha_077',
       'alpha_078',
       'alpha_079',
       'alpha_080',
       'alpha_081',
       'alpha_082',
       'alpha_083',
       'alpha_084',
       'alpha_085',
       'alpha_086',
       'alpha_087',
       'alpha_088',
       'alpha_089',
       'alpha_090',
       'alpha_091',
       'alpha_092',
       'alpha_093',
       'alpha_094',
       'alpha_095',
       'alpha_096',
       'alpha_097',
       'alpha_098',
       'alpha_099',
       'alpha_100',
       'alpha_101',
       'alpha_102',
       'alpha_103',
       'alpha_104',
       'alpha_105',
       'alpha_106',
       'alpha_107',
       'alpha_108',
       'alpha_109',
       'alpha_110',
       'alpha_111',
       'alpha_112',
       'alpha_113',
       'alpha_114',
       'alpha_115',
       'alpha_116',
       'alpha_117',
       'alpha_118',
       'alpha_119',
       'alpha_120',
       'alpha_121',
       'alpha_122',
       'alpha_123',
       'alpha_124',
       'alpha_125',
       'alpha_126',
       'alpha_127',
       'alpha_128',
       'alpha_129',
       'alpha_130',
       'alpha_131',
       'alpha_132',
       'alpha_133',
       'alpha_134',
       'alpha_135',
       'alpha_136',
       'alpha_137',
       'alpha_138',
       'alpha_139',
       'alpha_140',
       'alpha_141',
       'alpha_142',
       'alpha_143',
       'alpha_144',
       'alpha_145',
       'alpha_146',
       'alpha_147',
       'alpha_148',
       'alpha_149',
       'alpha_150',
       'alpha_151',
       'alpha_152',
       'alpha_153',
       'alpha_154',
       'alpha_155',
       'alpha_156',
       'alpha_157',
       'alpha_158',
       'alpha_159',
       'alpha_160',
       'alpha_161',
       'alpha_162',
       'alpha_163',
       'alpha_164',
       'alpha_165',
       'alpha_166',
       'alpha_167',
       'alpha_168',
       'alpha_169',
       'alpha_170',
       'alpha_171',
       'alpha_172',
       'alpha_173',
       'alpha_174',
       'alpha_175',
       'alpha_176',
       'alpha_177',
       'alpha_178',
       'alpha_179',
       'alpha_180',
       'alpha_181',
       'alpha_182',
       'alpha_183',
       'alpha_184',
       'alpha_185',
       'alpha_186',
       'alpha_187',
       'alpha_188',
       'alpha_189',
       'alpha_190',
       'alpha_191', ]


def cc_alpha191_data(datelist):
    try:
        # pass
        jq.auth("18675594612", "Cmfchina123")
        # jq.auth("18845210680", "xa255389")
    except Exception as e:
        print(e)
    days_range = datelist
    code = get_data_base1()
    for day in days_range:
        end_date = day

        code = deal_code(code, end_date)

    if not code.empty:
        code = code['code']
        code = code.tolist()
        for d in code:

            o_code = list((d,))
            try:
                for method in lis:
                    # print(lis)
                    data = None
                    exc_str = method + '(o_code, "%s")' % (end_date)
                    if method in ['alpha_075', 'alpha_149', 'alpha_181', 'alpha_182']:
                        exc_str = method + "(o_code,'000300.XSHG','%s')" % (end_date)
                    data = eval(exc_str)
                    if not data.empty:
                        # print(data.values)
                        # data : 股票代码  aplpha factor_values ,还需要date，origin，factor_name,
                        stock_code = d
                        factor_values = data.values[0]
                        factor_origin = "alpha191"
                        factor_name = method
                        record_date = str(end_date)
                        dict_ = {
                            "stock_code": stock_code,
                            "factor_values": factor_values,
                            "record_date": record_date,
                            "factor_origin": factor_origin,
                            "factor_name": factor_name,
                        }

                        model.bulk_insert(dict_)
            except Exception as e:
                print(e)
        try:
            model.mark_record(record_date, status=1)
        except Exception as e:
            print(e)


def verify_stop_status(date_list, status):
    # 首先查records_status表，查询已经完成的数据
    recorded_date_list = model.find_record(status)
    condition = lambda t: t not in recorded_date_list
    date_list = list(filter(condition, date_list))
    return date_list


import numpy as np

if __name__ == '__main__':
    jq.auth("18675594612", "Cmfchina123")
    date_list = jq.get_all_trade_days()[-20:]
    len_datelist = len(date_list)
    per = len_datelist // 100
    print("多线程开始")
    try:
        thread_list = []
        for i in range(30):
            th = threading.Thread(target=cc_alpha191_data, args=([date_list[i], ],))
            th.start()
            thread_list.append(th)
        for t in thread_list:
            t.join()

    except Exception as e:
        print(e)

    print("end this")

