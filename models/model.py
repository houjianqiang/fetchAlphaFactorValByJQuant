# -*- coding: utf-8 -*-
# @File  : model.py
# @Author: hoke
# @Date  : 2018/4/16
# @Desc  :
import datetime
import jqdatasdk as jq
import pandas as pd
import pymysql
import jqdatasdk.alpha191 as a191
import jqdatasdk.alpha101 as a101
from log.log import Logger

# 创建日志
logger = Logger(logname='log.txt', loglevel=1, logger="fox").getlog()

# def read_data(file_path=os.path.join(Settings.CONF_DIR, 'dev.json')):
#     file_path == file_path.replace(r'\\', os.path.sep)
#     import json
#     with open(file_path, 'rb') as f:
#         data = json.loads(f.read())
#     return data

host = '192.168.8.178'
port = 3306
user = 'root'
passwd = '6wKAbnj4'
db = 'test_factor'
charset = 'utf8'


def create_conn():
    # data = self.data
    # print(data)

    # global host,port,user,db,charset,passwd
    connection = pymysql.connect(host=host,
                                 user=user,
                                 password=passwd,
                                 db=db,
                                 charset=charset,
                                 cursorclass=pymysql.cursors.DictCursor)
    return connection


def create_cursor(conn):
    cursor = conn.cursor()
    return cursor


def bulk_insert(data):
    try:
        conn = create_conn()
        cur = create_cursor(conn)
        exc_str = "insert into factor_values (record_date, stock_code, factor_origin, factor_name, facotr_values) " \
                  "VALUES('%s','%s','%s','%s',%f)" % (
                      data['record_date'], data['stock_code'], data["factor_origin"], data['factor_name'],
                      data['factor_values'])
        cur.execute(exc_str)
        conn.commit()
        logger.info(u"插入数据，参数：记录日期：{}股票代码：{}因子来源：{}因子名称：{}因子值：{}".format(
                      data['record_date'], data['stock_code'], data["factor_origin"], data['factor_name'],
                      data['factor_values']))
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(msg=u'发生异常>>>>%s' % e)


def bulk_insert_series(data, factor_origin, factor_name, record_date):
    try:
        conn = create_conn()
        cur = create_cursor(conn)
        steps = 0
        for x in range(data.size):
            stock_code = data.index.values[x]
            factor_values = data.values[x]
            exc_str = "insert into factor_values (record_date, stock_code, factor_origin, factor_name, facotr_values) VALUES('%s','%s','%s','%s',%f)" % (
                record_date, stock_code, factor_origin, factor_name, factor_values)
            cur.execute(exc_str)
            logger.info(u"插入数据，参数：记录日期：{}股票代码：{}因子来源：{}因子名称：{}因子值：{}".format(record_date, stock_code, factor_origin,
                                                                                 factor_name, factor_values))
            if steps % 10 == 0:
                conn.commit()

        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(msg=u'发生异常>>>>%s' % e)


def mark_record(d, status):
    try:
        conn = create_conn()
        cur = create_cursor(conn)
        status = status
        insert_date = datetime.datetime.now().strftime("%Y-%m-%d ")
        record_date = d
        exc_str = "insert into records_status (status,insert_date,record_date) VALUES(%d,'%s','%s')" % (
            status, insert_date, record_date)
        cur.execute(exc_str)
        conn.commit()
        logger.info(msg=u"插入数据（记录完成进度）,参数:status:{}插入时间{}记录时间{}".format(status,insert_date,record_date))
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(msg=u'发生异常>>>>%s' % e)


def find_record(status):
    conn = create_conn()
    cur = create_cursor(conn)
    exc_str = "select record_date from records_status where status = %s" % status
    cur.execute(exc_str)
    data = cur.fetchall()
    conn.commit()
    cur.close()
    conn.close()
    return data


import jqdatasdk as jq


def find_nofinish(lis, origin):
    factor_origin = origin
    conn = create_conn()
    cur = create_cursor(conn)
    exc_str1 = "select fv.record_date from factor_values as fv left join records_status as rs on rs.record_date = fv.record_date where fv.factor_origin= %s group by fv.record_date"
    cur.execute(exc_str1, (factor_origin,))
    no_finish_data = cur.fetchmany(100)
    conn.commit()
    if not no_finish_data:
        return
    for o in no_finish_data:
        o = o['record_date']
        code_lis = get_data_base1()
        if not code_lis.empty:
            code_lis = code_lis['code'].tolist()
            for code in code_lis:

                exc_str2 = "select factor_name from factor_values where record_date='%s' and factor_origin='%s' and stock_code = '%s' " % (
                    o, factor_origin, code)
                cur.execute(exc_str2)
                finish_method_lis = cur.fetchall()
                conn.commit()

                conn.close()
                tmp_lis = []
                if finish_method_lis:
                    for i in finish_method_lis:
                        tmp_lis.append(i['factor_name'])
                    finish_method_lis = tmp_lis
                    condition = lambda t: t not in finish_method_lis
                    no_finish_method_lis = list(filter(condition, lis))
                # print(no_finish_method_lis)
                for method in no_finish_method_lis:
                    if factor_origin[0] == 'alpha191':
                        try:
                            # pass
                            jq.auth("18675594612", "Cmfchina123")
                            # jq.auth("18845210680", "xa255389")
                        except Exception as e:
                            logger.error(msg=u'发生异常>>>>%s' % e)

                        if method in ['alpha_075', 'alpha_149', 'alpha_181', 'alpha_182']:
                            eval_str = method + "(o_code,'000300.XSHG','%s')" % (o)
                        else:
                            eval_str = 'a191.' + method + '(code, "%s")' % (o)
                        data = eval(eval_str)
                        if not data.empty:
                            # print(data.values)
                            # # data : 股票代码  aplpha factor_values ,还需要date，origin，factor_name,
                            stock_code = code
                            factor_values = data.values[0]
                            factor_origin = "alpha191"
                            factor_name = method

                            dict_ = {
                                "stock_code": stock_code,
                                "factor_values": factor_values,
                                "record_date": o,
                                "factor_origin": factor_origin,
                                "factor_name": factor_name,
                            }
                            bulk_insert(dict_)


def getBetweenDay(s1, s2='2018-04-13'):
    date_list = jq.get_trade_days(start_date=s1, end_date=s2, count=None)
    return date_list


def get_data_base1():
    # li = jq.get_all_securities(types=['stock'])
    li = pd.read_csv("df.csv")
    return li


def deal_code(df, e_time):
    bt = pd.Timestamp(e_time)
    df[['start_date', 'end_date']] = df[['start_date', 'end_date']].apply(pd.to_datetime)
    df = df[df.start_date < bt]
    if not df.empty:
        df = df[df.end_date > bt]
    return df


# factor_values.bulk_insert()
if __name__ == '__main__':
    pass
