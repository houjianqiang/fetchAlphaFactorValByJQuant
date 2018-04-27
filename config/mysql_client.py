# -*- coding: utf-8 -*-
# @File  : mysql_client.py
# @Author: hoke
# @Date  : 2018/4/17
# @Desc  :
import pymysql
from config.Settings import *
import json

file_name = "dev.json"
with open(os.path.join(CONF_DIR, file_name), "rb") as f:
    data = f.read()
    data = json.loads(data)
    # data = json.dumps(data)

host = data['mysql']['host']
port = int(data['mysql']['port'])
user = data['mysql']['username']
passwd = data['mysql']['password']
db = data['mysql']['database']


# 创建连接
def create_conn():
    conn = pymysql.connect(host=host, port=port, user=user, passwd=passwd, db=db, charset='utf8')
    return conn


# 创建游标
def create_cursor(conn):
    cursor = conn.cursor()
    return cursor

'''

# 执行SQL，并返回收影响行数
conn = create_conn()
cursor = create_cursor(conn)

effect_row = cursor.execute("select * from factor_values")

# 执行SQL，并返回受影响行数
# effect_row = cursor.execute("update tb7 set pass = '123' where nid = %s", (11,))

# 执行SQL，并返回受影响行数,执行多次
# effect_row = cursor.executemany("insert into tb7(user,pass,licnese)values(%s,%s,%s)", [("u1","u1pass","11111"),("u2","u2pass","22222")])


# 提交，不然无法保存新建或者修改的数据
conn.commit()

# 关闭游标
cursor.close()
# 关闭连接
conn.close()

conn = create_conn()
cursor = create_cursor(conn)
cursor.execute("select * from factor_values")

'''
'''

# 获取剩余结果的第一行数据
row_1 = cursor.fetchone()
print(row_1)
# 获取剩余结果前n行数据
# row_2 = cursor.fetchmany(3)

# 获取剩余结果所有数据
# row_3 = cursor.fetchall()

conn.commit()
cursor.close()
conn.close()

'''