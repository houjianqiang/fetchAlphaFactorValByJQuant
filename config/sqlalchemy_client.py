# -*- coding: utf-8 -*-
# @File  : sqlalchemy_client.py
# @Author: hoke
# @Date  : 2018/4/17
# @Desc  :
import sqlalchemy
import json
import os
from sqlalchemy import create_engine, Column, String, Date, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from config.Settings import *


def get_engine():
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

    query = 'mysql+mysqlconnector://%s:%s@%s:%s/%s?charset=%s' % (user, passwd, host, port, db, 'utf8')
    engine = create_engine(query, echo=True)
    return engine


engine = get_engine()


def create_sessin(engine):
    db_session = sessionmaker(bind=engine)

    return db_session()


session = create_sessin(engine)


# 数据模型基础操作类
class BaseManager(object):
    # 增加
    @staticmethod
    def create_obj(params):
        session.add(params)
        session.commit()
        return True

    # 按条件删除
    @staticmethod
    def delete_by_condition(obj, **kwargs):
        data = session.query(obj).filter_by(**kwargs).all()
        for u in data:
            session.delete(u)
        session.commit()
        return True

    # 单查询
    @staticmethod
    def query_by_condition(obj, **kwargs):
        return session.query(obj).filter_by(**kwargs)

    # 多查询
    @staticmethod
    def query_all(obj):
        return session.query(obj).all()


Base = declarative_base()



class factor_values(Base):
    __tablename__ = 'factor_values'
    date = Column(Date)
    stock_code = Column(String)
    factor_values = Column(Float)
    factor_name = Column(String)
    factor_origin = Column(String)

    def __repr__(self):
        return '<factor_values(date=%s,stock_code=%s,factor_values=%s,factor_name=%s,factor_origin=%s)>' \
               % (str(self.date), self.stock_code, str(self.factor_values), self.factor_name, self.factor_origin)

    # def to_dict(self):
    #     dict_ = {}
    #     for filed in self.__get_fields():
    #         dict_[filed] = getattr(self, filed)
    #     return dict_
    #
    # @classmethod
    # def __get_fields(cls):
    #     return {f: getattr(cls, f).expression.type for f in cls._sa_class_manager._all_key_set}
# ### create table----------------------------------------------
# Base.metadata.create_all(engine)
# ##-------------------------------------------------------------
#
# ### create session
# Session = sessionmaker(bind=engine)
# session = Session()
#
# ## insert new data to table
# a = [1, 2, 3, 4, 5, 6]
# b = [6, 7, 8, 9, 0, 1]
# hehe_list = [factor_values(a=str(a[i]), b=str(b[i])) for i in range(6)]
# session.add_all(hehe_list)
# session.commit()


if __name__ == '__main__':
    pass
    # create_tables()
