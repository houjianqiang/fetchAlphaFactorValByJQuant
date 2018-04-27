# -*- coding: utf-8 -*-
# @File  : mongo_client.py
# @Author: hoke
# @Date  : 2018/4/16
# @Desc  :
import pymongo
from pymongo import MongoClient
import json
from config.Settings import CONF_DIR
import os

file_name = "dev.json"
with open(os.path.join(CONF_DIR, file_name), "rb") as f:
    data = f.read()
    data = json.loads(data)
    # data = json.dumps(data)

host = data['mongodb']['host']
port = int(data['mongodb']['port'])


def get_db(dt_name, *args,**kwargs):
    client = MongoClient(host, port)
    db = client[dt_name]
    # collection = db[c_name]
    return db

# from bson.objectid import ObjectId


# def get(post_id):
#     document = client.db.collection.find_one({"_id", ObjectId(post_id)})
#
#     return document
