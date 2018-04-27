# -*- coding: utf-8 -*-
# @File  : Settings.py
# @Author: hoke
# @Date  : 2018/4/16
# @Desc  :
import sys
import os

# 根目录
BASE_DIR=os.path.dirname(__file__).replace("/config", "")

# 配置
CONF_DIR = os.path.dirname(__file__)

# 模型
MODEL_DIR = os.path.join(BASE_DIR, "models")

# 工具
UTILS_DIR = os.path.join(BASE_DIR, "utils")
