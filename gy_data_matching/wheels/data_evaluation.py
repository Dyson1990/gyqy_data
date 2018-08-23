# -*- coding: utf-8 -*-
# /usr/bin/python3
"""
--------------------------------
Created on %(date)s
@author: Dyson
--------------------------------
"""
import os
import sys
import pandas as pd
import numpy as np



class data_evaluation(object):

    def __init__(self):
        pass

    def df_empty(self, data, args=None):
        # 判断dataframe是否为空，是的话终止整个线程
        df = data[args[2]]
        if df.empty:
            data['trigger'] = False
        return data