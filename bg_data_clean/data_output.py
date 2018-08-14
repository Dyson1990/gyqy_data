# -*- coding:utf-8 -*-  
#/usr/bin/python3
"""
--------------------------------
    @Author: Dyson
    @Contact: Weaver1990@163.com
    @file: data_output.py
    @time: 2018/6/27 17:13
--------------------------------
"""
import os

class data_output(object):

    def __init__(self):
        pass
    
    def csv_output(self, data, args=None):
        arg = args[-1]
        
        path = arg['path']
        if not os.path.exists(path):
            os.system('mkdir ' + path.replace('/', '\\'))
        

        file_name = arg['file_name']
        table_name = arg['table_name']
        
        data[table_name].to_csv(path+file_name+'.csv')
        return data
        
        