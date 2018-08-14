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
import codecs

class data_output(object):

    def __init__(self):
        pass
    
    def csv_output(self, data, args=None):
        # 输出csv
        # [data_output, 'csv_output', 're_table', {'path': 'D:/bg_data_30_test/', 'file_name': 'test_re'}],
        
        arg = args[-1]
        
        path = arg['path']
        if not os.path.exists(path):
            os.system('mkdir ' + path.replace('/', '\\'))
        
        file_name = arg['file_name']
        table_name = args[2]
        
        with codecs.open(path+file_name+'.csv', 'w', 'utf8') as f0:
            data[table_name].to_csv(f0, encoding='utf_8_sig')
        return data
        
        