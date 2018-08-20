# -*- coding:utf-8 -*-  
#/usr/bin/python3
"""
--------------------------------
    @Author: Dyson
    @Contact: Weaver1990@163.com
    @file: main_proess.py
    @time: 2018/6/29 18:20
--------------------------------
"""
import sys
import os
import numpy as np
import pandas as pd
import time
import threading
import traceback
import datetime
import queue
import codecs
import shutil

import wheels.get_data2
get_data2 = wheels.get_data2.get_data2()

import wheels.data_clean
data_clean = wheels.data_clean.data_clean()

import wheels.data_match
data_match = wheels.data_match.data_match()

import wheels.data_output
data_output = wheels.data_output.data_output()

import wheels.data_cal
data_cal = wheels.data_cal.data_cal()

class main_proess(threading.Thread):
    def __init__(self, args_queue, lock, datamark, count_dict):
        threading.Thread.__init__(self)
        self.args_queue = args_queue
        self.lock = lock
        self.datamark = datamark
        self.count_dict = count_dict

    def run(self):
        while True:
            lookup_args = self.args_queue.get()
            self._thread0(lookup_args)
            self.args_queue.task_done()

    def _thread0(self, lookup_args):

        start_time = time.time()
        
        log_path = 'D:\\process_log_%s' %self.datamark
        data = {
            'i': 0,
            'path': os.path.join(log_path, lookup_args['year'], lookup_args['district_code']),
            'res': pd.DataFrame([]),
            'info': {'year':lookup_args['year'], 'province_code':lookup_args['district_code']},
            'count':self.count_dict, # 每个data_match中涉及的结果统计
            'lock': self.lock,
        }
        
        # 清空原有的对应县码的日志文件
        if os.path.exists(data['path']):
            shutil.rmtree(data['path'])
        os.makedirs(data['path'].replace('/', '\\'))
        
        # 规范data中的df的标题行
        for key in data:
            if isinstance(data[key], pd.core.frame.DataFrame):
                data[key].columns = [s.lower() for s in data[key].columns]

        # method_list为一个列表，其中[module, method, 0]代表针对test_data的方法，其中[module, method, 1]代表针对parent_data的方法
        
        # bg30提取 new
        method_list = [
              [get_data2, 'xlsx_multi_sheets_data', 're_table', {'file_path':'D:/徐丽俊.xlsx'}],
              [data_clean, 'punctuation_eng2cn', 're_table', {'col_name':'正则表达式'}],
              [data_clean, 'punctuation_eng2cn', 're_table', {'col_name':'正则表达式-'}],
              [data_clean, 'punctuation_eng2cn', 're_table', {'col_name':'举例'}],
              
              # 根据“举例”来测试正则表达式的准确性
              [data_cal, 'check_re_str', 're_table', {}],
              [data_output, 'csv_output', 're_table', {'path': 'D:/bg_data_30_test/', 'file_name': 'test_re'}],
              
# =============================================================================
#               # 检查还未记录的市码
#               [get_data2, 'bg_prov_code', 'bg_prov_code', lookup_args],
#               [data_output, 'csv_output', 'bg_prov_code', {'path': 'D:/bg_data_30_test/', 'file_name': 'prov_code_list'}],
#               
#               # 使用正则表达式检测一个市的所有BG数据
#               [get_data2, 'bg_data', 'bg_data', lookup_args],
#               [data_cal, 'bg_data_re_clean', 'bg_data', {}],
#               [data_output, 'csv_output', 'bg_data', {'path': 'D:/bg_data_30_test/', 'file_name': 'bg_data_%s' %lookup_args['district_code']}],
# =============================================================================
              ]

        try:
            thread_info = {}
            for method in method_list:
                start_time0 = time.time()
                print("\n%s\n%s年%s省第%s步\n%s\n" %(datetime.datetime.now(), 
                                                       lookup_args['year'],
                                                       lookup_args['district_code'],
                                                       str(data['i']),
                                                       method))
    
                func = getattr(method[0], method[1])
                data = func(data, args=method)
                
                if 'test_data' in data and data['test_data'].empty:
                    break
                
                # 统计每一步各个DataFrame的情况
                step_info = {}
                for key in data:
                    if isinstance(data[key], pd.core.frame.DataFrame):
                        step_info.update(
                                {'step{}'.format(data['i']):{key: data[key].shape}})
                thread_info.update(step_info)
                        
                if method[2]:
                    # 记录前200条数据以供查阅
                    data[method[2]].iloc[:200,].to_csv('%s\\%s.%s[%s%s].csv' % (data['path']
                                                                            , str(data['i'])
                                                                            , method[1]
                                                                            , method[2]
                                                                            , data[method[2]]
                                                                            .shape[0])
                                                       , encoding = 'utf_8_sig')
                
                data['i'] = data['i'] + 1
                print("\n耗时： %s\n" % (time.time() - start_time0))
                

            # 存入整个程序运行日志
            with self.lock:
                count_info_df = pd.DataFrame(data['count'])
                count_info_df.to_csv(os.path.join(data['path'], 'process_info.csv')
                                     , encoding = 'utf_8_sig')
            
            # 保存步骤中数据集大小变化
            with self.lock:
                thread_info_df = pd.DataFrame(thread_info)
                thread_info_df.to_csv(os.path.join(log_path, 'thread_info.csv')
                                       , encoding = 'utf_8_sig')
            
            # 证明程序运行完了，没有报错
            pd.DataFrame([]).to_csv('%s%s.程序运行完毕[总耗时:%s].csv' % (data['path']
                                                                          , str(data['i'])
                                                                          , (time.time() - start_time)))
        except:
            with self.lock:
                with codecs.open('error.log', 'a', 'utf-8') as f:
                    f.write("\n%s\n%s年%s省第%s步%s\n%s\n" %(datetime.datetime.now(), 
                                                           lookup_args['year'], 
                                                           lookup_args['district_code'], 
                                                           data['i'], 
                                                           method,
                                                           traceback.format_exc()))
            time.sleep(4)

        
        print("总耗时： %s" %(time.time() - start_time))
        
        return None

if __name__ == '__main__':
    with codecs.open("%s\\args\\district_code.csv" %os.getcwd(),'r','utf-8') as f0:
        district_name = pd.read_csv(f0, dtype=np.str) # 第一列不作为index
    prov_list = set([str(s)[0:2] for s in district_name['code'].tolist()])
    lock = threading.Lock()
    args_queue = queue.Queue()
    datamark = 'test'
    count_list = []
    
    for y in range(1998, 1999):
        for s in prov_list:
            args_queue.put({'district_code':s, 'year':str(y)})
    

    # 10个线程
    for i in range(1):
        t = main_proess(args_queue, lock, datamark, count_list)
        t.setDaemon(True)
        t.start()
        
    args_queue.join()
    
    print("程序运行完毕！！！！")
    time.sleep(36000)
    







# =============================================================================
#         # bg30提取 new
#         method_list = [
#               [get_data2, 'sql_data', 'bg_30', lookup_args],
#               [data_clean, 'str_punctuation_clean', 'bg_30', {'col_name': 'altaf'}],
#               [data_clean, 'str_punctuation_clean', 'bg_30', {'col_name': 'altbe'}],
#               [data_clean, 'punctuation_eng2cn', 'bg_30', {'col_name': 'altbe'}],
#               [data_clean, 'punctuation_eng2cn', 'bg_30', {'col_name': 'altaf'}],
#               [data_output, 'csv_output', 'bg_30', {'path': 'D:/bg_30_new/'
#                                                     , 'file_name': '_'.join(lookup_args['list'])
#                                                     , 'table_name': 'bg_30'}],
#               ]
# =============================================================================
