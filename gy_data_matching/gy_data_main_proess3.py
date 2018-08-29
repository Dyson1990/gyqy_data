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
import multiprocessing
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

import wheels.data_evaluation
data_evaluation = wheels.data_evaluation.data_evaluation()

class main_proess(object):
    def __init__(self):
        self.lock = multiprocessing.Manager().Lock()
        self.datamark = 'bg_30'
        self.count_dict = {}
        self.log_path = 'D:\\process_log_%s' %self.datamark

    def run(self):
        with codecs.open("%s\\args\\district_code.csv" %os.getcwd(),'r','utf-8') as f0:
            district_name = pd.read_csv(f0, dtype=np.str) # 第一列不作为index
        prov_list = set([str(s)[0:4] for s in district_name['code'].tolist()])
        args_list = []
        
        for y in range(1998, 1999):
            for s in prov_list:
                args_list.append({'district_code':s, 'year':str(y)})
        
        process_count = 10
        pool = multiprocessing.Pool(process_count)
        pool.map_async(self._proess0, args_list)
        pool.close() # 关闭进程池，不再接受新的进程
        pool.join() # 主进程阻塞等待子进程的退出
                    
        print("程序运行完毕！！！！")
        time.sleep(36000)

    def _proess0(self, lookup_args):
        print('Starting process{}<{}>'.format(multiprocessing.current_process().pid
                                              , multiprocessing.current_process().name))
        start_time = time.time()
        
        data = {
            'i': 0
            , 'path': os.path.join(self.log_path, lookup_args['year'], lookup_args['district_code'])
            , 'res': pd.DataFrame([])
            , 'info': {'year':lookup_args['year'], 'province_code':lookup_args['district_code']}
            , 'count':self.count_dict # 每个data_match中涉及的结果统计
            , 'lock': self.lock
            , 'trigger': True
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
              [get_data2, 'xlsx_multi_sheets_data', 're_table', {'file_path':'D:/正则表达式_修改.xlsx'}],
              [data_cal, 're_table_value_filter', 're_table', {}],
              [data_evaluation, 'df_empty', 're_table', {}],
              [data_clean, 'punctuation_eng2cn', 're_table', {'col_name':'正则表达式'}],
              [data_clean, 'punctuation_eng2cn', 're_table', {'col_name':'正则表达式-'}],
              [data_clean, 'punctuation_eng2cn', 're_table', {'col_name':'举例'}],
              [data_clean, 'punctuation_eng2cn', 're_table', {'col_name':'字段说明'}],
              
# =============================================================================
#               # 根据“举例”来测试正则表达式的准确性
#               [data_cal, 'check_re_str', 're_table', {}],
#               [data_output, 'csv_output', 're_table', {'path': 'D:/bg_data_30_test/', 'file_name': 'test_re'}],
# =============================================================================
              
# =============================================================================
#               # 检查还未记录的市码
#               [get_data2, 'bg_prov_code', 'bg_prov_code', lookup_args],
#               [data_output, 'csv_output', 'bg_prov_code', {'path': 'D:/bg_data_30_test/', 'file_name': 'prov_code_list'}],
# =============================================================================
              
              # 使用正则表达式检测一个市的所有BG数据
              [get_data2, 'sql_data', 'bg_30_origin', lookup_args],
              [data_cal, 'bg_data_re_clean', 'bg_30_origin', {}],
              [data_output, 'csv_output', 'bg_30_origin', {'path': 'D:/bg_data_30_test/'
                                                           , 'file_name': 'bg_data_%s' %lookup_args['district_code']}],
              ]
        try:
            process_info = {}
            for method in method_list:
                start_time0 = time.time()
                print("\n%s\n%s年%s省第%s步\n%s\n" %(datetime.datetime.now() 
                                                     , lookup_args['year']
                                                     , lookup_args['district_code']
                                                     , str(data['i'])
                                                     , method))
                # 执行method_list中每一个步骤
                func = getattr(method[0], method[1])
                data = func(data, args=method)
                
                # 'trigger'参数为False的时候，说明已经没有必要进行剩下的步骤了
                if not data['trigger']:
                    pd.DataFrame([]).to_csv('%s\\%s.-程序中断[总耗时:%s].csv' % (data['path']
                                                                                , str(data['i'])
                                                                                , int(time.time() - start_time)))
                    break
                
       #############################以下是日志部分#############################
                
                if 'test_data' in data and data['test_data'].empty:
                    break
                
                # 统计每一步各个DataFrame的情况
                step_info = {}
                for key in data:
                    if isinstance(data[key], pd.core.frame.DataFrame):
                        step_info.update(
                                {'step{}'.format(data['i']):{key: data[key].shape}})
                process_info.update(step_info)
                        
                if method[2]:
                    # 记录前200条数据以供查阅
                    data[method[2]].iloc[:200,].to_csv('%s\\%s.%s[%s%s].csv' % (data['path']
                                                                            , str(data['i'])
                                                                            , method[1]
                                                                            , method[2]
                                                                            , data[method[2]].shape[0])
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
                process_info_df = pd.DataFrame(process_info)
                process_info_df.to_csv(os.path.join(self.log_path, 'process_info.csv')
                                       , encoding = 'utf_8_sig')
            
            # 证明程序运行完了，没有报错
            pd.DataFrame([]).to_csv('%s\\%s.程序运行完毕[总耗时:%s].csv' % (data['path']
                                                                          , str(data['i'])
                                                                          , int(time.time() - start_time)))
        except:
            with self.lock:
                with codecs.open(os.path.join(self.log_path, 'error.log'), 'a', 'utf-8') as f:
                    f.write("\n%s\n%s年%s省第%s步%s\n%s\n" %(datetime.datetime.now() 
                                                             , lookup_args['year']
                                                             , lookup_args['district_code']
                                                             , data['i']
                                                             , method
                                                             , traceback.format_exc()))
            time.sleep(4)

        
        print("总耗时： %s" %(time.time() - start_time))
        
        return None

if __name__ == '__main__':
    main_proess = main_proess()
    main_proess.run()
    







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
