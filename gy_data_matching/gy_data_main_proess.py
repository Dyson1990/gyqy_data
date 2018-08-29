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

import wheels.get_data
get_data = wheels.get_data.get_data()

import wheels.data_clean
data_clean = wheels.data_clean.data_clean()

import wheels.data_match
data_match = wheels.data_match.data_match()

import wheels.data_output
data_output = wheels.data_output.data_output()

import wheels.data_cal
data_cal = wheels.data_cal.data_cal()

#import set_log  

#log_obj = set_log.Logger('main_proess.log', set_log.logging.WARNING,
#                         set_log.logging.DEBUG)
#log_obj.cleanup('main_proess.log', if_cleanup = True)  # 是否需要在每次运行程序前清空Log文件


class main_proess(threading.Thread):

    def __init__(self, args_queue, lock, data_resource):
        threading.Thread.__init__(self)
        self.args_queue = args_queue
        self.lock = lock
        self.data_resource = data_resource

    def run(self):
        while True:
            lookup_args = self.args_queue.get()
            self._thread0(lookup_args)
            self.args_queue.task_done()


    def _thread0(self, lookup_args):

        start_time = time.time()

        data = {
            'i': 0,
            'path': 'D:/process_log_test_data_%s/%s/%s/' %(self.data_resource, lookup_args['year'], '_'.join(lookup_args['list']))[:255],
            'res': pd.DataFrame([]), #,columns=['lookup_name', 'entid', 'myid']),
            'info': {'year':lookup_args['year']
                    , 'province_code':'_'.join(lookup_args['list'])[:255]
                    },
            'count':{},
            'lock': self.lock,
        }

        if not os.path.exists(data['path']):
            os.system('mkdir ' + data['path'].replace('/', '\\'))
        
        # 规范标题行
        for key in data:
            if isinstance(data[key], type(pd.DataFrame([]))):
                data[key].columns = [s.lower() for s in data[key].columns]

        # method_list为一个列表，其中[module, method, 0]代表针对test_data的方法，其中[module, method, 1]代表针对parent_data的方法

            
        #北大工业企业数据纵向匹配    
        method_list = [
            [get_data, 'test_data_beida0', 'test_data', data['info']],
            [data_clean, 'entname_punctuation_clean', 'test_data', {'col_name': 'entname'}],
            [data_match, 'data_check0', None, {}],
        ]
        
        
        try:
            for method in method_list:
                # print(type(method))
                start_time0 = time.time()
                print("\n%s\n%s年%s省第%s步\n%s\n" %(datetime.datetime.now(), 
                                                       lookup_args['year'], 
                                                       lookup_args['list'],
                                                       str(data['i']),
                                                       method))
    
                func = getattr(method[0], method[1])
                data = func(data, args=method)
                
                if 'test_data' in data and data['test_data'].empty:
                    break
                
                if method[2]:
                    # 记录前200条数据以供查阅
                    data[method[2]].iloc[:200,].to_csv('%s%s.%s[%s%s].csv' % (data['path'], str(data['i']), method[1], method[2], data[method[2]].shape[0]))
                
                data['i'] = data['i'] + 1
                print("\n耗时： %s\n" % (time.time() - start_time0)) 
                
                
            # 存数据
            count_info_path = 'D://count_info_%s.csv' %data_resource
            with lock:
                if data['count']:
                    with codecs.open(count_info_path, 'r', 'utf-8') as f0:
                        count_info_df = pd.read_csv(f0, index_col=0, dtype=np.str)
                    count_info_df = count_info_df.append(data['count'], ignore_index=True)
                    count_info_df.to_csv(count_info_path)
        except:
            with self.lock:
                with codecs.open('error.log', 'a', 'utf-8') as f:
                    f.write("\n%s\n%s年%s省第%s步\n%s\n" %(datetime.datetime.now(), 
                                                       lookup_args['year'], 
                                                       lookup_args['list'], 
                                                       method,
                                                       traceback.format_exc()))
            time.sleep(4)

        pd.DataFrame([]).to_csv('%s%s.完成.csv' % (data['path'], str(data['i'])))
        print("总耗时： %s" %(time.time() - start_time))
        
        return None

if __name__ == '__main__':
    
    # main_proess.thread0({'list':('34', ), 'year':'2000'})
# =============================================================================
#     for s in tuple(('34{:0>2d}'.format(i) for i in range(30))):
#         main_proess.thread0({'list':(s, ), 'year':'2000'})
# =============================================================================
# =============================================================================
#     main_proess.thread0({'list':list(('34{:0>2d}'.format(i) for i in range(30))), 'year':'2000'})
# =============================================================================
    
    with codecs.open("%s\\args\\district_code.csv" %os.getcwd(),'r','utf-8') as f0:
        district_name = pd.read_csv(f0, dtype=np.str) # 第一列不作为index
    prov_list = set([str(s)[0:2] for s in district_name['code'].tolist()])
    lock = threading.Lock()
    args_queue = queue.Queue()
    count_queue = queue.Queue()
    data_resource = 'beida'
    count_info_path = 'D://count_info_%s.csv' %data_resource
    # 初始化CSV文件
    pd.DataFrame([]).to_csv(count_info_path)
    
    
    for y in range(1998, 2014):
        for s in prov_list:
            args_queue.put({'list':(s, ), 'year':str(y)})
    

    # 10个线程
    for i in range(3):
        t = main_proess(args_queue, lock, data_resource)
        t.setDaemon(True)
        t.start()
        
    args_queue.join()
    
    print("程序运行完毕！！！！")
    time.sleep(36000)
    
    
# =============================================================================
#         #数据横向匹配    
#         method_list = [
#             # 读取原始数据
#             [get_data, 'test_data_%s' %self.data_resource, 'test_data', lookup_args],
#             # 读取清洗好的数据
#             [get_data, 'csv_data', 'parent_data', {'file_path':'D:/namelist2/%s.csv' %lookup_args['list'][0], 'table_name':'parent_data'}],
#             # 数据初步清洗
#             [data_clean, 'entname_punctuation_clean', 'test_data', {'col_name': 'lookup_name'}],
#             [data_clean, 'entname_punctuation_clean', 'test_data', {'col_name': 'r_name'}],
#             
#             # 对应 企业全名 + 省码
#             # ？？？？？？此处的“正确”匹配的数据并没有检验？？？？？？？
#             # [data_cal, 'name_district_comb', None, {}],
#             [data_match, 'data_merge', None, {'lookup_name_x': 'lookup_name', 'lookup_name_y': 'lookup_name', 'bool_col': 'entid'}],
#             # [data_cal, 'col_drop', 'test_data', {'col_name': 'lookup_name0'}],
#             # [data_cal, 'col_drop', 'parent_data', {'col_name': 'lookup_name0'}],
#             
# # =============================================================================
# #             # 读取清洗好的法人信息
# #             [get_data, 'csv_data', 'r_name_list', {'file_path':'D:/combined_r_name_list/%s.csv' %lookup_args['list'][0], 'table_name':'r_name_list'}],
# #             
# #             [data_match, 'data_check', None, {'lookup_name_x': 'lookup_name0', 'lookup_name_y': 'lookup_name0', 'bool_col': 'entid'}],
# # =============================================================================
#             
#             # 去除lookup_name中的括号
#             [data_clean, 'bracket_clean', 'test_data', ''],
#             [data_clean, 'bracket_clean', 'parent_data', ''],
#             
#             # 分离企业属性
#             [data_clean, 'company_type_split', 'test_data', ''],
#             [data_clean, 'company_type_split', 'parent_data', ''],
#             
#             # 分离企业的省份
#             [data_clean, 'place_split', 'test_data', {'str_col':'province'}],
#             [data_clean, 'place_split', 'parent_data', {'str_col':'province'}],
#             
# # =============================================================================
# #             # 分离企业的市
# #             [data_clean, 'place_split', 'test_data', {'str_col':'city'}],
# #             [data_clean, 'place_split', 'parent_data', {'str_col':'city'}],
# #             
# #             # 分离企业的县
# #             [data_clean, 'place_split', 'test_data', {'str_col':'county'}],
# #             [data_clean, 'place_split', 'parent_data', {'str_col':'county'}],
# # =============================================================================
#             
#             [data_match, 'data_merge', None, {'lookup_name_x': 'lookup_name', 'lookup_name_y': 'lookup_name', 'bool_col': 'entid'}],
#         ]
# =============================================================================
    
# =============================================================================
#         # 海关数据整理
#         method_list = [
#             # 读取原始数据
#             [get_data, 'f_trade_info_2017', 'trade_info', data['info']],
#             # 读取清洗好的数据
#             [get_data, 'csv_data', 'parent_data', {'file_path':'D:/namelist2/%s.csv' %lookup_args['list'][0], 'table_name':'parent_data'}],
#             # 数据初步清洗
#             [data_clean, 'entname_punctuation_clean', 'trade_info', {'col_name': 'entname'}],
#             
#             [data_match, 'data_merge', None, {'table_x':'trade_info',
#                                               'lookup_name_x': 'entname', 
#                                               'lookup_name_y': 'lookup_name', 
#                                               'bool_col': 'entid',
#                                               'filter_parent_df': ['name_type', 'current']}
#     
#             ],
#             [data_match, 'data_merge', None, {'table_x':'trade_info',
#                                               'lookup_name_x': 'entname', 
#                                               'lookup_name_y': 'lookup_name', 
#                                               'bool_col': 'entid',
#                                               'filter_parent_df': ['name_type', 'old'],}
#     
#             ],
# 
#         ]
# =============================================================================
    
# =============================================================================
#         # 将企业现用名和曾用名整合到一起
#         method_list = [
#             [get_data, 'jbxx_data_entname', 'jbxx', data['info']],
#             [data_clean, 'name2id_list', 'jbxx', {}],
#             [data_clean, 'entname_punctuation_clean', 'jbxx', {'col_name': 'lookup_name'}],
#             [data_output, 'csv_output', 'jbxx', {'path': 'D:/name_list2/', 'file_name': '_'.join(lookup_args['list']), 'table_name': 'jbxx'}],
#         ]
# =============================================================================
    
# =============================================================================
#         # 备份法人数据
#         method_list = [
#                 
#               [get_data, 'r_name_list', 'r_name_list', data['info']],
#               
#               [data_clean, 'entname_punctuation_clean', 'r_name_list', {'col_name': 'altaf'}],
#               [data_clean, 'entname_punctuation_clean', 'r_name_list', {'col_name': 'altbe'}],
#               
#               [data_output, 'csv_output', 'r_name_list', {'path': 'D:/r_name_prov_list/', 'file_name': '_'.join(lookup_args['list']), 'table_name': 'r_name_list'}],
#               ]
# 
# =============================================================================


# =============================================================================
#         #数据横向匹配    
#         method_list = [
#             # 读取原始数据
#             [get_data, 'test_data_%s' %self.data_resource, 'test_data', lookup_args],
#             # 读取清洗好的数据
#             [get_data, 'csv_data', 'parent_data', {'file_path':'D:/namelist/%s.csv' %lookup_args['list'][0], 'table_name':'parent_data'}],
#             # 数据初步清洗
#             [data_clean, 'entname_punctuation_clean', 'test_data', {'col_name': 'lookup_name'}],
#             [data_clean, 'entname_punctuation_clean', 'test_data', {'col_name': 'r_name'}],
#             
#             # 对应 企业全名 + 省码
#             # ？？？？？？此处的“正确”匹配的数据并没有检验？？？？？？？
#             # [data_cal, 'name_district_comb', None, {}],
#             [data_match, 'data_merge', None, {'lookup_name_x': 'lookup_name', 'lookup_name_y': 'lookup_name', 'bool_col': 'entid'}],
#             # [data_cal, 'col_drop', 'test_data', {'col_name': 'lookup_name0'}],
#             # [data_cal, 'col_drop', 'parent_data', {'col_name': 'lookup_name0'}],
#             
#             # 读取清洗好的法人信息
#             [get_data, 'csv_data', 'r_name_list', {'file_path':'D:/r_name_prov_list/%s.csv' %lookup_args['list'][0], 'table_name':'r_name_list'}],
#             
#             # 使用正则表达处理
#             [data_clean, 'replace_by_re', 'r_name_list', {'re_str':',', 'replace_col':'altbe', 'replace_str': '|'}],
#             [data_clean, 'replace_by_re', 'r_name_list', {'re_str':',', 'replace_col':'altaf', 'replace_str': '|'}],
#             
#             # 组合法人信息中，同一家公司，在不同行中的法人信息
#             [data_clean, 'row_comb', 'r_name_list', {'groupby_col':'entid', 'comb_col':['altbe', 'altaf']}],
#             
#             # 对比法人信息
#             [data_clean, 'str_punctuation_clean', 'r_name_list', {'col_name': 'altaf'}],
#             [data_clean, 'str_punctuation_clean', 'r_name_list', {'col_name': 'altbe'}],
#             [data_clean, 'make_r_name_list', 'r_name_list', {}],
#             [data_match, 'data_check', None, {'lookup_name_x': 'lookup_name0', 'lookup_name_y': 'lookup_name0', 'bool_col': 'entid'}],
#             
# # =============================================================================
# #             # 去除lookup_name中的括号
# #             [data_clean, 'bracket_clean', 'test_data', ''],
# #             [data_clean, 'bracket_clean', 'parent_data', ''],
# #             
# #             # 分离企业属性
# #             [data_clean, 'company_type_split', 'test_data', ''],
# #             [data_clean, 'company_type_split', 'parent_data', ''],
# #             
# #             # 分离企业的省份
# #             [data_clean, 'place_split', 'test_data', {'str_col':'province'}],
# #             [data_clean, 'place_split', 'parent_data', {'str_col':'province'}],
# #             
# # # =============================================================================
# # #             # 分离企业的市
# # #             [data_clean, 'place_split', 'test_data', {'str_col':'city'}],
# # #             [data_clean, 'place_split', 'parent_data', {'str_col':'city'}],
# # #             
# # #             # 分离企业的县
# # #             [data_clean, 'place_split', 'test_data', {'str_col':'county'}],
# # #             [data_clean, 'place_split', 'parent_data', {'str_col':'county'}],
# # # =============================================================================
# #             
# #             [data_match, 'data_merge', None, {'lookup_name_x': 'lookup_name', 'lookup_name_y': 'lookup_name', 'bool_col': 'entid'}],
# # =============================================================================
#         ]
# =============================================================================


# =============================================================================
#         #北大工业企业数据纵向匹配    
#         method_list = [
#             [get_data, 'test_data_beida0', 'test_data', data['info']],
#             [data_clean, 'entname_punctuation_clean', 'test_data', {'col_name': 'entname'}],
#             [data_match, 'data_check0', None, {}],
#         ]
# =============================================================================

# =============================================================================
#         #法人数据合并   
#         method_list = [
#             [get_data, 'csv_data', 'parent_data', {'file_path':'D:/namelist/%s.csv' %lookup_args['list'][0], 'table_name':'parent_data'}],
#             # 读取清洗好的法人信息
#             [get_data, 'csv_data', 'r_name_list', {'file_path':'D:/r_name_prov_list/%s.csv' %lookup_args['list'][0], 'table_name':'r_name_list'}],
#             
#             # 使用正则表达处理
#             [data_clean, 'replace_by_re', 'r_name_list', {'re_str':',', 'replace_col':'altbe', 'replace_str': '|'}],
#             [data_clean, 'replace_by_re', 'r_name_list', {'re_str':',', 'replace_col':'altaf', 'replace_str': '|'}],
#             
#             # 组合法人信息中，同一家公司，在不同行中的法人信息
#             [data_clean, 'row_comb', 'r_name_list', {'groupby_col':'entid', 'comb_col':['altbe', 'altaf']}],
#             
#             # 对比法人信息
#             [data_clean, 'str_punctuation_clean', 'r_name_list', {'col_name': 'altaf'}],
#             [data_clean, 'str_punctuation_clean', 'r_name_list', {'col_name': 'altbe'}],
#             [data_clean, 'make_r_name_list', 'r_name_list', {}],
#             
#             [data_output, 'csv_output', 'r_name_list', {'path': 'D:/combined_r_name_list/', 'file_name': '_'.join(lookup_args['list']), 'table_name': 'r_name_list'}],
# 
#         ]
# =============================================================================
