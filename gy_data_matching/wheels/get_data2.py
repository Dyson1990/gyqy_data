# -*- coding:utf-8 -*-  
#/usr/bin/python3
"""
--------------------------------
    @Author: Dyson
    @Contact: Weaver1990@163.com
    @file: get_data.py
    @time: 2018/6/26 13:32
--------------------------------
"""
import sys
import os
import numpy as np
import pandas as pd
import time
import codecs
import json

import wheels.wheels.sql_manager
sql_manager = wheels.wheels.sql_manager.sql_manager()

class get_data2(object):

    def __init__(self):
        pass

    def sql_data(self, data, args=None):
        """
        读取各类数据库中的数据数据
        {'list':
        , 'get_data_func_name':
        , 'year':
        }
        """
        start_time = time.time()
        lookup_args = args[-1]
        table_name = args[2]
        
        print(lookup_args)
        lookup_code = lookup_args['district_code']
        func_name = args[2]
        
        # 读取对应函数中所需的参数
        with codecs.open("%s\\args\\func_args.json" %os.getcwd(), 'r', 'utf-8') as f0:
            func_args = json.load(f0)
        
        func_args = func_args[func_name]
        func_args['year'] = lookup_args['year']
        
        if 'where_district_type' in func_args:
            func_args['where_district_str'] = func_args['where_district_type'].format(len(lookup_code), lookup_code)
        
        # 读取相关sql连接参数
        with codecs.open("%s\\args\\sql_args.json" %os.getcwd(), 'r', 'utf-8') as f0:
            sql_args_dict = json.load(f0) # 读取连接不同数据库的参数
        sql_args = sql_args_dict[func_args['db_type']]

        # 补充一些参数
        if os.path.exists("%s\\args\\table_args_%s.json" %(os.getcwd(), func_name)):
            with codecs.open("%s\\args\\table_args_%s.json" %(os.getcwd(), func_name), 'r', 'utf-8') as f0:
                table_args = json.load(f0)
            func_args.update(table_args[func_args['year']])
        
        # 输出完整的sql代码
        with codecs.open("%s\\args\\sql_str_%s.sql" %(os.getcwd(), func_name), 'r', 'utf-8') as f0:
            sql = f0.read().format(**func_args)
        print(sql)
        
        df = sql_manager.connect(sql, sql_args)
        if df is None:
            df = pd.DataFrame([])
        df.columns = [s.lower() for s in df.columns]
        
        data[table_name] = df

        print('总数据：%s行, 读取耗时：%s' % (df.shape[0], time.time() - start_time))

        return data
    
        
    def file_data(self, data, args=None):
        """
        从csv文件中读取已经清洗好的数据
        [get_data, 'file_data', '', {'file_path':'D:/namelist/{}.csv'.format(lookup_args['district_code'])}],
        """
        start_time = time.time()
        arg = args[-1]
        
        file_path = arg['file_path']
        table_name = args[2]
        
        if os.path.exists(file_path):
            # 确定文件类型
            if os.path.splitext(file_path)[-1] == 'csv':
                # 读取csv格式文件， 以第一列为index
                with codecs.open(file_path,'r','utf-8') as f0:
                    df = pd.read_csv(f0, dtype=np.str, index_col=0)
            elif os.path.splitext(file_path)[-1] == 'xlsx' or \
                 os.path.splitext(file_path)[-1] == 'xls':
                # 读取excel格式的文件
                df = pd.read_excel(file_path)
                
                
            if df is None:
                df = pd.DataFrame([])
            
            df.columns = [s.lower() for s in df.columns]
            
            data[table_name] = df
            print('总数据：%s行, 读取耗时：%s' % (df.shape[0], time.time() - start_time))
            
            return data
        else:
            data[table_name] = pd.DataFrame([])
            return data
    
    def dir_csv_data(self, data, args=None):
        """
        读取一个文件夹中所有的csv文件
        """
        start_time = time.time()
        arg = args[-1]
        
        dir_path = arg['dir_path']
        table_name = args[2]
        
        if os.path.exists(dir_path):
            df_sum = pd.DataFrame([])
            for file_name in os.listdir(dir_path):
                if os.path.splitext(file_name)[-1] == 'csv':
                    with codecs.open(os.path.join(dir_path, file_name),'r','utf-8') as f0:
                        df0 = pd.read_csv(f0, dtype=np.str, index_col=0)
                    
                        if df0 is None:
                            continue
                        df_sum = pd.concat([df_sum, df0], sort=False)

            df_sum.columns = [s.lower() for s in df_sum.columns]
            data[table_name] = df_sum
            print('总数据：%s行, 读取耗时：%s' % (df_sum.shape[0], time.time() - start_time))
            
            return data
        else:
            data[table_name] = pd.DataFrame([])
            return data
        
    def xlsx_multi_sheets_data(self, data, args=None):
        """
        读取xlsx文件夹中所有sheet的文件
        """
        start_time = time.time()
        arg = args[-1]
        
        file_path = arg['file_path']
        table_name = args[2]
        
        if os.path.exists(file_path):
            # 确定文件类型
            df_dict = pd.read_excel(file_path, sheet_name=None, dtype=np.str)
            
            df_sum = pd.concat(df_dict.values(), join='outer', ignore_index=True, sort=False)
                
            data[table_name] = df_sum.fillna('')
            print('总数据：%s行, 读取耗时：%s' % (df_sum.shape[0], time.time() - start_time))
            return data
        else:
            data[table_name] = pd.DataFrame([])
            return data
        



if __name__ == '__main__':
    get_data2 = get_data2()
