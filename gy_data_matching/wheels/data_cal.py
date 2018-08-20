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
import re

class data_cal(object):

    def __init__(self):
        pass

    def name_district_comb(self, data, args=None):
        # 组合两个字符串
        test_df = data['test_data']
        parent_df = data['parent_data']
        
        test_df['lookup_name0'] = test_df['lookup_name'] + test_df['district'].apply(lambda s:s[:2])
        parent_df['lookup_name0'] = parent_df['lookup_name'] + parent_df['district'].apply(lambda s:s[:2])
        
        data['parent_data'] = parent_df
        data['test_data'] = test_df
        
        return data
    
    def col_copy(self, data, args=None):
        # 备份清洗好的企业名称列
        arg = args[-1]
        old_name = arg['old_name']
        new_name = arg['new_name']
        df = data[args[2]]
        
        df[new_name] = df[old_name].copy()

        data[args[2]] = df
        
        return data
    
    def col_drop(self, data, args=None):
        # 删除特定数据列
        arg = args[-1]
        col_name = arg['col_name']
        df = data[args[2]]
        
        df= df.drop([col_name, ], axis=1)

        data[args[2]] = df
        
        return data
    
    def check_duplicate(self, data, args=None):
        # 输出重复数据情况
        arg = args[-1]
        col_name = arg['col_name']
        df = data[arg['table_name']]
        
        df[col_name+'[duplicate_bool]'] = df[col_name].duplicated()
        F0 = df[col_name+'[duplicate_bool]'][df[col_name+'[duplicate_bool]'] == False].count()
        T0 = df[col_name+'[duplicate_bool]'][df[col_name+'[duplicate_bool]'] == True].count()
        print(T0,F0)
        
        df.to_csv('%s%s.check_duplicate【F%s】【T%sF%s】.csv' % (data['path'], data['i'], arg['table_name'], T0, F0))
        
        return data
    
    def check_re_str(self, data, args=None):
        # 在清洗bg data时， 确认正则表达式的正确性
        # [data_cal, 'check_re_str', 're_table', {}],
        text_col = '举例'# arg['text_col']
        re_col = '正则表达式'# arg['re_col']
        
        df = data[args[2]]
        
        for i in df.index:
            
            # 计算'match'列
            if str(df.loc[i, '类型']) == '2':
                m = re.search(df.loc[i, re_col], df.loc[i, text_col])
                if m:
                    df.loc[i, 'match'] = str(m.groups())
                else:
                    df.loc[i, 'match'] = None
            elif str(df.loc[i, '类型']) == '3':
                df.loc[i, 'match'] = str(re.findall(df.loc[i, re_col], df.loc[i, text_col]))
            else:
                df.loc[i, 'match'] = ''
                
            # 三种函数都使用一遍，用来辅助核对结果
            if str(df.loc[i, '类型']) in ('2', '3'):
                m = re.search(df.loc[i, re_col], df.loc[i, text_col])
                if m:
                    df.loc[i, 'group()'] = str(m.group())
                    df.loc[i, 'groups()'] = str(m.groups())
                df.loc[i, 'findall()'] = str(re.findall(df.loc[i, re_col], df.loc[i, text_col]))
        
        data[args[2]] = df
        
        return data
    
    def bg_data_re_clean(self, data, args=None):
        """
        用正则表达式表来测试数据
        """
        
        re_table = data['re_table']
        bg_data = data['bg_data']
        
        if 'result' not in bg_data.columns:
            bg_data['result'] = ''
        
        # 对每个市的正则表达式进行读取，再对这个市的相关变更数据进行清洗
        for re_i in re_table.index: 
            if str(re_table.loc[re_i, '类型']) in ('2', '3'):
                # 编译正则表达式
                re_str = re_table[re_i, '正则表达式']
                comp = re.compile(re_str)
                for bg_i in bg_data.index:
                    # 分情况处理数据
                    if str(re_table.loc[re_i, '类型']) == '2':
                        # 变更前的数据
                        m = comp.search(bg_data.loc[bg_i, 'altbe'])
                        bg_data.loc[bg_i, 'm_altbe'] = str(m.groups()) if m else None
                        
                        # 变更后的数据
                        m = comp.search(bg_data.loc[bg_i, 'altaf'])
                        bg_data.loc[bg_i, 'm_altaf'] = str(m.groups()) if m else None
                    else:
                        bg_data.loc[bg_i, 'm_altbe'] = str(comp.findall(bg_data.loc[bg_i, 'altbe']))
                        bg_data.loc[bg_i, 'm_altaf'] = str(comp.findall(bg_data.loc[bg_i, 'altaf']))
        
        data['re_table'] = re_table
        data['bg_data'] =  bg_data
        
        return data
                       
                        
                        
                        
                        
                        
                        
                        
                        
                
                