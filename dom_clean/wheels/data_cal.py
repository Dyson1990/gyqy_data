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
import json
import itertools
import time


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
    
    def value_filter(self, data, args=None):
        # [data_cal, 'bg_data_re_clean', 'bg_30_origin', {'filter_col':, 'filter_value':}],
        arg = args[-1]
        df = data[args[2]]
        
        filter_col = arg['filter_col']
        filter_value = arg['filter_value']
        
        df = df[df[filter_col]==filter_value]
        
        data[args[2]] = df
        
        return data
    
    def re_table_value_filter(self, data, args=None):
        # 专用
        df = data[args[2]]
        # df = df.dropna(subset=['市码', '类型'])
        df['市码'] = df['市码'].astype(np.int).astype(np.str)
        df['类型'] = df['类型'].astype(np.int).astype(np.str)

        prov_code = str(data['info']['province_code'])
        
        df = df[(df['市码']==prov_code)
                & ((df['类型']=='2') | (df['类型']=='3'))]
        
        data[args[2]] = df
        
        return data

    
    def check_re_str(self, data, args=None):
        # 在清洗bg data时， 确认正则表达式的正确性
        # [data_cal, 'check_re_str', 're_table', {}],
        text_col = '举例'# arg['text_col']
        re_col = '正则表达式'# arg['re_col']
        
        df = data[args[2]]
        
        for i in df.index:
            # 若正则表达式本身就有错无法编译，则跳过运行下一条
            try:
                comp = re.compile(df.loc[i, re_col])
            except:
                continue
            
            # 计算'match'列
            if str(df.loc[i, '类型']) == '2':
                m = comp.search(df.loc[i, text_col])
                if m:
                    df.loc[i, 'match'] = str(m.groups())
                else:
                    df.loc[i, 'match'] = None
            elif str(df.loc[i, '类型']) == '3':
                df.loc[i, 'match'] = str(comp.findall(df.loc[i, text_col]))
            else:
                df.loc[i, 'match'] = ''
                
            # 三种函数都使用一遍，用来辅助核对结果
            if str(df.loc[i, '类型']) in ('2', '3'):
                m = comp.search(df.loc[i, text_col])
                if m:
                    df.loc[i, 'group()'] = str(m.group())
                    df.loc[i, 'groups()'] = str(m.groups())
                df.loc[i, 'findall()'] = str(comp.findall(df.loc[i, text_col]))
        
        data[args[2]] = df
        
        return data
    
    def bg_data_re_clean(self, data, args=None):
        """
        用正则表达式表来测试数据
        """
        
        re_table = data['re_table']
        bg_data = data['bg_30_origin']
        bg_data = bg_data.fillna('')
        
        # 初始化表格
        if 'm_altbe2' not in bg_data:
            bg_data['m_altbe'] = json.dumps({}, ensure_ascii=False)
        if 'm_altaf2' not in bg_data:
            bg_data['m_altaf'] = json.dumps({}, ensure_ascii=False)
# =============================================================================
#         if 'result' not in bg_data.columns:
#             bg_data['result'] = json.dumps({}, ensure_ascii=False)
#         if 'm_altbe2' not in bg_data:
#             bg_data['m_altbe2'] = ''
#         if 'm_altaf2' not in bg_data:
#             bg_data['m_altaf2'] = ''
#         if 'm_altbe3' not in bg_data:
#             bg_data['m_altbe3'] = ''
#         if 'm_altaf3' not in bg_data:
#             bg_data['m_altaf3'] = ''
# =============================================================================
        
        # 对每个市的正则表达式进行读取，再对这个市的相关变更数据进行清洗
        for re_i, bg_i in itertools.product(re_table.index,bg_data.index): 
            # 直接略过不需要使用正则表达式的行
            re_type = str(re_table.loc[re_i, '类型'])
            if re_type not in ('2', '3'):
                continue
            
            # 编译正则表达式
            re_str = re_table.loc[re_i, '正则表达式']
            try:
                comp = re.compile(re_str)
            except:
                continue
            
            altbe = bg_data.loc[bg_i, 'altbe']
            altaf = bg_data.loc[bg_i, 'altaf']
            info = re_table.loc[re_i, '字段说明']
            
            # 分情况处理数据
            if re_type == '3':

                # 变更前的数据
                m_altbe = bg_data.loc[bg_i, 'm_altbe']
                m_altbe = json.loads(m_altbe)
                res_l = comp.findall(altbe)
                if res_l:
                    m_altbe[info] = res_l
                
                # 变更后的数据
                m_altaf = bg_data.loc[bg_i, 'm_altaf']
                m_altaf = json.loads(m_altaf)
                res_l = comp.findall(altaf)
                if res_l:
                    m_altaf[info] = res_l
               
            else:
                info = '{%s}' %info.replace(';', ',')
                title = eval(info)
                
                # 变更前的数据
                m_altbe = bg_data.loc[bg_i, 'm_altbe']
                m_altbe = json.loads(m_altbe)
                m = comp.search(altbe)
                if m:
                    res = {title[k]: m.groups()[k] for k in title}
                    m_altbe['groups_data'] = res
                
                # 变更后的数据
                m_altaf = bg_data.loc[bg_i, 'm_altaf']
                m_altaf = json.loads(m_altaf)
                m = comp.search(altaf)
                if m:
                    res = {title[k]: m.groups()[k] for k in title}
                    m_altbe['groups_data'] = res
            
            # 将数据填入dataframe
            bg_data.loc[bg_i, 'm_altbe'] = json.dumps(m_altbe, ensure_ascii=False)
            bg_data.loc[bg_i, 'm_altaf'] = json.dumps(m_altaf, ensure_ascii=False)
        
        data['re_table'] = re_table
        data['bg_30_origin'] =  bg_data
        
        return data
    
def is_in_polygon(lon, lat, point_list):  
    ''''' 
    :param lon: double 经度 
    :param lat: double 纬度 
    :param point_list: list [(lon, lat)...] 多边形点的顺序需根据顺时针或逆时针，不能乱 
    '''  
    i_sum = 0  
    i_count = len(point_list)  
    
    # 若多边形的列表中的点不够构成一个多边形
    if i_count < 3 :  
        return False  
      
    for i in range(i_count):  
          
        pLon1 = point_list[i][0]  
        pLat1 = point_list[i][1]  
          
        if(i == i_count - 1):  
              
            pLon2 = point_list[0][0]  
            pLat2 = point_list[0][1]  
        else:  
            pLon2 = point_list[i + 1][0]  
            pLat2 = point_list[i + 1][1]  
          
        if ((lat >= pLat1) and (lat < pLat2)) or ((lat>=pLat2) and (lat < pLat1)):  
              
            if (abs(pLat1 - pLat2) > 0):
                  
                pLon = pLon1 - ((pLon1 - pLon2) * (pLat1 - lat)) / (pLat1 - pLat2);  
                  
                if(pLon < lon):  
                    i_sum += 1  
  
    if(i_sum % 2 != 0):  
        return True  
    else:  
        return False  
                       
                        
                        
                        
                        
                        
                        
                        
                        
                
                