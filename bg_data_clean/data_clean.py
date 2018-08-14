# -*- coding:utf-8 -*-  
#/usr/bin/python3
"""
--------------------------------
    @Author: Dyson
    @Contact: Weaver1990@163.com
    @file: data_clean.py
    @time: 2018/7/2 9:00
--------------------------------
"""
import sys
import os
import numpy as np
import pandas as pd
import re
import functools
import datetime

import wheels.oracle_connecter
oracle_connecter = wheels.oracle_connecter.oracle_connecter()

#import set_log  

#log_obj = set_log.Logger('data_clean.log', set_log.logging.WARNING,
#                         set_log.logging.DEBUG)
#log_obj.cleanup('data_clean.log', if_cleanup = True)  # 是否需要在每次运行程序前清空Log文件

district_name = pd.read_csv('district_code.csv') # 第一列不作为index

class data_clean(object):

    def __init__(self):
        pass

    def name2id_list(self, data, args=None):
        """
        将曾用名与现用名混合在一起，赋予现用名的唯一ID
        """

        df = data[args[2]]
        if df.empty:
            return data

        df['lookup_name'] = df['entname'] + df['oldname_list'].apply(
                lambda s: ',' + s if isinstance(s, str) or isinstance(s, np.str) else '')

        name_str = df['lookup_name'].str.split(',', expand=True)
        # print("name_str: \n%s\n%s" %(type(name_str), name_str))
        new_col = name_str.stack().reset_index(level=1, drop=True).rename('lookup_name')
        # print("new_col: \n" + new_col)

        df = df.drop('lookup_name', axis=1).join(new_col)

        data[args[2]] = df.drop(['entname', 'oldname_list'], axis=1)
        
        return data
    
    def add_id(self, data, args=None):
        # 增加自定义的ID， 用来跟踪数据
        df = data[args[2]]
        if df.empty:
            return data
        
        
        id_list = range(df.shape[0])
        str_len = len(str(id_list[-1]))
        str_type = '{{:0>%sd}}' %str_len
        df['myid'] = [str_type.format(i) for i in id_list]
        
        data[args[2]] = df
        return data

    def punctuation_clean(self, data, args=None):
        # 清除企业名称中一些扰乱匹配的标点
        # args = {'name_col'：****}

        df = data[args[2]]
        if df.empty:
            return data

        name_col = args[-1]['name_col']

        df[name_col] = df[name_col].apply(lambda s: re.sub(r'\*|-|\.|\d|\s|？|\?|·|ⅳ', '', str(s)).replace('（', '(').replace('）', ')'))

        data[args[2]] = df
        return data

    def col_comb(self, data, args=None):
        # 将各个列的字符串合并

        df = data[args[2]]
        if df.empty:
            return data

        col_list = args[-1]['col_list']

        df['lookup_name'] = functools.reduce(lambda a, b: df[a] + '|' + df[b], col_list)

        data[args[2]] = df

        return data

    def company_type_split(self, data, args=None):
        # 分离出企业属性
        df = data[args[2]]
        if df.empty:
            return data

        comp = re.compile(r'(股份有限公司$|有限责任公司$|有限公司$|实业总公司$|实业公司$|总公司$|公司$|总厂$|厂$|集团$)')
        df['company_type1'] = df['lookup_name'].str.extract(comp)
        df['lookup_name'] = df['lookup_name'].str.replace(comp, '')

        comp = re.compile(r'(^中外合资|^中国)')
        df['company_type2'] = df['lookup_name'].str.extract(comp)
        df['lookup_name'] = df['lookup_name'].str.replace(comp, '')

        data[args[2]] = df

        return data

    def place_split(self, data, args=None):
        # 分离企业的
        df = data[args[2]]
        if df.empty:
            return data
        
        arg = args[-1]

        global district_name

        ser = district_name[arg['str_col']].copy()
        re_set = set(ser)
        re_set = {s.replace('[', '').replace(']', '') for s in re_set} # 暂时先清除[]，过去的县市与现在的混在一起
        re_set = {'^' + s + '?' if len(s)>2 else '^' + s for s in re_set} # 加上正则表达式
        re_str = r'(%s)' %('|'.join(re_set))
        comp = re.compile(re_str)
        # print(comp.search('诸暨市').group())

        # df['company_type'] = df['lookup_name'].apply(lambda s: comp.search(s).group() if comp.search(s) else '')
        # df['lookup_name'] = df['lookup_name'].apply(lambda s: comp.sub('', s) if comp.search(s) else s)
        df[arg['str_col']] = ''
        df[arg['str_col']] = df['lookup_name'].str.extract(comp)
        df['lookup_name'] = df['lookup_name'].str.replace(comp, '')

        data[args[2]] = df

        return data
    
    def bracket_clean(self, data, args=None):
        # 清理括号
        
        df = data[args[2]]
        if df.empty:
            return data

        comp = re.compile(r'(\(.+?\))')
        df['bracket'] = df['lookup_name'].str.extract(comp)
        df['lookup_name'] = df['lookup_name'].str.replace(comp, '')
        
        data[args[2]] = df

        return data
    

    def data_extract(self, data, args=None):
        pro_code = args[-1]['pro_code']
        
        
        sql = """
        SELECT 
            F_QY_JBXX.ENTID                             --JBXX中的企业ID
            , F_QY_JBXX.district                        --JBXX中的省地县码
            , to_char(F_QY_BG.ALTBE) ALTBE              --BG中的变更前内容
            , to_char(F_QY_BG.ALTAF) ALTAF              --BG中的变更后内容
            , F_QY_BG.ALTDATE                           --BG中的变更时间
        FROM F_QY_JBXX
        LEFT JOIN F_QY_BG                               --JBXX左连接BG
        ON F_QY_JBXX.ENTID = F_QY_BG.ENTID
        WHERE SUBSTR(F_QY_JBXX.district, 0, 4) = %s     --筛选市码
        AND F_QY_BG.ALTITEM = '03'                      --筛选变更事项
        AND (
             LENGTH(F_QY_BG.ALTBE) > 3                  --┓
             OR                                         --┨=>筛选变更内容长度大于3
             LENGTH(F_QY_BG.ALTAF) > 3                  --┛
            ) 
        """ %pro_code

        oracle_args = {'user': 'VW_NOV06'
                        , 'password': 'C372M5c590'
                        , 'host': '172.17.32.2'
                        , 'sid': 'orcl'
                        , 'dbname': 'CDB_NOV'
                        , 'charset': 'UTF8'}

        df = oracle_connecter.df_output(sql, oracle_args)
        if df.empty:
            return data
        df.columns = [s.lower() for s in df.columns]
        
        with data['lock']:
            df.to_csv('test.csv')
        
        comp = re.compile(r'\s')
        df['altbe'] = df['altbe'].str.replace(comp, '')
        df['altaf'] = df['altaf'].str.replace(comp, '')
        

        # 解决标点不同的干扰
        d = {
            '：':':',
            '；':' ',
            ';':' ',
            ',':'|'
                }
        for key in d:
            df['altbe'] = df['altbe'].str.replace(key, d[key])
            df['altaf'] = df['altaf'].str.replace(key, d[key])
        
        
        comp = re.compile(r'((?<=姓名:).+?\s)')
        
        df['altbe_new'] = df['altbe'].str.extract(comp)
        df['altaf_new'] = df['altaf'].str.extract(comp)
        
        comp = re.compile(r'\s')
        df['altbe'] = df['altbe'].str.replace(comp, '')
        df['altaf'] = df['altaf'].str.replace(comp, '')
        
        path = 'D:/r_name_list/'
        if not os.path.exists(path):
            os.mkdir(path)
        
        df.to_csv(path + pro_code + '.csv')
        
        return data
    
    def fill_na(self, data, args=None):
        # 若参数应为{被替换的列：替换列或者替换字符}，
        # 字典中的值存在于df的列名中，
        # 则用列中的值来填充，否则用fill_by中的字符串填充
        df = data[args[2]]
        if df.empty:
            return data
        
        arg = args[-1]
        
        for key in arg:
            if arg[key] in df.columns:
                df[key] = df[key].fillna(df[arg[key]])
            else:
                df[key] = df[key].fillna(arg[key])
                
        data[args[2]] = df
        return data
        
    
    def row_comb(self, data, args=None):
        # 行之间的字符串合并
        df = data[args[2]]
        if df.empty:
            return data
        arg = args[-1]
        
        groupby_col = arg['groupby_col']
        comb_col_list = arg['comb_col']
        df0 = df[comb_col_list].groupby(df[groupby_col]).aggregate(lambda x: '|'.join(x))
        df0 = df0.rename({s: s.replace('_new', '') for s in df0.columns}, axis=1)
        df0 = df0.reset_index()
        df0 = df0.reindex(['entid', 'altbe', 'altaf'], axis=1)

        data[args[2]] = df0
        return data
        
        
        

if __name__ == '__main__':
    data_clean = data_clean()