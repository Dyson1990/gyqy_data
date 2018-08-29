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

import wheels.wheels.oracle_connecter
oracle_connecter = wheels.wheels.oracle_connecter.oracle_connecter()

import wheels.wheels.mysql_connecter
mysql_connecter = wheels.wheels.mysql_connecter.mysql_connecter()

#import set_log  

#log_obj = set_log.Logger('get_data.log', set_log.logging.WARNING,
#                         set_log.logging.DEBUG)
#log_obj.cleanup('get_data.log', if_cleanup = True)  # 是否需要在每次运行程序前清空Log文件

pcname = os.getenv('computername')
if pcname == 'NOV06PC':
    oracle_args = {'user': 'VW_NOV06'
        , 'password': 'C372M5c590'
        , 'host': '172.17.32.2'
        , 'sid': 'orcl'
        , 'dbname': 'CDB_NOV'
        , 'charset': 'UTF8'
        , 'data_type': 'DataFrame'
        }
elif pcname == 'NOV10PC':
    oracle_args = {'user': 'VW_NOV10'
        , 'password': 'S457A7y545'
        , 'host': '172.17.32.2'
        , 'sid': 'orcl'
        , 'dbname': 'CDB_NOV_20180604'
        , 'charset': 'UTF8'
        , 'data_type': 'DataFrame'
        }

mysql_args = {'user': 'root'
    , 'password': '122321'
    , 'host': 'localhost'
    , 'dbname': 'gyqysj'
    , 'data_type': 'DataFrame'
    }

mysql_args2 = {'user': 'root'
    , 'password': '122321'
    , 'host': 'localhost'
    , 'dbname': 'bdgysj'
    , 'data_type': 'DataFrame'
    }

#lookup_list = ['诸暨', '余姚']

table_args = {
        '1998': {
            'ent_name':'B02',
            'district':'B056',
            'r_name':'B031',
            },
        '1999': {
            'ent_name':'企业名称',
            'district':'省地县码',
            'r_name':'法人代表姓名',
            },
        '2000': {
            'ent_name':'B02',
            'district':'B056',
            'r_name':'B031',
            },
        '2001': {
            'ent_name':'B02',
            'district':'B056',
            'r_name':'B031',
            },
        '2002': {
            'ent_name':'法人单位',
            'district':'省地县码',
            'r_name':'法人',
            },
        '2003': {
            'ent_name':'法人单位',
            'district':'省地县码',
            'r_name':'法人',
            },
        '2004': {
            'ent_name':'B02',
            'district':'B056',
            'r_name':'B031',
            },
        '2005': {
            'ent_name':'B02',
            'district':'B056',
            'r_name':'B031',
            },
        '2006': {
            'ent_name':'B02',
            'district':'B056',
            'r_name':'B031',
            },
        '2007': {
            'ent_name':'法人单位名称',
            'district':'行政区划代码',
            'r_name':'法定代表人',
            },
        '2008': {
            'ent_name':'法人单位',
            'district':'行政区别',
            'r_name':'法定代表人',
            },
        '2009': {
            'ent_name':'法人单位',
            'district':'行政区别',
            'r_name':'法定代表人',
            },
        '2012': {
            'ent_name':'详细名称',
            'district':'省码',
            'r_name':'法定代表人',
            },
        '2013': {
            'ent_name':'单位详细名称',
            'district':'_行政区划代码',
            'r_name':'法定代表人',
            },
        }

# 北大
table_args2 = {
        '1998': {
            'ent_name':'企业名称',
            'district':'省地县码',
            'r_name':'法定代表人',
            },
        '1999': {
            'ent_name':'企业名称',
            'district':'省地县码',
            'r_name':'法定代表人',
            },
        '2000': {
            'ent_name':'企业名称',
            'district':'省地县码',
            'r_name':'法定代表人',
            },
        '2001': {
            'ent_name':'企业名称',
            'district':'省码',
            'r_name':'法定代表人',
            },
        '2002': {
            'ent_name':'企业名称',
            'district':'省地县码',
            'r_name':'法定代表人',
            },
        '2003': {
            'ent_name':'企业名称',
            'district':'省地县码',
            'r_name':'法定代表人',
            },
        '2004': {
            'ent_name':'企业名称',
            'district':'行政区划代码',
            'r_name':'法定代表人',
            },
        '2005': {
            'ent_name':'企业名称',
            'district':'行政区划代码',
            'r_name':'法定代表人',
            },
        '2006': {
            'ent_name':'企业名称',
            'district':'行政区划代码',
            'r_name':'法定代表人',
            },
        '2007': {
            'ent_name':'企业名称',
            'district':'行政区划代码',
            'r_name':'法定代表人',
            },
        '2008': {
            'ent_name':'企业名称',
            'district':'省地县码',
            'r_name':'法定代表人',
            },
        '2009': {
            'ent_name':'企业名称',
            'district':'行政区划代码',
            'r_name':'法定代表人',
            },
        '2010': {
            'ent_name':'企业名称',
            'district':'行政区划代码',
            'r_name':'法定代表人',
            },
        '2011': {
            'ent_name':'企业名称',
            'district':'行政区划代码',
            'r_name':'法定代表人',
            },
        '2012': {
            'ent_name':'企业名称',
            'district':'行政区划代码',
            'r_name':'法定代表人',
            },
        '2013': {
            'ent_name':'企业名称',
            'district':'行政区划代码',
            'r_name':'法定代表人',
            },
        }

class get_data(object):

    def __init__(self):
        pass
    
    def csv_data(self, data, args=None):
        """
        从csv文件中读取已经清洗好的数据
        """
        start_time = time.time()
        arg = args[-1]
        
        file_path = arg['file_path']
        table_name = arg['table_name']
        
        if os.path.exists(file_path):
        
            with codecs.open(file_path,'r','utf-8') as f0:
                df = pd.read_csv(f0, dtype=np.str, index_col=0)
                
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
        从csv文件中读取已经清洗好的数据
        """
        start_time = time.time()
        arg = args[-1]
        
        dir_path = arg['dir_path']
        table_name = arg['table_name']
        
        if os.path.exists(dir_path):
            df_sum = pd.DataFrame([])
            for file_name in os.listdir(dir_path):
                with codecs.open(os.path.join(dir_path, file_name),'r','utf-8') as f0:
                    df0 = pd.read_csv(f0, dtype=np.str, index_col=0)
                
                    if df0 is None:
                        continue
                    df_sum = pd.concat([df_sum, df0])

            df_sum.columns = [s.lower() for s in df_sum.columns]
            data[table_name] = df_sum
            print('总数据：%s行, 读取耗时：%s' % (df_sum.shape[0], time.time() - start_time))
            
            return data
        else:
            data[table_name] = pd.DataFrame([])
            return data

        
    def jbxx_data(self, args=None):
        start_time = time.time()
        thread_id = args['thread_id']
        q_len = args['q_len']
        
        sql = """
        SELECT * FROM
        (
            SELECT ENTID, DOM, ROWNUM r
            FROM F_QY_JBXX
            ORDER BY ROWID
        ) temp
        WHERE r BETWEEN {} AND {}
        """.format(thread_id * q_len, (thread_id+1) * q_len)

        print(sql)

        global oracle_args

        oracle_args['method'] = 'LOB'
        df = oracle_connecter.connect(sql, oracle_args)
        if df is None:
            df = pd.DataFrame([])
        df.columns = [s.lower() for s in df.columns]
        
        print('总数据：%s行, 读取耗时：%s' % (df.shape[0], time.time() - start_time))

        return df


if __name__ == '__main__':
    get_data = get_data()

    get_data.data_edit()