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

import wheels.oracle_connecter
oracle_connecter = wheels.oracle_connecter.oracle_connecter()


#import set_log  

#log_obj = set_log.Logger('get_data.log', set_log.logging.WARNING,
#                         set_log.logging.DEBUG)
#log_obj.cleanup('get_data.log', if_cleanup = True)  # 是否需要在每次运行程序前清空Log文件


oracle_args = {'user': 'VW_NOV06'
    , 'password': 'C372M5c590'
    , 'host': '172.17.32.2'
    , 'sid': 'orcl'
    , 'dbname': 'CDB_NOV'
    , 'charset': 'UTF8'}

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
            data[table_name] = df
            
            df.columns = [s.lower() for s in df.columns]
            
            print('总数据：%s行, 读取耗时：%s' % (df.shape[0], time.time() - start_time))
            
            return data
        else:
            data[table_name] = pd.DataFrame([])
            return data

    def get_bg_data(self, data, args=None):
        """
        读取全量数据
        :return:
        """

        start_time = time.time()
        lookup_args = args[-1]
        
        prov_code = lookup_args['code']
        
        sql_str = 'SUBSTR(F_QY_JBXX.DISTRICT, 0, {}) = {}'.format(len(prov_code), prov_code)

        sql = """
        SELECT 
            F_QY_JBXX.ENTID                                   --JBXX中的企业ID
            , F_QY_JBXX.DISTRICT                              --JBXX中的省地县码
            , to_char(F_QY_BG.ALTBE) ALTBE                    --BG中的变更前内容
            , to_char(F_QY_BG.ALTAF) ALTAF                    --BG中的变更后内容
            , F_QY_BG.ALTDATE                                 --BG中的变更时间
        FROM F_QY_JBXX
        LEFT JOIN F_QY_BG                                     --JBXX左连接BG
        ON F_QY_JBXX.ENTID = F_QY_BG.ENTID
        WHERE (%s)                                            --筛选市码
        AND F_QY_BG.ALTITEM = '30'                            --筛选变更事项
        """ %sql_str

        global oracle_args

        df = oracle_connecter.df_output(sql, oracle_args)
        df.columns = [s.lower() for s in df.columns]
        
        data['bg_data'] = df

        print('总数据：%s行, 读取耗时：%s' % (df.shape[0], time.time() - start_time))

        return data


if __name__ == '__main__':
    get_data = get_data()

    get_data.data_edit()