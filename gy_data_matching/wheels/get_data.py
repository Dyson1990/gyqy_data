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

        

    def parent_data(self, data, args=None):
        """
        读取全量数据
        :return:
        """

        start_time = time.time()
        lookup_args = args[-1]
        table_name = args[2]
        
        print(lookup_args)
        lookup_year = lookup_args['year']
        lookup_list = lookup_args['list']
        
        len0 = max([len(s) for s in lookup_list])
        sql_str = ['SUBSTR(JBXX0.DISTRICT, 0, {}) = {}'.format(len0, s) for s in lookup_list]

        sql = """
        WITH JBXX AS
        (
            SELECT
                JBXX0.ENTID
                , JBXX0.ENTNAME
                , JBXX0.OLDNAME_LIST
                , JBXX0.ESDATE
                , JBXX0.DISTRICT
                , JBXX0.NAME R_NAME
            FROM CDB_NOV.F_QY_JBXX JBXX0
            WHERE (%s) --AND (JBXX0.ESDATE < to_date('%s-01-01 00:00:00','yyyy-mm-dd hh24:mi:ss'))
            --ORDER BY JBXX0.ESDATE DESC
        )

        SELECT
            JBXX.ENTID
            , JBXX.ENTNAME
            , JBXX.OLDNAME_LIST
            , JBXX.DISTRICT
            , JBXX.R_NAME
            , JBXX.ESDATE
        FROM JBXX
        """ % (' OR '.join(sql_str), int(lookup_year) + 1)

        print(sql)

        global oracle_args

        oracle_args['method'] = 'LOB'
        df = oracle_connecter.connect(sql, oracle_args)
        if df is None:
            df = pd.DataFrame([])
        df.columns = [s.lower() for s in df.columns]
        
        data[table_name] = df

        print('总数据：%s行, 读取耗时：%s' % (df.shape[0], time.time() - start_time))

        return data


    def test_data_shen(self, data, args=None):
        """
        读取需要进行匹配的工业企业数据
        
        :return:
        """
        start_time = time.time()
        lookup_args = args[-1]
        table_name = args[2]
        
        print(lookup_args)
        lookup_year = lookup_args['year']
        lookup_list = lookup_args['list']
        
        global table_args

        ent_name = table_args[lookup_year]['ent_name']
        district = table_args[lookup_year]['district']
        r_name = table_args[lookup_year]['r_name']

        # sql_str = ['B02 LIKE \'%{}%\''.format(s) for s in lookup_list]
        len0 = max([len(s) for s in lookup_list])
        sql_str = ['LEFT({}, {}) = {}'.format(district, len0, s) for s in lookup_list]


        sql = """
        SELECT
            `%s` AS `lookup_name`
            , `%s` AS `entname` 
            , `%s` AS `district`
            , `%s` AS `r_name`
        FROM y%s

        WHERE %s
        """ % (ent_name, ent_name, district, r_name, lookup_year, ' OR '.join(sql_str))
        
        print(sql)

        global mysql_args

        df = mysql_connecter.connect(sql, mysql_args)
        if df is None:
            df = pd.DataFrame([])
        df.columns = [s.lower() for s in df.columns]
        
        data[table_name] = df

        print('样本数据：%s行, 读取耗时：%s' % (df.shape[0], time.time() - start_time))

        return data
    
    def test_data_gsgyqy(self, data, args=None):
        """
        读取需要进行匹配的工业企业数据

        :return:
        """
        start_time = time.time()
        lookup_args = args[-1]
        table_name = args[2]
        
        print(lookup_args)
        lookup_year = lookup_args['year']
        lookup_list = lookup_args['list']

        len0 = max([len(s) for s in lookup_list])
        sql_str = ['SUBSTR(fqy_xzqhdm, 0, {}) = \'{}\''.format(len0, s) for s in lookup_list]

        sql = """
        SELECT
            fqy_dwmc lookup_name
            , fqy_dwmc entname 
            , fqy_xzqhdm district
            , fqy_fddbr r_name
        FROM F_GSGYQY

        WHERE (fqy_time = '%s') AND (%s)
        """ % (lookup_year, ' OR '.join(sql_str))
        
        print(sql)

        global oracle_args

        df = oracle_connecter.connect(sql, oracle_args)
        if df is None:
            df = pd.DataFrame([])
        df.columns = [s.lower() for s in df.columns]
        
        data[table_name] = df

        print('样本数据：%s行, 读取耗时：%s' % (df.shape[0], time.time() - start_time))

        return data
    
    def test_data_beida(self, data, args=None):
        """
        读取需要进行匹配的工业企业数据

        :return:
        """
        start_time = time.time()
        lookup_args = args[-1]
        table_name = args[2]
                
        print(lookup_args)
        lookup_year = lookup_args['year']
        lookup_list = lookup_args['list']
        
        global table_args2

        ent_name = table_args2[lookup_year]['ent_name']
        district = table_args2[lookup_year]['district']
        r_name = table_args2[lookup_year]['r_name']


        # sql_str = ['B02 LIKE \'%{}%\''.format(s) for s in lookup_list]
        len0 = max([len(s) for s in lookup_list])
        sql_str = ['LEFT({}, {}) = {}'.format(district, len0, s) for s in lookup_list]



        sql = """
        SELECT
            `%s` AS `lookup_name`
            , `%s` AS `entname` 
            , `%s` AS `district`
            , `%s` AS `r_name`
        FROM y%s

        WHERE %s
        """ % (ent_name, ent_name, district, r_name, lookup_year, ' OR '.join(sql_str))
        
        print(sql)

        global mysql_args2

        df = mysql_connecter.connect(sql, mysql_args2)
        if df is None:
            df = pd.DataFrame([])
        df.columns = [s.lower() for s in df.columns]
        
        data[table_name] = df

        print('样本数据：%s行, 读取耗时：%s' % (df.shape[0], time.time() - start_time))

        return data
    
    def f_trade_info_2017(self, data, args=None):
        # 读取海关数据
        start_time = time.time()
        lookup_args = args[-1]
        table_name = args[2]
        
        print(lookup_args)

        sql = """
        SELECT
            DISTINCT(ENTNAME)
        FROM F_TRADE_INFO_2017
        """
        print(sql)

        global oracle_args

        df = oracle_connecter.connect(sql, oracle_args)
        if df is None:
            df = pd.DataFrame([])
        df.columns = [s.lower() for s in df.columns]
        
        data[table_name] = df

        print('总数据：%s行, 读取耗时：%s' % (df.shape[0], time.time() - start_time))

        return data
        


    def r_name_list(self, data, args=None):
        """
        读取变量表中的法人姓名

        :return:
        """
        start_time = time.time()
        lookup_args = args[-1]
        table_name = args[2]
        
        print(lookup_args)
        lookup_list = lookup_args['list']
        
        len0 = max([len(s) for s in lookup_list])

        sql_str = ['SUBSTR(JBXX0.DISTRICT, 0, {}) = {}'.format(len0, s) for s in lookup_list]

        sql = """
        SELECT
            JBXX0.ENTID
            , JBXX0.DISTRICT
            , BG0.ALTBE
            , BG0.ALTAF
            , BG0.ALTDATE
        FROM CDB_NOV.F_QY_JBXX JBXX0
        LEFT JOIN CDB_NOV.F_QY_BG BG0
        ON JBXX0.ENTID = BG0.ENTID
        WHERE %s
        AND BG0.ALTITEM = '03'

        """ % (' OR '.join(sql_str))

        print(sql)

        global oracle_args
        
        oracle_args['method'] = 'LOB'
        df = oracle_connecter.connect(sql, oracle_args)
        if df is None:
            df = pd.DataFrame([])
        df.columns = [s.lower() for s in df.columns]
        
        data[table_name] = df

        print('总数据：%s行, 读取耗时：%s' % (df.shape[0], time.time() - start_time))

        return data

    def jbxx_data_entname(self, data , args=None):
        # 读取jbxx中的企业名称和曾用名，以备清洗
        
        start_time = time.time()
        lookup_args = args[-1]
        table_name = args[2]
        
        print(lookup_args)
        lookup_list = lookup_args['list']
        
        len0 = max([len(s) for s in lookup_list])

        sql_str = ['SUBSTR(DISTRICT, 0, {}) = {}'.format(len0, s) for s in lookup_list]
        
        sql = """
        SELECT
            ENTID
            , DISTRICT
            , ENTNAME
            , OLDNAME_LIST
        FROM CDB_NOV.F_QY_JBXX
        WHERE %s
        """ %(' OR '.join(sql_str))
        print(sql)

        global oracle_args

        oracle_args['method'] = 'LOB'
        df = oracle_connecter.connect(sql, oracle_args)
        if df is None:
            df = pd.DataFrame([])
        df.columns = [s.lower() for s in df.columns]
        
        data[table_name] = df

        print('总数据：%s行, 读取耗时：%s' % (df.shape[0], time.time() - start_time))

        return data
        
        
    
    def add_data(self, data , args=None):
        # 复制一个阶段的数据作为备份
        arg = args[-1]
        data[arg['new']] = data[arg['old']].copy()
        return data

    def test_data_beida0(self, data, args=None):
        """
        读取需要进行匹配的工业企业数据
        :return:
        """
        start_time = time.time()        
        global table_args2
        table_name = args[2]

        sql = """
        SELECT
            `企业名称` AS `entname` 
            , `组织机构代码` AS `entid`
            , `data_year`
        FROM y{}
        """
        sql = '\nUNION ALL\n'.join([sql.format(i) for i in range(1998, 2008)])
        
        print(sql)

        global mysql_args2

        df = mysql_connecter.connect(sql, mysql_args2)
        if df is None:
            df = pd.DataFrame([])
        df.columns = [s.lower() for s in df.columns]
        
        data[table_name] = df

        print('样本数据：%s行, 读取耗时：%s' % (df.shape[0], time.time() - start_time))

        return data


    def parent_data0(self, lookup_args):
        """
        暂时弃用

        读取全量数据
        :return:
        """

        start_time = time.time()

        lookup_year = lookup_args['year']
        lookup_list = lookup_args['list']

        # sql_str1 = ['JBXX.ENTNAME LIKE \'%{}%\''.format(s) for s in lookup_list]
        # sql_str2 = ['OLDNAME_LIST LIKE \'%{}%\''.format(s) for s in lookup_list]
        sql_str = ['SUBSTR(JBXX0.DISTRICT, 0, 4) = {}'.format(s) for s in lookup_list]

        sql = """
        WITH JBXX AS
        (
            SELECT
                JBXX0.ENTID
                , JBXX0.ENTNAME
                , TO_CHAR(JBXX0.OLDNAME_LIST) OLDNAME_LIST
                , JBXX0.ESDATE
                , JBXX0.DISTRICT
                , JBXX0.NAME R_NAME
            FROM CDB_NOV.F_QY_JBXX JBXX0
            WHERE %s AND JBXX0.ESDATE < to_date('%s-01-01 00:00:00','yyyy-mm-dd hh24:mi:ss')
            ORDER BY JBXX0.ESDATE DESC
        ),
        t1 AS
        (
            SELECT 
                *
            FROM (
                SELECT 
                    ENTID
                    , ALTDATE
                    , TO_CHAR(ALTAF) ALTAF
                    , ALTITEM
                    , ROW_NUMBER() OVER(PARTITION BY ENTID ORDER BY ALTDATE DESC) row_index
                FROM CDB_NOV.F_QY_BG
                WHERE ALTITEM = '03' AND ALTDATE < to_date('%s-01-01 00:00:00','yyyy-mm-dd hh24:mi:ss')
            ) t0
            WHERE row_index = 1
        )
        
        SELECT
            JBXX.ENTID
            , JBXX.ENTNAME
            , JBXX.OLDNAME_LIST
            , JBXX.DISTRICT
            , JBXX.R_NAME
            , JBXX.ESDATE
            , t1.ALTITEM
            , t1.ALTDATE
            , t1.ALTAF
        FROM JBXX
        LEFT JOIN t1 
        ON JBXX.ENTID = t1.ENTID

        """ %(' OR '.join(sql_str), int(lookup_year) + 1, int(lookup_year) + 1)

        print(sql)

        global oracle_args

        df = oracle_connecter.connect(sql, oracle_args)
        df.columns = [s.lower() for s in df.columns]
        

        print('总数据：%s行, 读取耗时：%s' %(df.shape[0], time.time()-start_time))

        # mysql_connecter.create_table(set(df.columns), 'temp', mysql_args)
        # mysql_connecter.insert_df_data(df.drop_duplicates(), 'temp', mysql_args)

        return df


if __name__ == '__main__':
    get_data = get_data()

    get_data.data_edit()