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
import codecs

import wheels.wheels.oracle_connecter
oracle_connecter = wheels.wheels.oracle_connecter.oracle_connecter()

#import set_log  

#log_obj = set_log.Logger('data_clean.log', set_log.logging.WARNING,
#                         set_log.logging.DEBUG)
#log_obj.cleanup('data_clean.log', if_cleanup = True)  # 是否需要在每次运行程序前清空Log文件

with codecs.open("%s\\args\\district_code.csv" %os.getcwd(),'r','utf-8') as f0:
    district_name = pd.read_csv(f0, dtype=np.str) # 第一列不作为index

class data_clean(object):

    def __init__(self):
        pass

    def name2id_list(self, data, args=None):
        """
        清洗企业名称时候专用
        将曾用名与现用名混合在一起，赋予现用名的唯一ID
        """

        df = data[args[2]]
        if df.empty:
            return data

# =============================================================================
#         df['lookup_name'] = df['entname'] + df['oldname_list'].apply(
#                 lambda s: ',' + s if isinstance(s, str) or isinstance(s, np.str) else '')
# 
#         name_str = df['lookup_name'].str.split(',', expand=True)
#         # print("name_str: \n%s\n%s" %(type(name_str), name_str))
#         new_col = name_str.stack().reset_index(level=1, drop=True).rename('lookup_name')
#         # print("new_col: \n" + new_col)
# 
#         df = df.drop('lookup_name', axis=1).join(new_col)
# 
#         data[args[2]] = df.drop(['entname', 'oldname_list'], axis=1)
# =============================================================================
            
        df_cur_name = df.drop(['oldname_list', ], axis=1)
        df_cur_name['name_type'] = 'current'
        
        df_old_name = df.drop(['entname', ], axis=1)
        
        # 将target_col列分割后， 生成新的列，
        name_str = df_old_name['oldname_list'].str.split(',', expand=True)
        new_col = name_str.stack().reset_index(level=1, drop=True).rename('oldname_list')
        
        # 删除旧的target_col列，将前面生成的target_col列组合进去
        df_old_name = df_old_name.drop('oldname_list', axis=1).join(new_col) # 默认是左连接
        df_old_name['name_type'] = 'old'
        
        # 改字段名
        df_cur_name = df_cur_name.rename(columns={'entname': 'lookup_name'})
        df_old_name = df_old_name.rename(columns={'oldname_list': 'lookup_name'})
        
        # 去除企业名是重复的行
        df_cur_name = df_cur_name.dropna(subset=['lookup_name', ])
        df_old_name = df_old_name.dropna(subset=['lookup_name', ])
        
        df = pd.concat([df_cur_name,df_old_name])
        
        data[args[2]] = df
        return data
    
    def split2rows(self, data, args=None):
        """
        以某一个字段的数据为依据，将数据分成按某个符号分割成多行数据
        例如：
        1 a,a
        2 b,c,d
        ==>
        1 a
        1 a
        2 b
        2 c
        2 d
        """
        df = data[args[2]]
        arg = args[-1]
        if df.empty:
            return data
        
        target_col = arg['target_col']
        split_sign = arg['split_sign']
        
        # 将target_col列分割后， 生成新的列，
        name_str = df[target_col].str.split(split_sign, expand=True)
        new_col = name_str.stack().reset_index(level=1, drop=True).rename(target_col)
        
        # 删除旧的target_col列，将前面生成的target_col列组合进去
        df = df.drop(target_col, axis=1).join(new_col) # 默认是左连接
        
        data[args[2]] = df
        
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

    def entname_punctuation_clean(self, data, args=None):
        # 清除企业名称中一些扰乱匹配的标点
        # args = {'col_name'：****}

        df = data[args[2]]
        if df.empty:
            return data

        col_name = args[-1]['col_name']
        
        comp = re.compile(r'\*|-|\.|\d|\s|？|\?|·|ⅳ')
        df[col_name] = df[col_name].replace(comp, '').replace('（', '(').replace('）', ')')

        data[args[2]] = df
        return data
    
    def str_punctuation_clean(self, data, args=None):
        # 某文本中一些扰乱匹配的标点
        # args = {'col_name'：****}

        df = data[args[2]]
        if df.empty:
            return data

        col_name = args[-1]['col_name']
        
        comp = re.compile(r'\s|？|\?|·|ⅳ')
        df[col_name] = df[col_name].replace(comp, '').replace('（', '(').replace('）', ')')

        data[args[2]] = df
        return data
    
    def punctuation_eng2cn(self, data, args=None):
        # 将中文符号转英文符号
        # args = {'col_name'：****}
        
        df = data[args[2]]
        if df.empty:
            return data
        
        punctuation_dict = {
                '—':'-'        #8212 -> 45
                , '；': ';'     #65307 -> 59
                , '：': ':'     #65306 -> 58
                , '（': '('     #65288 -> 40
                , '）': ')'     #65289 -> 41
                , '，': ','     #65292 -> 44
                , '！': '!'     #65281 -> 33
                , '【': '['     #12304 -> 91
                , '】': ']'     #12305 -> 93
                , '“':'"'      #8220 -> 34
                , '”':'"'      #8221 -> 34
                , '‘':'\''     #8216 -> 39
                , '’':'\''     #8217 -> 39
                }
        
        col_name = args[-1]['col_name']
        
        for key in punctuation_dict:
            df[col_name] = df[col_name].replace(key, punctuation_dict[key], regex=True)
        
        data[args[2]] = df
        return data
     
    def col_comb(self, data, args=None):
        # 将各个列的字符串合并
        """
        args = {'col_name'：
                'col_list': }
        """

        df = data[args[2]]
        if df.empty:
            return data

        col_list = args[-1]['col_list']
        col_name = args[-1]['col_list']

        df[col_name] = functools.reduce(lambda a, b: df[a] + '|' + df[b], col_list)

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
        df = df.fillna('') # 某列有NAN的话， join语句报错，则会使得aggregate函数略过此列
        df = df.reindex(comb_col_list + [groupby_col,], axis=1)
        df0 = df.groupby(groupby_col).aggregate(lambda x: '|'.join(x))
        df0 = df0.reset_index()

        data[args[2]] = df0
        return data
    
    def replace_by_re(self, data, args=None):
        # 自定义正则表达式
        df = data[args[2]]
        if df.empty:
            return data
        arg = args[-1]
        
        re_str = arg['re_str']
        replace_col = arg['replace_col']
        replace_str = arg['replace_str']
        
        comp = re.compile(re_str)
        df[replace_col] = df[replace_col].replace(comp, replace_str)
        
        data[args[2]] = df
        return data
        
    
    def name_divide(self):
        # 单独自用函数
        with codecs.open('D:/企业分支机构.csv', 'r', 'utf-8') as f:
            df = pd.read_csv(f)
        df.columns = [s.lower() for s in df.columns]
        df['district'] = df['district'].astype(np.str)
        df['code'] = df['district'].apply(lambda x: x[:2])
        for s in set(df['code'].tolist()):
            if s:
                print("s=%s" %s)
                df0 = df[df['code'] == s]
                with codecs.open('D:/namelist/%s.csv' %s, 'r', 'utf-8') as f:
                    df1 = pd.read_csv(f, index_col=0)
                df0 = df0.drop(['code', ], axis=1)
                df1 = df1.append(df0, ignore_index=True)
                df1 = df1.drop_duplicates()
                df1.to_csv('D:/namelist/%s.csv' %s)
                
    def make_r_name_list(self, data, args=None):
        
        if data['info']['year'] == 2008 and 'beida' in data['path']:
            print("北大2008年没有法人数据！！！！")
            return data
        
        # 将变量表中的03和现在法人合并， 生成完整的r_name_list
        r_name_df = data['r_name_list']
        if r_name_df.empty:
            return data
        
        parent_df = data['parent_data']
        
        # 制作entid对应的所有法人名字额名单
        name_list = pd.merge(parent_df,r_name_df, how='left',
                              left_on='entid',
                              right_on='entid',
                              suffixes=('','_'),
                              )
        
        name_list['name_list'] = name_list['r_name'] \
                                + '|' + name_list['altbe'].fillna('') \
                                + '|' + name_list['altaf'].fillna('')
                                
        name_list = name_list.reindex(['entid', 'name_list'], axis=1)
        name_list = name_list.drop_duplicates('entid')
        
        data['r_name_list'] = name_list
        
        return data
        
        
        
        

if __name__ == '__main__':
    data_clean = data_clean()
    data_clean.name_divide()