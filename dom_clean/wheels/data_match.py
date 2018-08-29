# -*- coding:utf-8 -*-  
#/usr/bin/python3
"""
--------------------------------
    @Author: Dyson
    @Contact: Weaver1990@163.com
    @file: data_match.py
    @time: 2018/6/27 17:13
--------------------------------
"""
import sys
import os
import numpy as np
import pandas as pd

import wheels.get_data
get_data = wheels.get_data.get_data()

import wheels.data_clean
data_clean = wheels.data_clean.data_clean()

#import set_log  

#log_obj = set_log.Logger('data_match.log', set_log.logging.WARNING,
#                         set_log.logging.DEBUG)
#log_obj.cleanup('data_match.log', if_cleanup = True)  # 是否需要在每次运行程序前清空Log文件

class data_match(object):

    def __init__(self):
        pass

    def data_lookup(self, test_df, name2id_df, args=None):
        # 为test_df增加一个新的列res，在test_df表格的test_col列中， 哪些存在于name2id_df表格中的name_col列里
        return test_df.join(test_df[args['lookup_name_x']].isin(name2id_df[args['lookup_name_y']]).rename('bool'))

    def data_filter(self, data, args=None):
        # 分离数据
        if 'test_data' in data and data['test_data'].empty:
            print("已经完全匹配！！！！")
            return data

        res_temp = self.data_lookup(data['test_data'], data['parent_data'], args[-1])  # 增加bool列，判断数据是否能匹配上
        data['test_data'] = res_temp[res_temp['bool'] == False].drop(['bool', ], axis=1).copy()  # 未匹配数据
        data['res'] = data['res'].append(res_temp[res_temp['bool'] == True].drop(['bool', ], axis=1), ignore_index=True, sort=False)  # 提取出已匹配数据

        data['res'].to_csv('%s%s.result【%s】.csv' % (data['path'], data['i'], args[-1]['lookup_name_y']))
        data['test_data'].to_csv('%s%s.remaining_data【%s】.csv' % (data['path'], data['i'], args[-1]['lookup_name_x']))

        print('\nmatched data: %s\nunmatched data: %s\n' % (data['res']['lookup_name'].count(), data['test_data']['lookup_name'].count()))
        return data
    
    def data_merge(self, data, args=None):
        # 按照lookup_name匹配ENTID

        res = data['res']
        arg = args[-1]
        test_df_name = arg['table_x'] if 'table_x' in arg else 'test_data'
        parent_df_name = arg['table_y'] if 'table_y' in arg else 'parent_data'
        test_df = data[test_df_name]
        parent_df = data[parent_df_name]
        
        if test_df_name in data and data[test_df_name].empty:
            print("已经完全匹配！！！！")
            return data

        test_df_col = tuple(test_df.columns)
        
        if 'filter_parent_df' in arg:
            if not parent_df.empty:
                filter_col = arg['filter_parent_df'][0]
                filter_value = arg['filter_parent_df'][1]
                parent_df = parent_df[parent_df[filter_col] == filter_value]
                
        res_temp = pd.merge(test_df,parent_df, how='left',
                              left_on=arg['lookup_name_x'],
                              right_on=arg['lookup_name_y'],
                              suffixes=('','[step%s]' %data['i']),
                              )
        
        test_df = res_temp[res_temp[arg['bool_col']].isna() == True].copy()  # 未匹配数据
        # 提取出已匹配数据
        res_temp_f = res_temp[res_temp[arg['bool_col']].isna() == False]
        res = pd.concat([res, res_temp_f], join='outer', ignore_index=True, sort=False)

        test_df = test_df.reindex(test_df_col, axis=1)
        test_df.to_csv('%s%s.remaining_data【%s】.csv' % (data['path'], data['i'], test_df.shape[0])
                       , encoding = 'utf_8_sig')
        res.to_csv('%s%s.result【%s】.csv' % (data['path'], data['i'], res.shape[0])
                   , encoding = 'utf_8_sig')
        
        data[test_df_name] = test_df
        data['res'] = res
        info = data['info']
        year_str = '%s-%s' %(info['year'], info['province_code'])

        d = {'year_str': year_str,
            'remaining_data[step%s]' %data['i']: data[test_df_name].shape[0],
            'result[step%s]' %data['i']: data['res'].shape[0],
            }
        data['count'].update(d)
        
        return data
    

    def data_check(self, data, args=None):
        # 检查数据匹配结果
        # 暂时不用arg，没有想好之后的怎么检验
        if 'test_data' in data and data['test_data'].empty:
            print("已经完全匹配！！！！")
            return data
        
        if data['info']['year'] == 2008 and 'beida' in data['path']:
            print("北大2008年没有法人数据！！！！")
            return data
        
        name_list = r_name_df = data['r_name_list']
        if r_name_df.empty:
            return data
        
        res = data['res']        
        
        # 匹配名字列表到待检测文件
        res_temp = pd.merge(res, name_list, how='left',
                              left_on='entid',
                              right_on='entid',
                              suffixes=('','_'),
                              )
        res_temp['bool'] = res_temp.apply(lambda row: row['r_name'] in row['name_list']  \
                                          if isinstance(row['r_name'], str) and isinstance(row['name_list'], str) \
                                          else False, axis=1)
        
        # 暂时先直接删除法人缺失的数据
        res_temp = res_temp.dropna(subset=['r_name',])
        
# =============================================================================
#         # 暂时不考虑将检验出错的数据放回去检验
#         f_data = res_temp[res_temp['bool'] == False].drop(['bool', ], axis=1).copy()
#         f_data = f_data.reindex(test_df_col, axis=1)
#         test_df = test_df.append(f_data, ignore_index=True)
#         data['test_data'] = test_df
#         
#         data['res'] = res_temp[res_temp['bool'] == True].drop(['bool', ], axis=1).copy()
# =============================================================================
        
        T0 = res_temp['bool'][res_temp['bool'] == True].count()
        F0 = res_temp['bool'][res_temp['bool'] == False].count()
        res_temp.to_csv('%s%s.data_check【T%sF%s】.csv' % (data['path'], data['i'], T0, F0)
                        , encoding = 'utf_8_sig')

        info = data['info']
        year_str = '%s-%s' %(info['year'], info['province_code'])
        d = {'year_str': year_str,
            'check_TRUE[step%s]' %data['i']: T0,
            'check_FALSE[step%s]' %data['i']: F0,
            }
        data['count'].update(d)
        
        
        return data
    
    
    def data_check0(self, data, args=None):
        # 数据纵向匹配
        # df_ori ==》 原始数据
# =============================================================================
#         df_ori = pd.DataFrame([
#             {'entname':'D', 'entid':'112', 'year':'2000'},
#             {'entname':'A', 'entid':'113', 'year':'1999'},
#             {'entname':'A', 'entid':'111', 'year':'1998'},
#             {'entname':'A', 'entid':'111', 'year':'1998'},
#             {'entname':'C', 'entid':'113', 'year':'2000'},
#             {'entname':'F', 'entid':'113', 'year':'2001'},
#             {'entname':'B', 'entid':'112', 'year':'1998'},
#         ])
# =============================================================================
        df_ori = data['test_data']
        
        df_ori = df_ori.sort_values(by=['entname', 'data_year'])
        # df_s ==》df_short
        df_s = df_ori.drop_duplicates(['entid','entname'])
        # df_id ==>以entid左连接
        df_id = pd.merge(df_s, df_s, how='left', on='entid')
        # df_drop1 ==》第一次筛选
        df_drop1 = df_id[(df_id['data_year_x']!=df_id['data_year_y']) \
                        and df_id['data_year_x' ] < df_id['data_year_y']]
        
        df0 = df_drop1.copy()
        df0['gdid'] = ''
        name2id = {}
        for i in df0.index:
            # 若这一行数据已经被抛弃，则无视
            if df0.loc[i, 'gdid'] == '0':
                continue
            
            entname_x = df0.loc[i, 'entname_x']
            entname_y = df0.loc[i, 'entname_y']
            # 筛选出entname_x列为[i, 'entname_y']的部分，将这部分的gdid设为'0'，抛弃不用
            df0['gdid'][df0['entname_x'] == entname_y] = '0'
            
            if entname_x not in name2id:
                name2id[entname_x] = 'GD{:0>8d}'.format(int(i))
            df0.loc[i, 'gdid'] = name2id[entname_x]
            
            if entname_y not in name2id:
                name2id[entname_y] = name2id[entname_x]
        
            df_ori['gdid'] = df_ori['entname'].apply(lambda s: name2id[s] if s in name2id else s)
            
            df0.to_csv('D:/df0_test.csv')
            df_ori.to_csv('D:/df_ori_test.csv')
        return data


if __name__ == '__main__':
    data_match = data_match()
