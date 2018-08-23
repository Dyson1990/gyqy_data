# -*- coding: utf-8 -*-
# /usr/bin/python3
"""
--------------------------------
Created on %(date)s
@author: Dyson
--------------------------------
"""
import pandas as pd
import baidu_api
baidu_api = baidu_api.baidu_api()
import numpy as np
import codecs
import re
import threading
import queue
import time
import traceback

class dom_clean(threading.Thread):

    def __init__(self, args_queue, lock):
        threading.Thread.__init__(self)
        self.args_queue = args_queue
        self.lock = lock

    def run(self):
        while True:
            thread_args = self.args_queue.get()
            self._thread0(thread_args)
            self.args_queue.task_done()

    def _thread0(self, thread_args):
        # print(thread_args)
        df = thread_args['data']
        for i in df.index:
            print('RUNNING {}API  => {}'.format(thread_args['id'], i))
            try:
                df.loc[i, 'res'] = baidu_api.get_data(df.loc[i, 'dom_road']
                                                      , df.loc[i, 're地市名']
                                                      , ak = thread_args['AK']
                                                      , sk = thread_args['SK'])
                df.to_csv('C:\\Users\\gooddata\\Desktop\\res\\{}.csv'.format(thread_args['id']), encoding='utf_8_sig')
                time.sleep(1.5)
            except:
                df.loc[i, 'res'] = '百度API错误'
                print(traceback.format_exc())
# =============================================================================
#             df['res'] = df.apply(lambda row: baidu_api.get_data(row['dom'], row['地市名']), axis=1)
#             print(df.loc[i,:])
#             time.sleep(1)
# =============================================================================
                
        
        
if __name__ == '__main__':
    dom_table = pd.read_excel('JBXX中dom有问题的数据(随机取样15万)20180820.xlsx', dtype=np.str)
    dom_table['city_code'] = dom_table['district'].apply(lambda s: s[0:4])
    
    # 正则表达式取出路名
    re_str1 = '((路南区)|(路北区)|(让胡路)|(路桥区)|(路南彝族自治县))'
    re_str2 = '^.*?{}.+?路'
    func1 = lambda s: re.search(re_str1, s).group() if  re.search(re_str1, s) else ''
    func2 = lambda s: re.search(re_str2.format(func1(s)), s).group() if re.search(re_str2.format(func1(s)), s) else None
    dom_table['dom_road'] = dom_table['dom'].apply(func2)
    dom_table['dom_road'] = dom_table['dom_road'].fillna(dom_table['dom'])
    
    # 提取dom中的市名
    with codecs.open('district_code20180813_3.csv','r','utf-8') as f0:
        district_name = pd.read_csv(f0, dtype=np.str) # 第一列不作为index
    
    ser = district_name['地市名'].copy()
    re_set = set(ser)
    re_set = {s.replace('[', '').replace(']', '') for s in re_set} # 暂时先清除[]，过去的县市与现在的混在一起
    re_set = {s + '?' if len(s)>2 else '^' + s for s in re_set} # 加上正则表达式
    re_str = r'(%s)' %('|'.join(re_set))
    comp = re.compile(re_str)
    
    dom_table['re地市名'] = ''
    dom_table['re地市名'] = dom_table['dom'].str.extract(comp)
    
    with codecs.open('district_code20180813_3.csv', 'r', 'utf-8') as f:
        dist_table = pd.read_csv(f, dtype=np.str)
    dist_table['city_code'] = dist_table['县市区代码'].apply(lambda s: s[0:4])
    dist_table['地市名'] = dist_table['地市名'].replace(re.compile(r'[\[\]]'), '')
    dist_table['县市区名'] = dist_table['县市区名'].replace(re.compile(r'[\[\]]'), '')
    dist_table['地市名'] = dist_table.apply(lambda row: row['县市区名'] 
                                            if row['地市名'] == row['省市区名']
                                               or row['地市名'] == '省直辖县级行政区划'
                                            else row['地市名'], axis=1)
    
    dist_table = dist_table.drop_duplicates('city_code')
    dist_table = dist_table.reindex(['city_code', '地市名'], axis=1)
    
    merged_table = pd.merge(dom_table, dist_table, how='left', on='city_code')
    merged_table['re地市名'] =  merged_table['re地市名'].fillna(merged_table['地市名'])
    
    merged_table.to_csv('C:\\Users\\gooddata\\Desktop\\res\\merged_table.csv', encoding='utf_8_sig')
    
    key_list = [
# =============================================================================
#             {
#                 'AK':'ZGz27O8UEXkC3SEIdHAn7u6aFL1CH0u0'
#                 , 'SK':'70CTq3TOe3x6XGuhW7RC7z9YVG4r5zuZ'
#                 , 'id':'Dyson'
#             },
# =============================================================================
            {# 徐丽俊
                'AK':'cXADMjz74v8vECTfW64sUKwx0o8xZCe4'
                , 'SK':'xlZksrwzdmeabO7LzPGotmjjS4yegucq'
                , 'id':'徐丽俊'
            },
            {# 金日超
                'AK':'09GDAIA5ouDbRle9pqTtg3c2OaB6w71n'
                , 'SK':'vVM0dq62nFbxrF70jIGidb8XoyBKTVsP'
                , 'id':'金日超'
            },
            {# 丹燕
                'AK':'DE3NnX0MGUbhzIPdG7I9NLivGaY6tR4L'
                , 'SK':'7aVoGPqx8sA77y8IAAqZrCWlCMu5GRp8'
                , 'id':'丹燕'
            },
            {# 胜蓝
                'AK':'lVFeGEaOLeBqmXfZCnIGz89Le6YrOdTp'
                , 'SK':'LdwKKrphGWKOQn2UyVEtUP4TfkLekj99'
                , 'id':'胜蓝'
            },
            {# 乐乐
                'AK':'6EiaeGhILmRuEhDO3Y03O3KE6aoVKzDU'
                , 'SK':'WUlLRD1hmREd3jF58m6S4LLOjzBG3DMQ'
                , 'id':'乐乐'
            },
            {# 泽清
                'AK':'LceUeujRqgsQViSNYYAQpSP128vzm37Y'
                , 'SK':'5s20UQXEt0BCiWcHp47GhGPgmMfOIMf2'
                , 'id':'泽青'
            },
            
            ]
    
    lock = threading.Lock()
    args_queue = queue.Queue()
    
    i = 0
    for d in key_list:
        d['data'] = merged_table.reindex(range(i * 25000, (i+1) * 25000))
        args_queue.put(d)
        i = i + 1
    

    # 10个线程
    for i in range(6):
        t = dom_clean(args_queue, lock)
        t.setDaemon(True)
        t.start()
        
    args_queue.join()
    
    print("程序运行完毕！！！！")
    time.sleep(36000)