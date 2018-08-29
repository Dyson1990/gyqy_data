# -*- coding: utf-8 -*-
# /usr/bin/python3
"""
--------------------------------
Created on %(date)s
@author: Dyson
--------------------------------
"""
import os
import pandas as pd
import numpy as np
import codecs
import re
import threading
import queue
import time
import traceback
import json
import get_data
get_data = get_data.get_data()

re_str = '(^北京市?|^天津市?|^河北省?|^山西省?|^内蒙古(自治区)?|^辽宁省?|^吉林省?|^黑龙江省?|^上海市?|^江苏省?|^浙江省?|^安徽省?|^福建省?|^江西省?|^山东省?|^河南省?|^湖北省?|^湖南省?|^广东省?|^广西(壮族)?(自治区)?|^海南省?|^重庆市?|^四川省?|^贵州省?|^云南省?|^西藏(自治区)?|^陕西省?|^甘肃省?|^青海省?|^宁夏(回族)?(自治区)?|^新疆(维吾尔)?族?(自治区)?)'


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
        start_time = time.time()
        df = get_data.jbxx_data(thread_args)
        global re_str
        df['dom_ori'] = df['dom'].copy()
        df['prov'] = df['dom'].str.extract(re.compile(re_str))[0]
        df['dom'] = df['dom'].str.replace(re.compile(re_str), '')
        
        path0 = 'D:/dom_clean_res'
        if not os.path.exists(path0):
            os.makedirs(path0)
        df.to_csv(os.path.join(path0, 'id{}.csv'.format(thread_args['thread_id']))
                  , encoding='utf_8_sig')
        print('耗时：{}'.format(time.time()-start_time))
        
if __name__ == '__main__':
    lock = threading.Lock()
    args_queue = queue.Queue()
    
    total = 53000000
    q_len = 60000
    # 53000000 / 60000 = 883.333
    for i in range(443, 884):
        d = {'thread_id': i, 'q_len': q_len}
        args_queue.put(d)
        
    thread_count = 10
    for i in range(thread_count):
        t = dom_clean(args_queue, lock)
        t.setDaemon(True)
        t.start()
        
    args_queue.join()
    
    print("程序运行完毕！！！！")
    time.sleep(36000)