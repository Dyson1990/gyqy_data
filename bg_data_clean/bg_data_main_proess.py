# -*- coding:utf-8 -*-  
#/usr/bin/python3
"""
--------------------------------
    @Author: Dyson
    @Contact: Weaver1990@163.com
    @file: main_proess.py
    @time: 2018/6/29 18:20
--------------------------------
"""
import sys
import os
import numpy as np
import pandas as pd
import time
import threading
import traceback
import datetime
import queue
import codecs


import get_data
get_data = get_data.get_data()

import data_clean
data_clean = data_clean.data_clean()

import data_output
data_output = data_output.data_output()

#import set_log  

#log_obj = set_log.Logger('main_proess.log', set_log.logging.WARNING,
#                         set_log.logging.DEBUG)
#log_obj.cleanup('main_proess.log', if_cleanup = True)  # 是否需要在每次运行程序前清空Log文件


class bg_data_main_proess(threading.Thread):

    def __init__(self, args_queue, lock):
        threading.Thread.__init__(self)
        self.args_queue = args_queue
        self.lock = lock

    def run(self):
        while True:
            lookup_args = self.args_queue.get()
            self._thread0(lookup_args)
            self.args_queue.task_done()


    def _thread0(self, lookup_args):

        start_time = time.time()

        data = {
            'i': 0,
            'path': 'D:/bg_process_log/%s/' %(lookup_args['code']),
            'res': pd.DataFrame([]),
            'info': {'province_code':lookup_args['code'], },
            'count':{},
            'lock': self.lock,
        }

        if not os.path.exists(data['path']):
            os.system('mkdir ' + data['path'].replace('/', '\\'))
        
        # 规范标题行
        for key in data:
            if isinstance(data[key], type(pd.DataFrame([]))):
                data[key].columns = [s.lower() for s in data[key].columns]

        # method_list为一个列表，其中[module, method, 0]代表针对test_data的方法，其中[module, method, 1]代表针对parent_data的方法
        method_list = [
            # 读取原始数据
            [get_data, 'get_bg_data', 'bg_data', lookup_args],
            [data_output, 'csv_output', 'bg_data', {'path':'D:/bg_data/', 'file_name': lookup_args['code'], 'table_name':'bg_data'}],
        ]
        try:
            for method in method_list:
                # print(type(method))
                start_time0 = time.time()
                print("\n%s\n%s省第%s步\n%s\n" %(datetime.datetime.now(), 
                                                       lookup_args['code'],
                                                       str(data['i']),
                                                       method))

                func = getattr(method[0], method[1])
                data = func(data, args=method)

                
                if method[2]:
                    # 记录前200条数据以供查阅
                    data[method[2]].iloc[:200,].to_csv('%s%s.%s[%s%s].csv' % (data['path'], str(data['i']), method[1], method[2], data[method[2]].shape[0]))
                
                data['i'] = data['i'] + 1
                print("\n耗时： %s\n" % (time.time() - start_time0)) 
                
        except:
            with self.lock:
                with codecs.open('error.log', 'a', 'utf-8') as f:
                    f.write("\n%s\n%s省第%s步\n%s\n" %(datetime.datetime.now(), 
                                                       lookup_args['code'], 
                                                       method,
                                                       traceback.format_exc()))
            time.sleep(4)

        pd.DataFrame([]).to_csv('%s%s.完成.csv' % (data['path'], str(data['i'])))
        print("总耗时： %s" %(time.time() - start_time))
        
        return None

if __name__ == '__main__':
        
    district_name = pd.read_csv('district_code.csv') # 第一列不作为index
    prov_list = set([str(s)[0:2] for s in district_name['code'].tolist()])
    lock = threading.Lock()
    args_queue = queue.Queue()
    
    for s in prov_list:
        args_queue.put({'code':s,})
    

    # 10个线程
    for i in range(6):
        t = bg_data_main_proess(args_queue, lock)
        t.setDaemon(True)
        t.start()
        
    args_queue.join()
        
    time.sleep(36000)
    
