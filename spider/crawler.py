# -*- coding:utf-8 -*-  
#/usr/bin/python3
"""
--------------------------------
    @Author: Dyson
    @Contact: Weaver1990@163.com
    @file: crawler.py
    @time: 2018/7/11 
--------------------------------
"""
import re
import pandas as pd
import time
import traceback
import datetime

import requests_manager
requests_manager = requests_manager.requests_manager()

county_code = []
county_detail = pd.DataFrame([])

class crawler(object):

    def __init__(self):
        pass
    
    def get_url(self):
        return ['https://www.youbianku.cn/postcode?page=%s' %i for i in range(401)]
    
    def catalog_parser(self, html):
        str_list = re.findall('(?<=\<span class=\"field-content\"\>\<a href=\")\/postcode\/\d+?-0(?=\"\>)', html)
        return ['https://www.youbianku.cn' + s for s in str_list]
    
    def county_parser(self, html):            
        str_list = re.findall('\<span class=[\"\']field-item[\"\']\> .*?\<\/span\>\<\/span\>', html, re.S)
        str_list = [re.sub('\<.+?\>', '', s) for s in str_list]
        
        title = ['国家', '省份', '城市', '区县', '地区', '邮政编码']
        
        d = dict(zip(title, str_list))
        #print(d)
        
        more_str = re.search('\<div class=[\"\']more\-link[\"\']\>\s*?\<a href=[\"\']/postcode/\d+?[\"\']\>', html, re.S).group()
        more_str = re.sub('(\<div class=[\"\']more\-link[\"\']\>\s*?\<a href=[\"\'])|([\"\']\>)', '', more_str)
        
        return d, 'https://www.youbianku.cn' + more_str
    
    def more_parser(self, html):
        m = re.search('\<table\s*?class=[\"\']views\-table\s*?cols\-2[\"\']>.+\<\/table\>', html, re.S)
        if m:
            html = m.group()
            df = pd.read_html(html)[0]
            # print(df)
            return df
        else:
            l0 = re.findall('\<li\s*?class\=\"pager-current\">.+?\<\/li\>', html, re.S)
            s0 = re.findall('\<h1\s*?class\=\"title\"\s*?id\=\"page-title\"\>.+?\<\/h1\>', html, re.S)
            with open('more_parser.log', 'a', encoding='utf8') as f:
                f.write(re.sub('\s', '', ("%s|||%s" %(''.join(s0), ''.join(l0)))) + '\n')
            
            return pd.DataFrame([])
            
    def go(self):
        url_list = self.get_url()
        
        for url0 in url_list:
            try:
                t0 = datetime.datetime.strftime(datetime.datetime.now(), '%H:%M')
                print('\n%s 准备解析目录页：%s\n' %(t0, url0))
                
                html = requests_manager.get_html(url0, charset='utf8')
                county_url_list = self.catalog_parser(html)
                
                for url1 in county_url_list:
                    t0 = datetime.datetime.strftime(datetime.datetime.now(), '%H:%M')
                    print('\n%s 准备解析县级明细页：%s\n' %(t0, url1))
                    
                    html = requests_manager.get_html(url1, charset='utf8')
                    county_info, more_url = self.county_parser(html)
                    
                    global county_code
                    county_code.append(county_info) # list 增加， 不是DataFrame
                    pd.DataFrame(county_code).to_excel('county_code.xlsx')
                    # print(county_code)
                    
                    i = 0
                    while True:
                        url2 = more_url + '?page=%s' %i
                        
                        t0 = datetime.datetime.strftime(datetime.datetime.now(), '%H:%M')
                        print('\n%s 准备解析乡村明细第%s页：%s\n' %(t0, i, url2))
                        
                        html = requests_manager.get_html(url2, charset='utf8')
                        
                        global county_detail
                        df = self.more_parser(html)
                        if df.empty:
                            break
                        
                        county_detail = county_detail.append(df, ignore_index=True)
                        county_detail.to_excel('county_detail.xlsx')
                        
                        i = i + 1
                        time.sleep(5)
            except:
                with open('error.log', 'a', encoding='utf8') as f:
                    f.write( ("%s\n%s\n\n" %(url0, traceback.format_exc())))
                
        
if __name__ == '__main__':
    crawler = crawler()
    crawler.go()
    
                    
                    
                
            
            
        
        
        
        
        
        
        
        