#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/10/12 14:29
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : company_spider_3.py
# @Software: PyCharm

import json
import os
import requests
from bs4 import BeautifulSoup
import time
import traceback

url_countries = r'https://companylist.org/countries/'

home_data = r'E:\work_all\topease\hs_spider_data\company_spider'
home_countries_list_data = r'E:\work_all\topease\hs_spider_data\company_spider\countries_list'

home_url = r'https://companylist.org'

countries_html_path = os.path.join(home_data, 'countries.html')
countries_json_path = os.path.join(home_data, 'countries.json')



headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:49.0) Gecko/20100101 Firefox/49.0',
    'Connection': 'keep-alive'
}

def download_countries_html():
    '''
    下载国家列表页
    :return:
    '''
    r = requests.get(url_countries, headers=headers)
    with open(countries_html_path, 'w', encoding='utf8') as fp:
        fp.write(r.text)


def parse_countries_html_to_json():
    '''
    解析国家列表页，并存储为json数据
    :return:
    '''

    countries_json = {}

    with open(countries_html_path, 'r', encoding='utf8') as fp:
        data = fp.read()

    soup = BeautifulSoup(data, 'html.parser')
    countries_select = soup.select('ul.col-md-4')

    for col_item in countries_select:
        for col_a_item in col_item.find_all('a'):
            countries_json[col_a_item.text] = home_url + col_a_item.get('href')

    with open(countries_json_path, 'w', encoding='utf8') as fp:
        fp.write(json.dumps(countries_json))



def try_catch_func(func):
    '''
    捕获异常，继续执行times次
    :param func:
    :param times:
    :return:
    '''
    def try_catch_func_in(times=10):
        '''
        闭包内部方法
        :return:
        '''
        for i in range(times):
            try:
                print('第%d次恢复爬取' % i)
                func()
            except:
                print('第%d次爬取中断' % (i+1))
                continue

    return try_catch_func_in


@try_catch_func
def download_all_countries_data_1w():
    '''
    获取所有国家的数据（每个国家最多1w条）
    :return:
    '''
    with open(countries_json_path, 'r', encoding='utf8') as fp:
        data = json.loads(fp.read())

    for key in data:
        # if key != 'Togo':
        #     continue
        print('=======  ', key)

        countries_dir_path = os.path.join(home_data, key)
        if not os.path.exists(countries_dir_path):
            os.mkdir(countries_dir_path)
        countries_url = data[key] + '1.html'

        while True:

            file_path = os.path.join(countries_dir_path, countries_url.replace('https://','').replace(r'/', '_'))

            file_is_exists = False
            if os.path.exists(file_path):
                file_is_exists = True
                with open(file_path, 'r', encoding='utf8') as fp:
                    context = fp.read()
            else:
                print(countries_url)
                r = requests.get(countries_url, headers=headers)
                time.sleep(2)
                context = r.text

            soup = BeautifulSoup(context, 'html.parser')
            next_select = soup.select('a.paginator-next')

            if not file_is_exists:
                with open(file_path, 'w', encoding='utf8') as fp:
                    fp.write(context)

            if len(next_select) == 0:
                break
            else:
                countries_url = home_url + next_select[0].get('href')


# def try_catch_func(func, times=10):
#     '''
#     捕获异常，继续执行times次
#     :param func:
#     :return:
#     '''
#     for i in range(times):
#         try:
#             func()
#         except:
#             continue




if __name__ == '__main__':
    # download_countries_html()
    # parse_countries_html_to_json()
    download_all_countries_data_1w(times=10)
