#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/10/20 14:27
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : company_spider_4.py
# @Software: PyCharm

import json
import os
import requests
from bs4 import BeautifulSoup
import time
import random
from multiprocessing import Pool


home_url = r'http://www.listcompany.org'

home_data = r'E:\work_all\topease\company_spider_2'
countries_list_dir_path = os.path.join(home_data, r'country_list')

company_countries_list_html = os.path.join(home_data, r'company_countries_list.html')
countries_url_list_json_path = os.path.join(home_data, r'countries_url_list.json')

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:49.0) Gecko/20100101 Firefox/49.0',
    'Connection': 'keep-alive'
}


def download_countries_list():
    '''
    下载国家列表页
    :return:
    '''
    result = requests.get(home_url + '/', headers=headers, timeout=5)
    # print(result.text)
    with open(company_countries_list_html, 'w', encoding='utf8') as fp:
        fp.write(result.text)



def parse_countries_list_to_json():
    '''
    解析国家列表界面，并持久化为json文件
    :return:
    '''
    countries_url_list_json = []

    with open(company_countries_list_html, 'r', encoding='utf8') as fp:
        data_stream = fp.read()

    soup = BeautifulSoup(data_stream, 'html.parser')

    countries_select = soup.select('ul.clearfix > li > a')

    for item_select in countries_select:
        country_url_end = item_select.get('href')
        if country_url_end == '#':
            continue

        country_name = item_select.text
        country_url = home_url + country_url_end

        countries_url_list_json.append({
            'country_name': country_name,
            'country_url': country_url
        })

    with open(countries_url_list_json_path, 'w', encoding='utf8') as fp:
        fp.write(json.dumps(countries_url_list_json))



def while_requests_get(page_url):
    '''
    循环请求
    :return:
    '''
    while_times = 0
    while True:
        try:
            result = requests.get(page_url, headers=headers, timeout=5)
        except Exception as e:
            if while_times < 100:
                while_times += 1
                print('**********', '尝试重新链接', while_times, '次:', page_url)
                continue
            else:
                raise e
        else:
            return result



def download_countries_company_list_files():
    '''
    下载各个国家的公司列表页面
    :return:
    '''
    with open(countries_url_list_json_path, 'r', encoding='utf8') as fp:
        countries_url_list_json = json.loads(fp.read())

    # print(countries_url_list_json)
    for item_country_url_dict in countries_url_list_json:
        country_name = item_country_url_dict['country_name']
        country_url = item_country_url_dict['country_url']
        country_url_home = str(country_url).replace(r'.html','')

        print(country_name)

        country_dir_path = os.path.join(countries_list_dir_path, country_name)
        if not os.path.exists(country_dir_path):
            os.mkdir(country_dir_path)

        first_page_url = country_url_home + r'/p1.html'
        first_page_url_file_name = first_page_url.replace('http://','').replace('/','_')
        first_page_url_path = os.path.join(country_dir_path, first_page_url_file_name)

        if os.path.exists(first_page_url_path):
            with open(first_page_url_path, 'r', encoding='utf8') as fp:
                context = fp.read()
        else:
            result = while_requests_get(first_page_url)
            context = result.text
            with open(first_page_url_path, 'w', encoding='utf8') as fp:
                fp.write(context)

        soup = BeautifulSoup(context, 'html.parser')

        last_page_select = soup.select('div.pagea > ul > a')

        if len(last_page_select) > 0:
            if last_page_select[-1].text == 'Last':
                page_size_href = last_page_select[-1].get('href').replace('.html','')
                page_size_index_start = page_size_href.rfind('p') + 1
                page_size = int(page_size_href[page_size_index_start:])
            else:
                continue
        else:
            continue


        for page_index in range(2, page_size + 1):
            page_url = country_url_home + r'/p' + str(page_index) + r'.html'
            page_url_file_name = page_url.replace('http://', '').replace('/', '_')
            page_url_path = os.path.join(country_dir_path, page_url_file_name)

            if os.path.exists(page_url_path):
                continue
            else:
                result = while_requests_get(page_url)
                inner_context = result.text
                with open(page_url_path, 'w', encoding='utf8') as fp:
                    fp.write(inner_context)

                print(country_name, page_size, page_url_path)
                time.sleep(random.randint(10, 20)/10)

        # break

        # print(country_name, country_url)








if __name__ == '__main__':
    # download_countries_list()
    # parse_countries_list_to_json()
    download_countries_company_list_files()