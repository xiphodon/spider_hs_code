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
import random

url_countries = r'https://companylist.org/countries/'

home_data = r'E:\work_all\topease\hs_spider_data\company_spider'
home_countries_list_data = r'E:\work_all\topease\hs_spider_data\company_spider\countries_list'
home_countries_city_list_data = r'E:\work_all\topease\hs_spider_data\company_spider\countries_city_list'

home_url = r'https://companylist.org'

countries_html_path = os.path.join(home_data, 'countries.html')
countries_json_path = os.path.join(home_data, 'countries.json')
has_286_pages_countries_json_path = os.path.join(home_data, 'has_286_pages_countries.json')
countries_city_json_path = os.path.join(home_data, 'countries_city.json')


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

        countries_dir_path = os.path.join(home_countries_list_data, key)
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


def get_has_286_pages_countries():
    '''
    获取拥有286页的国家
    :return:
    '''
    has_286_pages_countries_list = []
    countries_dir_list = os.listdir(home_countries_list_data)
    for country_dir in countries_dir_list:
        # print(country_dir)
        country_dir_path = os.path.join(home_countries_list_data, country_dir)
        if len(os.listdir(country_dir_path)) == 286:
            # print(country_dir_path)
            has_286_pages_countries_list.append(country_dir_path)

    with open(has_286_pages_countries_json_path, 'w', encoding='utf8') as fp:
        fp.write(json.dumps(has_286_pages_countries_list))


def get_has_286_pages_countries_dir_path_json_list():
    '''
    获取拥有286页的国家文件夹路径列表
    :return: list列表
    '''
    with open(has_286_pages_countries_json_path, 'r', encoding='utf8') as fp:
        data_stream = fp.read()
    return json.loads(data_stream)


def parse_has_286_pages_countries_city():
    '''
    解析拥有286页的国家拥有的城市
    :return:
    '''
    countries_dir_path_list = get_has_286_pages_countries_dir_path_json_list()
    countries_city_json = {}

    for country_dir_path in countries_dir_path_list:
        country_name = country_dir_path.split('\\')[-1]
        this_country_s_city = set()
        print(country_name)
        for file_name in os.listdir(country_dir_path):
            file_path = os.path.join(country_dir_path, file_name)
            # print(file_path)
            with open(file_path, 'r', encoding='utf8') as fp:
                context = fp.read()
            soup = BeautifulSoup(context, 'html.parser')
            city_select = soup.select('span.result-txt > em > a')

            for item in city_select:
                city_name = item.text.strip()
                city_href = item.get('href')
                this_country_s_city.add((city_name, city_href))

        this_country_s_city_dict = dict(this_country_s_city)
        countries_city_json[country_name] = this_country_s_city_dict
        # print(countries_city_json)
        # break

    with open(countries_city_json_path, 'w', encoding='utf8') as fp:
        fp.write(json.dumps(countries_city_json))


def get_countries_city_json():
    '''
    获取国家城市及其链接列表
    :return:
    '''
    with open(countries_city_json_path, 'r', encoding='utf8') as fp:
        data_stream = fp.read()
    return json.loads(data_stream)


@try_catch_func
def download_has_286_pages_countries_city_company_list():
    '''
    下载拥有286页的国家按照城市搜索的所有公司的列表网页
    :return:
    '''
    countries_city_json = get_countries_city_json()
    # print(countries_city_json)
    for country_key in countries_city_json:

        countries_dir_path = os.path.join(home_countries_city_list_data, country_key)
        if not os.path.exists(countries_dir_path):
            os.mkdir(countries_dir_path)

        for city_key in countries_city_json[country_key]:
            city_href = home_url + countries_city_json[country_key][city_key] + '1.html'

            print('=======  ', country_key, ' -- ', city_key)

            while True:

                file_path = os.path.join(countries_dir_path, city_href.replace('https://', '').replace(r'/', '_'))

                file_is_exists = False
                if os.path.exists(file_path):
                    file_is_exists = True
                    with open(file_path, 'r', encoding='utf8') as fp:
                        context = fp.read()
                else:
                    print(country_key, ',', city_key, ':', city_href)

                    while_times = 0
                    while True:
                        try:
                            r = requests.get(city_href, headers=headers, timeout=5)
                        except Exception as e:
                            if while_times < 100:
                                while_times += 1
                                print('**********', '尝试重新链接', while_times, '次:', city_href)
                                continue
                            else:
                                raise e
                        else:
                            break
                    time.sleep(random.randint(10, 20)/10)
                    context = r.text

                soup = BeautifulSoup(context, 'html.parser')
                next_select = soup.select('a.paginator-next')

                if not file_is_exists:
                    with open(file_path, 'w', encoding='utf8') as fp:
                        fp.write(context)

                if len(next_select) == 0:
                    break
                else:
                    city_href = home_url + next_select[0].get('href')




if __name__ == '__main__':
    # download_countries_html()
    # parse_countries_html_to_json()
    # download_all_countries_data_1w(times=10)
    # get_has_286_pages_countries()
    # parse_has_286_pages_countries_city()
    download_has_286_pages_countries_city_company_list(10)