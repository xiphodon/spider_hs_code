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

home_data = r'E:\work_all\topease\company_spider'
home_countries_list_data = r'E:\work_all\topease\company_spider\countries_list'
home_countries_city_list_data = r'E:\work_all\topease\company_spider\countries_city_list'

home_url = r'https://companylist.org'

countries_html_path = os.path.join(home_data, 'countries.html')
countries_json_path = os.path.join(home_data, 'countries.json')
has_286_pages_countries_json_path = os.path.join(home_data, 'has_286_pages_countries.json')
countries_city_json_path = os.path.join(home_data, 'countries_city.json')
all_countries_company_list_json_path = os.path.join(home_data, 'all_countries_company_list.json')
countries_city_company_list_json_path = os.path.join(home_data, 'countries_city_company_list.json')


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



def parse_all_countries_company_list():
    '''
    解析所有的国家的公司列表
    :return:
    '''
    all_countries_company_list_json = {}
    countries_city_json = get_countries_city_json()

    for item_country in os.listdir(home_countries_list_data):

        # if item_country != 'China':
        #     continue
        all_countries_company_list_json[item_country] = []

        item_country_path = os.path.join(home_countries_list_data, item_country)
        print(item_country)

        for item_file_name in os.listdir(item_country_path):

            item_file_path = os.path.join(item_country_path, item_file_name)
            # print(item_file_path)

            # 是否为拥有详细城市的国家
            is_countries_city_company = False
            if item_country in countries_city_json:
                is_countries_city_company = True

            with open(item_file_path, 'r', encoding='utf8') as fp:
                context = fp.read()

            soup = BeautifulSoup(context, 'html.parser')
            # 所属国家、城市
            country_and_city_select = soup.select('div.container > h1')
            # 每个公司条目
            item_company_text_select = soup.select('div.result > span.result-txt')

            if len(country_and_city_select) > 0:
                country_and_city_text = country_and_city_select[0].text.strip()
            else:
                country_and_city_text = ''
            # print(country_and_city_text)

            for item_company_text in item_company_text_select:
                # 公司名称、联系方式、详情链接
                item_company_name_info_desc_href = item_company_text.select('span.result-name')[0]
                item_company_name_str = item_company_name_info_desc_href.select('a')[0].text.strip()
                item_company_desc_href_str = item_company_name_info_desc_href.select('a')[0].get('href')
                # print(item_company_name_str)
                # print(item_company_desc_href_str)

                # 所在城市、公司地址
                city_name_and_company_address = item_company_text.select('em')[0]
                city_name_select = city_name_and_company_address.select('a')
                if len(city_name_select) > 0:
                    city_name = city_name_select[0].text.strip()
                else:
                    city_name = ''
                company_address = city_name_and_company_address.select('span')[0].text.strip()
                # print(city_name)
                # print(company_address)

                # 产品类型
                product_type = item_company_text.select('span.result-cats')
                if len(product_type) > 0:
                    product_type_text_list = [i.text.strip() for i in product_type[0].select('a')]
                else:
                    product_type_text_list = []
                # print(product_type_text_list)

                if not is_countries_city_company or (is_countries_city_company and city_name == ''):
                        temp_company_dict = {
                            'country': item_country,
                            'company_page_title': country_and_city_text,
                            'company_name': item_company_name_str,
                            'desc_href': home_url + item_company_desc_href_str,
                            'city_name': city_name,
                            'company_address': company_address,
                            'product_type': product_type_text_list
                        }
                        all_countries_company_list_json[item_country].append(temp_company_dict)

    # print(all_countries_company_list_json)
    with open(all_countries_company_list_json_path, 'w', encoding='utf8') as fp:
        fp.write(json.dumps(all_countries_company_list_json))


def read_all_countries_company_list_json():
    '''
    读取所有国家公司列表json（不包拥有含超过1w家公司且有城市的公司数据）
    :return:
    '''
    with open(all_countries_company_list_json_path, 'r', encoding='utf8') as fp:
        data_stream = fp.read()
    return json.loads(data_stream)



def parse_countries_city_company_list():
    '''
    解析按城市划分的公司列表
    :return:
    '''
    countries_city_company_list_json = {}

    for item_country in os.listdir(home_countries_city_list_data):
        print(item_country)
        item_country_dir_path = os.path.join(home_countries_city_list_data, item_country)
        # print(item_country_dir_path)
        countries_city_company_list_json[item_country] = []

        for item_file_name in os.listdir(item_country_dir_path):
            item_file_path = os.path.join(item_country_dir_path, item_file_name)

            with open(item_file_path, 'r', encoding='utf8') as fp:
                context = fp.read()

            soup = BeautifulSoup(context, 'html.parser')
            # print(soup)

            # 所属国家、城市
            country_and_city_select = soup.select('div.container > h1')
            # 每个公司条目
            item_company_text_select = soup.select('div.result > span.result-txt')

            if len(country_and_city_select) > 0:
                country_and_city_text = country_and_city_select[0].text.strip()
            else:
                country_and_city_text = ''

            for item_company_text in item_company_text_select:
                # 公司名称、联系方式、详情链接
                item_company_name_info_desc_href = item_company_text.select('span.result-name')[0]
                item_company_name_str = item_company_name_info_desc_href.select('a')[0].text.strip()
                item_company_desc_href_str = item_company_name_info_desc_href.select('a')[0].get('href')
                # print(item_company_name_str)
                # print(item_company_desc_href_str)

                # 所在城市、公司地址
                city_name_and_company_address = item_company_text.select('em')[0]
                city_name_select = city_name_and_company_address.select('a')
                if len(city_name_select) > 0:
                    city_name = city_name_select[0].text.strip()
                else:
                    city_name = ''
                company_address = city_name_and_company_address.select('span')[0].text.strip()
                # print(city_name)
                # print(company_address)


                # 产品类型
                product_type = item_company_text.select('span.result-cats')
                if len(product_type) > 0:
                    product_type_text_list = [i.text.strip() for i in product_type[0].select('a')]
                else:
                    product_type_text_list = []
                # print(product_type_text_list)

                temp_company_dict = {
                    'country': item_country,
                    'company_page_title': country_and_city_text,
                    'company_name': item_company_name_str,
                    'desc_href': home_url + item_company_desc_href_str,
                    'city_name': city_name,
                    'company_address': company_address,
                    'product_type': product_type_text_list
                }
                countries_city_company_list_json[item_country].append(temp_company_dict)


            # break
        # break
    # print(countries_city_company_list_json)
    with open(countries_city_company_list_json_path, 'w', encoding='utf8') as fp:
        fp.write(json.dumps(countries_city_company_list_json))



def read_data_test():
    '''
    读数据测试
    :return:
    '''
    all_countries_company_list_json = read_countries_city_company_list_json()

    for item_country in all_countries_company_list_json:
        temp_dict_list = all_countries_company_list_json[item_country]
        for item in temp_dict_list:
            print(item['company_page_title'])
            # break
        # break

def read_countries_city_company_list_json():
    '''
    读取按城市划分的国家的公司列表
    :return:
    '''
    with open(countries_city_company_list_json_path, 'r', encoding='utf8') as fp:
        data_stream = fp.read()
    return json.loads(data_stream)



if __name__ == '__main__':
    # download_countries_html()
    # parse_countries_html_to_json()
    # download_all_countries_data_1w(times=10)
    # get_has_286_pages_countries()
    # parse_has_286_pages_countries_city()
    # download_has_286_pages_countries_city_company_list(times=10)
    # parse_all_countries_company_list()
    parse_countries_city_company_list()
    # read_data_test()
