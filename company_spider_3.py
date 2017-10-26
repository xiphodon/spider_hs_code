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
# import traceback
import random
from multiprocessing import Pool
import re

url_countries = r'https://companylist.org/countries/'

home_data = r'E:\work_all\topease\company_spider'
home_countries_list_data = r'E:\work_all\topease\company_spider\countries_list'
home_countries_city_list_data = r'E:\work_all\topease\company_spider\countries_city_list'
home_countries_company_desc_list_data = r'E:\work_all\topease\company_spider\countries_company_desc_list'

home_url = r'https://companylist.org'

countries_html_path = os.path.join(home_data, 'countries.html')
countries_json_path = os.path.join(home_data, 'countries.json')
has_286_pages_countries_json_path = os.path.join(home_data, 'has_286_pages_countries.json')
countries_city_json_path = os.path.join(home_data, 'countries_city.json')
all_countries_company_list_json_path = os.path.join(home_data, 'all_countries_company_list.json')
countries_city_company_list_json_path = os.path.join(home_data, 'countries_city_company_list.json')
final_company_list_json_path = os.path.join(home_data, 'final_company_list_json.json')
final_id_company_list_json_path = os.path.join(home_data, 'final_id_company_list_json.json')
all_company_desc_no_web_json_path = os.path.join(home_data, 'all_company_desc_no_web.json')
company_desc_list_json_path = os.path.join(home_data, 'company_desc_list.json')
company_desc_list_has_web_json_path = os.path.join(home_data, 'company_desc_list_has_web.json')
company_desc_list_has_web_json_error_path = os.path.join(home_data, 'company_desc_list_has_web_error.json')

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:49.0) Gecko/20100101 Firefox/49.0',
    'Connection': 'keep-alive'
}


def download_countries_html():
    """
    下载国家列表页
    :return:
    """
    r = requests.get(url_countries, headers=headers)
    with open(countries_html_path, 'w', encoding='utf8') as fp:
        fp.write(r.text)


def parse_countries_html_to_json():
    """
    解析国家列表页，并存储为json数据
    :return:
    """
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
    """  捕获异常，继续执行times次
    :param func:
    :return:
    """
    def try_catch_func_in(times=10):
        """      闭包内部方法
        :return:
        """
        for i in range(times):
            try:
                print('第%d次恢复爬取' % i)
                func()
            except Exception as e:
                print('第%d次爬取中断' % (i + 1))
                print(e)
                continue

    return try_catch_func_in


@try_catch_func
def download_all_countries_data_1w():
    """
    获取所有国家的数据（每个国家最多1w条）
    :return:
    """
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

            file_path = os.path.join(countries_dir_path, countries_url.replace('https://', '').replace(r'/', '_'))

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
#     """    捕获异常，继续执行times次
#     :param func:
#     :return:
#     """    for i in range(times):
#         try:
#             func()
#         except:
#             continue


def get_has_286_pages_countries():
    """
    获取拥有286页的国家
    :return:
    """
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
    """  获取拥有286页的国家文件夹路径列表
    :return: list列表
    """
    with open(has_286_pages_countries_json_path, 'r', encoding='utf8') as fp:
        data_stream = fp.read()
    return json.loads(data_stream)


def parse_has_286_pages_countries_city():
    """  解析拥有286页的国家拥有的城市
    :return:
    """
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
    """  获取国家城市及其链接列表
    :return:
    """
    with open(countries_city_json_path, 'r', encoding='utf8') as fp:
        data_stream = fp.read()
    return json.loads(data_stream)


@try_catch_func
def download_has_286_pages_countries_city_company_list():
    """
    下载拥有286页的国家按照城市搜索的所有公司的列表网页
    :return:
    """
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
                    time.sleep(random.randint(10, 20) / 10)
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
    """
    解析所有的国家的公司列表
    :return:
    """
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
    """
    读取所有国家公司列表json（不包拥有含超过1w家公司且有城市的公司数据）
    :return:
    """
    with open(all_countries_company_list_json_path, 'r', encoding='utf8') as fp:
        data_stream = fp.read()
    return json.loads(data_stream)


def parse_countries_city_company_list():
    """
    解析按城市划分的公司列表
    :return:
    """
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

                if country_and_city_text != '':
                    near_index = country_and_city_text.find('near')
                    douhao_index = country_and_city_text.rfind(',')
                    if near_index > 0 and douhao_index > 0:
                        city_name = country_and_city_text[near_index + 4:douhao_index].strip()
                        # print(city_name)

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
    # print(countries_city_company_list_json)
    with open(countries_city_company_list_json_path, 'w', encoding='utf8') as fp:
        fp.write(json.dumps(countries_city_company_list_json))


def read_countries_city_company_list_json():
    """
    读取按城市划分的国家的公司列表
    :return:
    """
    with open(countries_city_company_list_json_path, 'r', encoding='utf8') as fp:
        data_stream = fp.read()
    return json.loads(data_stream)


def sum_countries_company_list_json():
    """
    合并国家公司的列表json文件
    :return:
    """
    all_countries_company_list_json = read_all_countries_company_list_json()
    countries_city_company_list_json = read_countries_city_company_list_json()

    final_company_list_json = {}

    for item_1 in all_countries_company_list_json:
        final_company_list_json[item_1] = all_countries_company_list_json[item_1]
        for item_2 in countries_city_company_list_json:
            if item_1 == item_2:
                final_company_list_json[item_1].extend(countries_city_company_list_json[item_2])
                break

    with open(final_company_list_json_path, 'w', encoding='utf8') as fp:
        fp.write(json.dumps(final_company_list_json))


def read_final_company_list_json():
    """
    读取最终的公司列表json文件
    :return:
    """
    with open(final_company_list_json_path, 'r', encoding='utf8') as fp:
        data_stream = fp.read()
    return json.loads(data_stream)


def add_id_to_final_company_list_json():
    """
    添加id到最终的公司列表json文件
    :return:
    """
    company_id = 0
    final_company_list_json = read_final_company_list_json()
    for item_country in final_company_list_json:
        print(item_country)
        for item_company in final_company_list_json[item_country]:
            company_id += 1
            item_company['company_id'] = str(company_id)

    with open(final_id_company_list_json_path, 'w', encoding='utf8') as fp:
        fp.write(json.dumps(final_company_list_json))


def read_final_id_company_list_json():
    """
    读取拥有id的公司列表json文件
    :return:
    """
    with open(final_id_company_list_json_path, 'r', encoding='utf8') as fp:
        data_stream = fp.read()
    return json.loads(data_stream)


# @try_catch_func
def multiprocessing_download_all_countries_company_desc():
    """
    多进程下载所有公司的公司详情
    :return:
    """
    desc_href_and_desc_href_file_path_list = download_all_countries_company_desc()
    print(len(desc_href_and_desc_href_file_path_list))
    # print(desc_href_and_desc_href_file_path_list)
    pool = Pool(30)
    pool.map(download_all_countries_company_desc_inner_func_request_and_save_html,
             desc_href_and_desc_href_file_path_list)
    pool.close()
    pool.join()


# @try_catch_func
def download_all_countries_company_desc():
    """
    下载所有公司的公司详情
    :return:
    """
    read_json = read_final_id_company_list_json()
    desc_href_and_desc_href_file_path_list = []

    for item_country in read_json:
        print(item_country)
        item_country_dir = os.path.join(home_countries_company_desc_list_data, item_country)
        if not os.path.exists(item_country_dir):
            os.mkdir(item_country_dir)

        temp_dict_list = read_json[item_country]
        for item_company in temp_dict_list:
            desc_href = item_company['desc_href']
            company_id = item_company['company_id']

            desc_href_file_name = 'company_id' + '^' + company_id + '.html'
            desc_href_file_path = os.path.join(item_country_dir, desc_href_file_name)

            # if os.path.exists(desc_href_file_path):
            #     continue
            #
            if os.path.exists(desc_href_file_path) and os.path.getsize(desc_href_file_path) > 20 * 1000:
                continue

            # print(item_country, desc_href)

            desc_href_and_desc_href_file_path_list.append((desc_href, desc_href_file_path))

    return desc_href_and_desc_href_file_path_list


def download_all_countries_company_desc_inner_func_request_and_save_html(desc_href_and_desc_href_file_path):
    """
    下载所有国家公司详情的内部方法，请求及存储html页面
    :return:
    """

    desc_href, desc_href_file_path = desc_href_and_desc_href_file_path

    while_times = 0
    while True:
        try:
            result = requests.get(desc_href, headers=headers, timeout=5)
        except Exception as e:
            if while_times < 100:
                while_times += 1
                print('**********', '尝试重新链接', while_times, '次:', desc_href)
                continue
            else:
                raise e
        else:
            break

    with open(desc_href_file_path, 'w', encoding='utf8') as fp:
        fp.write(result.text)

    print(desc_href_file_path, ' ===== OK')

    # time.sleep(random.randint(5, 10) / 10)
    # time.sleep(random.randint(10, 20) / 10)
    # time.sleep(random.randint(20, 30) / 10)


def parse_all_countries_company_desc_files_to_json():
    """
    解析所有国家的详情页面 → json文件
    :return:
    """
    all_company_desc_no_web_json_list = []

    for item_country in os.listdir(home_countries_company_desc_list_data):
        # if item_country != 'Africa':
        #     continue
        item_country_dir_path = os.path.join(home_countries_company_desc_list_data, item_country)
        for item_file in os.listdir(item_country_dir_path):

            print(item_country, item_file)

            item_file_path = os.path.join(item_country_dir_path, item_file)

            with open(item_file_path, 'r', encoding='utf8') as fp:
                context = fp.read()

            soup = BeautifulSoup(context, 'html.parser')

            # if soup.find('REFLEX MOULDINGS LTD') == None:
            #     print(item_file_path.split('^')[-1].split('.')[0])
            #     continue

            # 公司id（define）
            company_id_index_start = item_file_path.rfind('^') + 1
            company_id_index_end = item_file_path.rfind('.html')
            company_id = item_file_path[company_id_index_start:company_id_index_end]

            # 公司名称
            company_name_select = soup.select('div#band > div.container > h1')
            company_name_text = company_name_select[0].text

            # 公司地址等详情
            company_desc_address_select = soup.select('div.address')[0]
            # 公司地址等详情-公司电话
            # company_telephone_select = company_desc_address_select.select('span#phone > a')[0]
            # company_telephone_onclick_text = str(company_telephone_select.get('onclick'))
            # company_telephone_index_start = company_telephone_onclick_text.find("'") + 1
            # company_telephone_index_end = company_telephone_onclick_text.rfind("'")
            # company_telephone_origin_text =
            #   company_telephone_onclick_text[company_telephone_index_start:company_telephone_index_end]
            # print(company_telephone_onclick_text)
            # print(company_telephone_origin_text)
            # print()
            # 公司地址等详情-公司网站
            company_web_href_select = company_desc_address_select.select('a#cdetail-web')
            if len(company_web_href_select) > 0:
                company_web_href_end = company_web_href_select[0].get('href')
            else:
                company_web_href_end = ''
            # 公司地址等详情-公司所属国家
            company_country_select = company_desc_address_select.select('span.flag')[0]
            company_country_text = company_country_select.text
            # 公司地址等详情-公司地址，公司电话
            company_address_select = company_desc_address_select.select('meta')

            company_address_text = ""
            company_telephone_origin_text = ""
            for item_meta in company_address_select:
                if item_meta.get('itemprop') == 'address':
                    company_address_text = item_meta.get('content').replace(r'<br />', ' ')
                if item_meta.get('itemprop') == 'telephone':
                    company_telephone_origin_text = item_meta.get('content')

            # 公司描述
            company_description_select = soup.select('div#company-desc-div')[0]
            company_description_text = company_description_select.text

            temp_dict = {
                'company_id': company_id,
                'company_name_text': company_name_text,
                'company_web_href_end': company_web_href_end,
                'company_country_text': company_country_text,
                'company_address_text': company_address_text,
                'company_telephone_origin_text': company_telephone_origin_text,
                'company_description_text': company_description_text
            }

            all_company_desc_no_web_json_list.append(temp_dict)

            # break
            # break

    with open(all_company_desc_no_web_json_path, 'w', encoding='utf8') as fp:
        fp.write(json.dumps(all_company_desc_no_web_json_list))


def read_all_company_desc_no_web_json():
    """
    读取所有的公司详情json（no web）
    :return:
    """
    with open(all_company_desc_no_web_json_path, 'r', encoding='utf8') as fp:
        data = fp.read()

    return json.loads(data)


def extend_all_company_desc_json():
    """
    补充扩展公司详情json文件
    :return:
    """
    all_company_desc_no_web_json = read_all_company_desc_no_web_json()
    final_id_company_list_json = read_final_id_company_list_json()

    company_desc_list_json = []

    for item_country in final_id_company_list_json:
        item_country_company_list = final_id_company_list_json[item_country]
        for item_company_2 in item_country_company_list:

            company_id_2 = item_company_2['company_id']
            company_desc_href_2 = item_company_2['desc_href']
            city_name_2 = item_company_2['city_name']
            product_type_2 = item_company_2['product_type']

            print(item_country, company_id_2)

            for item_company_1 in all_company_desc_no_web_json:
                company_id_1 = item_company_1['company_id']

                if company_id_1 == company_id_2:

                    company_name_1 = item_company_1['company_name_text']
                    company_web_href_end_1 = item_company_1['company_web_href_end']
                    country_name_1 = item_company_1['company_country_text']
                    company_address_1 = item_company_1['company_address_text']
                    company_telephone_1 = item_company_1['company_telephone_origin_text']
                    company_description_1 = item_company_1['company_description_text']

                    company_web = company_desc_href_2 + company_web_href_end_1 if company_web_href_end_1 != "" else ""

                    temp_dict = {
                        'company_id': company_id_1,
                        'company_name': company_name_1,
                        'company_country': country_name_1,
                        'company_city': city_name_2,
                        'company_adrress': company_address_1,
                        'company_telephone': company_telephone_1,
                        'company_web': company_web,
                        'company_product': product_type_2,
                        'company_description': company_description_1
                    }

                    company_desc_list_json.append(temp_dict)

                    print(item_country, company_id_2, 'OK\n')

                    break

                else:
                    continue

    print(len(company_desc_list_json))
    print(company_desc_list_json[0])
    print(company_desc_list_json[1])
    print(company_desc_list_json[2])

    with open(company_desc_list_json_path, 'w', encoding='utf8') as fp:
        fp.write(json.dumps(company_desc_list_json))


def read_company_desc_list_json():
    """
    读取公司详情列表（网址跳转）
    :return:
    """
    with open(company_desc_list_json_path, 'r', encoding='utf8') as fp:
        data = fp.read()
    return json.loads(data)


def read_company_desc_list_has_web_json_error():
    """
    读取公司详情列表（网址跳转）
    :return:
    """
    with open(company_desc_list_has_web_json_error_path, 'r', encoding='utf8') as fp:
        data = fp.read()
    return json.loads(data)


def while_requests_get(page_url, while_times_define=10, timeout=5):
    """
    循环请求
    :return:
    """
    while_times = 0
    while True:
        try:
            result = requests.get(page_url, headers=headers, timeout=timeout)
            # result = requests.get(page_url, headers=headers, proxies=proxies, timeout=5)
        except Exception as e:
            if while_times < while_times_define:
                while_times += 1
                print('**********', '尝试重新链接', while_times, '次:', page_url)
                continue
            else:
                raise e
        else:
            return result


def get_company_web_to_json():
    """
    获取公司网址
    :return:
    """
    if os.path.exists(company_desc_list_has_web_json_error_path):
        company_desc_list_json = read_company_desc_list_has_web_json_error()
        print('read error data OK')
    else:
        company_desc_list_json = read_company_desc_list_json()
        print('read origin data OK')

    # count = 0
    # for index, item in enumerate(company_desc_list_json):
    #     company_web = item['company_web']
    #     if company_web == '':
    #         count += 1
    #         print('\r%d' % count, end='')

    for company_desc_dict in company_desc_list_json:
        company_web_mask = company_desc_dict['company_web']
        if 'company_web_origin' in company_desc_dict or company_web_mask == '':
            continue
        # company_web_mask =
        # r'https://companylist.org/Details/11549714/Arabia/Webhostmaker_com_Alpha_Reseller_Hosting/clickthru/'
        try:
            result = while_requests_get(company_web_mask, while_times_define=3, timeout=3)
        except (IOError, requests.packages.urllib3.exceptions.MaxRetryError,
                requests.packages.urllib3.exceptions.LocationValueError) as e:
            pattern = r"\w{18}\W\w{4}\W{2}(.+)\W{2}\s\w{4}\W\d{2}"
            re_result = re.search(pattern, str(e))
            if re_result:
                company_desc_dict['company_web_origin'] = 'http://' + re_result.group(1)
                company_desc_dict['company_web_enable'] = False
            else:
                company_desc_dict['company_web_origin'] = ''
                company_desc_dict['company_web_enable'] = False
            print(company_desc_dict['company_id'], company_desc_dict['company_web_origin'], 'False **********')
        except Exception:
            with open(company_desc_list_has_web_json_error_path, 'w', encoding='utf8') as fp:
                fp.write(json.dumps(company_desc_list_json))
        else:
            company_desc_dict['company_web_origin'] = result.url
            company_desc_dict['company_web_enable'] = True
            print(company_desc_dict['company_id'], result.url, 'True')

    with open(company_desc_list_has_web_json_path, 'w', encoding='utf8') as fp:
        fp.write(json.dumps(company_desc_list_json))


def read_data_test():
    """
    读数据测试
    :return:
    """
    with open(company_desc_list_has_web_json_path, 'r', encoding='utf8') as fp:
        data = fp.read()
    read_json = json.loads(data)

    for i in read_json:
        print(i)
        break


if __name__ == '__main__':
    # download_countries_html()
    # parse_countries_html_to_json()
    # download_all_countries_data_1w(times=10)
    # get_has_286_pages_countries()
    # parse_has_286_pages_countries_city()
    # download_has_286_pages_countries_city_company_list(times=10)
    # parse_all_countries_company_list()
    # parse_countries_city_company_list()
    # sum_countries_company_list_json()
    # add_id_to_final_company_list_json()
    # download_all_countries_company_desc(times=10)
    # multiprocessing_download_all_countries_company_desc()
    # parse_all_countries_company_desc_files_to_json()
    # extend_all_company_desc_json()
    get_company_web_to_json()
    # read_data_test()
