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
# import random
from multiprocessing import Pool

home_url = r'http://www.listcompany.org'

home_data = r'E:\work_all\topease\company_spider_2'
countries_list_dir_path = os.path.join(home_data, r'country_list')
company_desc_list_dir_path = os.path.join(home_data, r'company_desc_list')

company_countries_list_html = os.path.join(home_data, r'company_countries_list.html')
countries_url_list_json_path = os.path.join(home_data, r'countries_url_list.json')
company_desc_url_list_path = os.path.join(home_data, r'company_desc_url_list.json')

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:49.0) Gecko/20100101 Firefox/49.0',
    'Connection': 'keep-alive'
}

proxies = {"http": "http://61.135.217.7:80", "https": "http://182.99.195.131:8118", }


def download_countries_list():
    """
    下载国家列表页
    :return:
    """
    result = requests.get(home_url + '/', headers=headers, timeout=5)
    # print(result.text)
    with open(company_countries_list_html, 'w', encoding='utf8') as fp:
        fp.write(result.text)


def parse_countries_list_to_json():
    """
    解析国家列表界面，并持久化为json文件
    :return:
    """
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
    """
    循环请求
    :return:
    """
    while_times = 0
    while True:
        try:
            result = requests.get(page_url, headers=headers, timeout=5)
            # result = requests.get(page_url, headers=headers, proxies=proxies, timeout=5)
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
    """
    下载各个国家的公司列表页面
    :return:
    """
    with open(countries_url_list_json_path, 'r', encoding='utf8') as fp:
        countries_url_list_json = json.loads(fp.read())

    # print(countries_url_list_json)
    for item_country_url_dict in countries_url_list_json:
        country_name = item_country_url_dict['country_name']
        country_url = item_country_url_dict['country_url']
        country_url_home = str(country_url).replace(r'.html', '')

        print(country_name)

        country_dir_path = os.path.join(countries_list_dir_path, country_name)
        if not os.path.exists(country_dir_path):
            os.mkdir(country_dir_path)

        first_page_url = country_url_home + r'/p1.html'
        first_page_url_file_name = first_page_url.replace('http://', '').replace('/', '_')
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
                page_size_href = last_page_select[-1].get('href').replace('.html', '')
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

                if len(inner_context) < 2 << 10:
                    print(1 / 0)
                else:
                    with open(page_url_path, 'w', encoding='utf8') as fp:
                        fp.write(inner_context)

                    print(country_name, page_size, page_url_path)
                    # time.sleep(random.randint(10, 50)/10)

                    # break

                    # print(country_name, country_url)


def parse_countries_company_list_to_json():
    """
    解析各个国家的公司列表到json文件
    :return:
    """
    company_id = 0
    company_desc_url_list = []
    for country_dir in os.listdir(countries_list_dir_path):
        country_dir_path = os.path.join(countries_list_dir_path, country_dir)
        for company_list_file in os.listdir(country_dir_path):
            company_list_file_path = os.path.join(country_dir_path, company_list_file)
            # print(country_dir, company_list_file_path)

            with open(company_list_file_path, 'r', encoding='utf8') as fp:
                context = fp.read()

            soup = BeautifulSoup(context, 'html.parser')

            company_list_select_ul = soup.select('div.body > ul')

            if len(company_list_select_ul) > 0:
                company_list_select = company_list_select_ul[0].select('li')

                for item_company in company_list_select:
                    item_company_name_and_href = item_company.select('h4 > a')[0]
                    company_name = item_company_name_and_href.get('title').strip()
                    company_href = item_company_name_and_href.get('href').strip()

                    company_id += 1

                    temp_dict = {
                        'company_id': company_id,
                        'company_name': company_name,
                        'company_href': company_href,
                    }

                    company_desc_url_list.append(temp_dict)
                    print(country_dir, company_id, company_name, company_href)

    with open(company_desc_url_list_path, 'w', encoding='utf8') as fp:
        fp.write(json.dumps(company_desc_url_list))


def read_company_desc_url_list():
    """
    读取公司列表json
    :return:
    """
    with open(company_desc_url_list_path, 'r', encoding='utf8') as fp:
        data = fp.read()
    return json.loads(data)


def download_company_desc_html(item_company_info_dict):
    """
    下载公司详情页
    :param item_company_info_dict:
    :return:
    """
    company_id = item_company_info_dict['company_id']
    # company_name = item_company_info_dict['company_name']
    company_href = item_company_info_dict['company_href']

    file_name = str(company_href).replace('http://', '').replace('/', '_').replace('-', '_')\
        .replace('.html', '') + '^' + str(company_id) + '.html'
    file_path = os.path.join(company_desc_list_dir_path, file_name)

    if not os.path.exists(file_path):
        result = while_requests_get(company_href)

        if len(result.text) < 2 << 10:
            while True:
                print('##################################################')
                print('####-------------- change IP -----------------####')
                print('##################################################')
                # _r = requests.get(home_url)
                _r = requests.get('http://www.baidu.com')
                if _r.status_code == 200:
                    break
                time.sleep(3)
        else:
            with open(file_path, 'w', encoding='utf8') as fp:
                fp.write(result.text)

            print(file_path)




def multiprocessing_download_files(download_file_func, read_json_list, pool_num=30):
    """
    多进程下载页面
    :return:
    """
    print(len(read_json_list))

    pool = Pool(pool_num)
    pool.map(download_file_func, read_json_list)
    pool.close()
    pool.join()


if __name__ == '__main__':
    # download_countries_list()
    # parse_countries_list_to_json()
    # download_countries_company_list_files()
    # parse_countries_company_list_to_json()
    multiprocessing_download_files(download_company_desc_html, read_company_desc_url_list(), pool_num=200)
