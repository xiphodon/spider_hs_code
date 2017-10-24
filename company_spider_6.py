#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/10/23 9:12
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : company_spider_6.py
# @Software: PyCharm

import json
import os
import requests
from bs4 import BeautifulSoup
# import time
# import random
from multiprocessing import Pool

home_url = r'http://buyer.waimaoba.com'

home_data = r'E:\work_all\topease\company_spider_4'
industry_list_dir_path = os.path.join(home_data, r'industry_list')
company_desc_dir_path = os.path.join(home_data, r'company_desc')

industry_list_html = os.path.join(home_data, r'industry_list.html')

industry_list_json_path = os.path.join(home_data, r'industry_list.json')
company_list_href_json_path = os.path.join(home_data, r'company_list_href.json')
company_desc_url_list_json_path = os.path.join(home_data, r'company_desc_url_list.json')

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:49.0) Gecko/20100101 Firefox/49.0',
    'Connection': 'keep-alive'
}


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


def download_industry_html():
    """
    下载产业名录页
    :return:
    """
    result = while_requests_get(home_url)
    # print(result.encoding)
    with open(industry_list_html, 'w', encoding='utf8') as fp:
        fp.write(bytes(result.text, 'iso-8859-1').decode('utf-8'))


def parse_industry_html_to_json():
    """
    解析产业名录页面
    :return:
    """
    industry_list_json = []
    with open(industry_list_html, 'r', encoding='utf8') as fp:
        context = fp.read()

    soup = BeautifulSoup(context, 'html.parser')
    industry_select = soup.select('div.content > ul.menu')[0].select('li')
    for item_industry in industry_select:
        industry_name = item_industry.text.strip()
        industry_href = home_url + item_industry.select('a')[0].get('href')

        temp_dict = {
            'industry_name': industry_name,
            'industry_href': industry_href
        }

        industry_list_json.append(temp_dict)

    with open(industry_list_json_path, 'w', encoding='utf8') as fp:
        fp.write(json.dumps(industry_list_json))


def read_industry_list_json():
    """
    读取产业名录json文件
    :return:
    """
    with open(industry_list_json_path, 'r', encoding='utf8') as fp:
        data = fp.read()
    return json.loads(data)


def get_company_list_files_to_json():
    """
    获取所有的公司列表页面，生成json文件
    :return:
    """

    company_list_href_json = []
    industry_list_json = read_industry_list_json()

    for item_industry_dict in industry_list_json:
        industry_name = item_industry_dict['industry_name']
        industry_href = item_industry_dict['industry_href']

        industry_name_path = os.path.join(industry_list_dir_path, industry_name)

        if not os.path.exists(industry_name_path):
            os.mkdir(industry_name_path)

        industry_href_name = industry_href.replace('http://', '').replace('/', '_') + '.html'
        industry_href_path = os.path.join(industry_name_path, industry_href_name)

        if os.path.exists(industry_href_path):
            with open(industry_href_path, 'r', encoding='utf8') as fp:
                context = fp.read()
        else:
            result = while_requests_get(industry_href)
            context = bytes(result.text, 'iso-8859-1').decode('utf-8')
            with open(industry_href_path, 'w', encoding='utf8') as fp:
                fp.write(context)

        soup = BeautifulSoup(context, 'html.parser')
        pages_size_select_list = soup.select('div.links > a')

        if len(pages_size_select_list) == 0:
            print(industry_name, 'only one page')
            continue

        pages_size_select = pages_size_select_list[-1]
        pages_size_href_str = str(pages_size_select.get('href'))

        pages_size_str_index_start = pages_size_href_str.rfind('_') + 1
        pages_size_str_index_end = pages_size_href_str.rfind('.html')

        pages_size_str = pages_size_href_str[pages_size_str_index_start:pages_size_str_index_end]

        # http://buyer.waimaoba.com/agriculture-food/index_2.html
        for pages_index in range(2, int(pages_size_str) + 1):
            this_industry_href = industry_href + 'index_' + str(pages_index) + '.html'

            company_list_href_json.append({
                'industry_name': industry_name,
                'industry_href': this_industry_href
            })

    with open(company_list_href_json_path, 'w', encoding='utf8') as fp:
        fp.write(json.dumps(company_list_href_json))


def read_company_list_href_json():
    """
    读取公司列表href的json文件
    :return:
    """
    with open(company_list_href_json_path, 'r', encoding='utf8') as fp:
        data = fp.read()
    return json.loads(data)


def download_company_list_files(item_dict):
    """
    下载所有的公司列表页
    :return:
    """
    industry_name = item_dict['industry_name']
    industry_href = item_dict['industry_href']

    industry_name_path = os.path.join(industry_list_dir_path, industry_name)

    file_name = industry_href.replace('http://', '').replace('/', '_')
    file_path = os.path.join(industry_name_path, file_name)

    if not os.path.exists(file_path):
        result = while_requests_get(industry_href)
        context = bytes(result.text, 'iso-8859-1').decode('utf-8')
        with open(file_path, 'w', encoding='utf8') as fp:
            fp.write(context)
        print(industry_name, industry_href, 'OK')
    else:
        print(industry_name, file_name, 'saved')


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


def parse_company_list_files_to_json():
    """
    解析所有公司列表页，生成json文件
    :return:
    """
    company_desc_url_list_json = []

    for industry_dir in os.listdir(industry_list_dir_path):
        industry_dir_path = os.path.join(industry_list_dir_path, industry_dir)
        for file_name in os.listdir(industry_dir_path):
            print(industry_dir, file_name)
            file_path = os.path.join(industry_dir_path, file_name)

            with open(file_path, 'r', encoding='utf8') as fp:
                context = fp.read()

            soup = BeautifulSoup(context, 'html.parser')
            company_list_select = soup.select('table.sticky-enabled > tbody > tr')

            for item_select in company_list_select:
                item_company_sample = item_select.select('td')

                if len(item_company_sample) != 3:
                    continue

                company_name = item_company_sample[0].text.strip()
                company_href = home_url + item_company_sample[0].select('a')[0].get('href')
                company_industry = industry_dir
                company_country = item_company_sample[2].text.strip()

                temp_dict = {
                    'company_name': company_name,
                    'company_href': company_href,
                    'company_industry': company_industry,
                    'company_country': company_country
                }

                company_desc_url_list_json.append(temp_dict)

    print(len(company_desc_url_list_json))

    with open(company_desc_url_list_json_path, 'w', encoding='utf8') as fp:
        fp.write(json.dumps(company_desc_url_list_json))


def read_company_desc_url_list_json():
    """
    读取公司详情链接列表的json文件
    :return:
    """
    with open(company_desc_url_list_json_path, 'r', encoding='utf8') as fp:
        data = fp.read()
    return json.loads(data)


def download_company_desc_file(company_dict):
    """
    下载当前公司的详情页
    :param company_dict:
    :return:
    """
    # company_name = company_dict['company_name']
    company_href = company_dict['company_href']
    company_industry = company_dict['company_industry']
    # company_country = company_dict['company_country']

    company_industry_dir = os.path.join(company_desc_dir_path, company_industry)
    if not os.path.exists(company_industry_dir):
        os.mkdir(company_industry_dir)

    file_name = company_href.replace('http://', '').replace('/', '_').replace('?', '_')
    file_path = os.path.join(company_industry_dir, file_name)

    if not os.path.exists(file_path):
        result = while_requests_get(company_href)
        context = bytes(result.text, 'iso-8859-1').decode('utf-8')
        with open(file_path, 'w', encoding='utf8') as fp:
            fp.write(context)
        print(company_industry, company_href, 'OK')
    else:
        print(company_industry, file_name, 'saved')


if __name__ == '__main__':
    # download_industry_html()
    # parse_industry_html_to_json()
    # get_company_list_files_to_json()
    # multiprocessing_download_files(download_company_list_files, read_company_list_href_json())
    # parse_company_list_files_to_json()
    # download_company_desc_file(read_company_desc_url_list_json()[0])
    multiprocessing_download_files(download_company_desc_file, read_company_desc_url_list_json(), pool_num=10)
