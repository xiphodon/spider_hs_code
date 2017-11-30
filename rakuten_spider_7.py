#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/28 10:34
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : rakuten_spider_7.py
# @Software: PyCharm

# 乐天
from company_spider_6 import while_requests_get
from company_spider_6 import multiprocessing_download_files
import os
from bs4 import BeautifulSoup
from lxml import etree
import json

types_url = r'https://directory.rakuten.co.jp/'

data_home = r'E:\work_all\topease\rakuten_spider'

types_html_path = os.path.join(data_home, 'types.html')
all_types_json_path = os.path.join(data_home, 'all_types.json')

html_files_dir_path = os.path.join(data_home, 'html_files')


def download_types_html():
    """
    下载数据类型一览表
    :return:
    """
    result = while_requests_get(types_url)

    with open(types_html_path, 'w', encoding='EUC-JP') as fp:
        fp.write(result.content.decode('EUC-JP'))


def parse_types_file_to_types_json():
    """
    解析数据类型一览表，并存为类型一览表json
    :return:
    """
    with open(types_html_path, 'r', encoding='EUC-JP') as fp:
        context = fp.read()
    # print(context)
    selector = etree.HTML(context)
    types_title_list = selector.xpath('//h2[@class="genreTtl"]/a')
    # print(len(types_title_list))
    types_list_ul_list = selector.xpath('//ul[@class="genreList"]')
    # print(len(types_list_ul_list))

    all_types_json_list = list()
    for item_types_title, item_types_ul in zip(types_title_list, types_list_ul_list):
        temp_dict_1 = dict()

        item_title_href = item_types_title.xpath('./@href')[0]
        item_title_str = item_types_title.xpath('./text()')[0]

        temp_dict_1['type_title_name'] = item_title_str
        temp_dict_1['type_title_href'] = item_title_href
        temp_dict_1['type_list'] = list()

        item_types_href = item_types_ul.xpath('./li/a/@href')
        item_types_str = item_types_ul.xpath('./li/a/text()')

        for item_types_href_item, item_types_str_item in zip(item_types_href, item_types_str):
            temp_dict_2 = dict()
            temp_dict_2['type_name'] = item_types_str_item
            temp_dict_2['type_href'] = item_types_href_item

            temp_dict_1['type_list'].append(temp_dict_2)

        all_types_json_list.append(temp_dict_1)

    # print(json.dumps(all_types_json_list))
    with open(all_types_json_path, 'w', encoding='utf8') as fp:
        fp.write(json.dumps(all_types_json_list))


def get_download_every_type_product_list():
    """
    获取下载每个类型内的商品列表
    :return:
    """
    with open(all_types_json_path, 'r', encoding='utf8') as fp:
        all_types_json = json.loads(fp.read())
    # print(all_types_json)

    multiprocessing_download_origin_list = list()

    for item_type_first in all_types_json:
        type_title_name = item_type_first['type_title_name']
        type_title_href = item_type_first['type_title_href']
        type_list = item_type_first['type_list']

        item_first_type_dir_path = os.path.join(html_files_dir_path, type_title_name)
        if not os.path.exists(item_first_type_dir_path):
            os.mkdir(item_first_type_dir_path)

        # print(type_title_name, type_title_href, type_list)
        for item_type_second in type_list:
            type_name = item_type_second['type_name'].replace('(=>)', '')
            type_href = item_type_second['type_href']
            # print(type_name, type_href)

            item_second_type_dir_path = os.path.join(item_first_type_dir_path, type_name)
            if not os.path.exists(item_second_type_dir_path):
                os.mkdir(item_second_type_dir_path)

            multiprocessing_download_origin_list.append((type_name, type_href, item_second_type_dir_path))

            # break
        # break

    return multiprocessing_download_origin_list


def download_one_type_product_list(item_tuple):
    """
    下载一个类型内的商品列表
    :param type_name: 该类型名称
    :param type_href: 该类型href
    :param item_type_dir_path: 该类型存储目录文件夹path
    :return:
    """
    type_name, type_href, item_type_dir_path = item_tuple
    page_index = 0
    while True:
        page_index += 1
        href_page_suffix = '?p=' + str(page_index)
        file_href = type_href + href_page_suffix

        html_file_path = os.path.join(item_type_dir_path, type_name + '_' + str(page_index) + '.html')

        if os.path.exists(html_file_path):
            continue

        result = while_requests_get(file_href)
        content_data = result.text
        # print(result.content)
        with open(html_file_path, 'w', encoding='utf8') as fp:
            fp.write(content_data)

        print(file_href, 'OK')

        selector = etree.HTML(content_data)

        next_page_button_list = selector.xpath('//a[@class="item -next"]')

        if len(next_page_button_list) == 0:
            break


if __name__ == '__main__':
    # download_types_html()
    # parse_types_file_to_types_json()
    multiprocessing_download_files(download_one_type_product_list, get_download_every_type_product_list(), 10)
