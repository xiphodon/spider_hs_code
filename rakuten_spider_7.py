#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/28 10:34
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : rakuten_spider_7.py
# @Software: PyCharm

# 乐天
from company_spider_6 import while_requests_get
import os
from bs4 import BeautifulSoup
from lxml import etree
import json

types_url = r'https://directory.rakuten.co.jp/'

data_home = r'E:\work_all\topease\rakuten_spider'

types_html_path = os.path.join(data_home, 'types.html')
all_types_json_path = os.path.join(data_home, 'all_types.json')


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


if __name__ == '__main__':
    # download_types_html()
    parse_types_file_to_types_json()
