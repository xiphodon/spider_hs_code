#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/10/6 19:34
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : hs_spider.py
# @Software: PyCharm

import json
import os
import requests
from bs4 import BeautifulSoup

catalog_url = r'https://www.365area.com/hscate'

home_data = r'E:\work_all\topease\hs_spider_data'
catalog_data = os.path.join(home_data,r'catalog_page_data.html')
catalog_json = os.path.join(home_data,r'spider_hs_code.json')


def get_hs_code_catalog():
    '''
    获取hs编码总目录，并输出为文件
    :return:
    '''
    result = requests.get(catalog_url)
    with open(catalog_data, 'w', encoding='utf-8') as fp:
        fp.write(result.text)


def parse_catalog():
    '''
    解析目录文件，并输出为json文件
    :return:
    '''
    spider_hs_code_json = []
    # 大类目录列表
    catalog_chapter_list = []

    with open(catalog_data, 'r', encoding='utf-8') as fp:
        catalog_content = fp.read()

    soup = BeautifulSoup(str(catalog_content), 'lxml')
    catalog_chapter_items = soup.select('div.catehs > a')
    # print(catalog_chapter_items)
    if len(catalog_chapter_items) > 0:
        for catalog_chapter_item in catalog_chapter_items:
            catalog_chapter_list.append(catalog_chapter_item.text.strip())

    # print(catalog_chapter_list)
    for i in range(len(catalog_chapter_list)):
        spider_hs_code_json.append({})
        spider_hs_code_json[i]['chapter_one_name'] = catalog_chapter_list[i]
        spider_hs_code_json[i]['chapter_two_list'] = []

    print(spider_hs_code_json)


    for catalog_chapter_index in range(len(catalog_chapter_list)):
        # print("====== ", catalog_chapter_list[catalog_chapter_index], " ======")
        catalog_chapter_index_str = str(catalog_chapter_index + 1)
        catalog_small_chapter_items = soup.select('div#c' + catalog_chapter_index_str + 'list' + ' > ' + 'div.catechapter > a')
        if len(catalog_small_chapter_items) > 0:
            for index in range(len(catalog_small_chapter_items)):
                catalog_small_chapter_item_text = catalog_small_chapter_items[index].text.strip()
                catalog_small_chapter_item_href = str(catalog_small_chapter_items[index].get('href')).replace(r'//','')

                spider_hs_code_json[catalog_chapter_index]['chapter_two_list'].append({})
                spider_hs_code_json[catalog_chapter_index]['chapter_two_list'][index]['chapter_two_name'] = catalog_small_chapter_item_text
                spider_hs_code_json[catalog_chapter_index]['chapter_two_list'][index]['chapter_two_href'] = catalog_small_chapter_item_href

    print(spider_hs_code_json)

    with open(catalog_json, 'w', encoding='utf8') as fp:
        fp.write(json.dumps(spider_hs_code_json))





if __name__ == "__main__":
    # get_hs_code_catalog()
    # parse_catalog()