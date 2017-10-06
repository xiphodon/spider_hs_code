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
    解析目录文件
    :return:
    '''
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

    print(catalog_chapter_list)



if __name__ == "__main__":
    # get_hs_code_catalog()
    parse_catalog()