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
import time

catalog_url = r'https://www.365area.com/hscate'

home_data = r'E:\work_all\topease\hs_spider_data'
chapter_href_html_dir = os.path.join(home_data, r'chapter_href_html_dir')
catalog_data = os.path.join(home_data,r'catalog_page_data.html')
catalog_json = os.path.join(home_data,r'spider_hs_code.json')

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:49.0) Gecko/20100101 Firefox/49.0',
    'Connection': 'keep-alive'
}


def get_hs_code_catalog():
    '''
    获取hs编码总目录，并输出为文件
    :return:
    '''
    result = requests.get(catalog_url, headers=headers)
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
                catalog_small_chapter_item_href = str(catalog_small_chapter_items[index].get('href')).replace(r'//','http://')

                spider_hs_code_json[catalog_chapter_index]['chapter_two_list'].append({})
                spider_hs_code_json[catalog_chapter_index]['chapter_two_list'][index]['chapter_two_name'] = catalog_small_chapter_item_text
                spider_hs_code_json[catalog_chapter_index]['chapter_two_list'][index]['chapter_two_href'] = catalog_small_chapter_item_href

    print(spider_hs_code_json)

    with open(catalog_json, 'w', encoding='utf8') as fp:
        fp.write(json.dumps(spider_hs_code_json))


def download_chapter_two_href_html():
    '''
    下载子目录链接的网页
    :return:
    '''
    with open(catalog_json, 'r', encoding='utf8') as fp:
        spider_hs_code_json = json.loads(fp.read())

    all_chapter_two_href_list = []

    for chapter_one_item in spider_hs_code_json:
        chapter_one_item_two_list = chapter_one_item['chapter_two_list']
        for chapter_two_item in chapter_one_item_two_list:
            all_chapter_two_href_list.append(chapter_two_item['chapter_two_href'])

    # print(all_chapter_two_href_list)

    re = 0
    for href in all_chapter_two_href_list:

        new_href = str(href).replace(r'http://','').replace(r'/','_')
        path_dir = os.path.join(chapter_href_html_dir, new_href)

        if not os.path.exists(path_dir):
            os.mkdir(path_dir)

        result = requests.get(href, headers=headers)

        soup = BeautifulSoup(result.text, 'lxml')
        page_select = soup.select('span.pagenext > a')

        if len(page_select) > 0:
            for page_number_item in page_select[::-1]:
                text_temp = page_number_item.text
                try:
                    text_temp_int = int(text_temp)
                except ValueError as e:
                    continue
                else:
                    page_number = text_temp_int
                    break
        else:
            page_number = 1



        for this_page in range(1, page_number+1):
            if this_page != 1:
                new_href_temp = new_href + '_' + str(this_page)
                result = requests.get(href + r'/' + str(this_page), headers=headers)
            else:
                new_href_temp = new_href

            time.sleep(1.5)

            file_path = os.path.join(path_dir, new_href_temp + '.html')
            with open(file_path, 'w', encoding='utf8') as fp:
                fp.write(result.text)

            print(new_href_temp)

        time.sleep(1.5)

        # re += 1
        # if re == 5:
        #     break


if __name__ == "__main__":
    # get_hs_code_catalog()
    # parse_catalog()
    download_chapter_two_href_html()