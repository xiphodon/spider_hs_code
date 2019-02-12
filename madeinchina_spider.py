#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/2/5 16:24
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : madeinchina_spider.py
# @Software: PyCharm


import WhileRequests
from lxml import etree
import json
import os
import time

home_url = r'https://www.made-in-china.com/'
suppliers_discovery_url = home_url + r'suppliers-discovery/'

home_path_dir = r'E:\work_all\topease\madeinchina'
suppliers_discovery_path = os.path.join(home_path_dir, 'suppliers_discovery.html')
suppliers_discovery_json_path = os.path.join(home_path_dir, 'suppliers_discovery.json')

suppliers_discovery_all_group_list_dir = os.path.join(home_path_dir, 'all_group_list_dir')

if not os.path.exists(suppliers_discovery_all_group_list_dir):
    os.mkdir(suppliers_discovery_all_group_list_dir)

request = WhileRequests.WhileRequests()


def download_suppliers_discovery():
    """
    下载供应商频道列表
    :return:
    """
    file_content = None

    if os.path.exists(suppliers_discovery_path):

        with open(suppliers_discovery_path, 'r', encoding='utf8') as fp:
            file_content = fp.read()

    else:
        print('get suppliers discovery from web')
        result = request.get(suppliers_discovery_url, request_times=0.5)

        with open(suppliers_discovery_path, 'w', encoding='utf8') as fp:
            fp.write(result.text)

        file_content = result.text

    if file_content is not None:
        # print(file_content)
        selector = etree.HTML(file_content)

        suppliers_discovery_div = selector.xpath('//div[@id="J-sd"]')[0]
        # print(suppliers_discovery_div)

        suppliers_category_name_list = suppliers_discovery_div.xpath('./ul/li/a/text()')
        # print(suppliers_category_name_list)

        suppliers_category_div_list = suppliers_discovery_div.xpath('./div[@class="tab-con"]')
        # print(suppliers_category_div_list)

        suppliers_all_category_url_list = list()

        for category_name, category_div in zip(suppliers_category_name_list, suppliers_category_div_list):
            suppliers_category_url_list = category_div.xpath('.//a[@class="cat-icon"]/@href')

            # print(category_name)
            # print(suppliers_category_url_list)

            for item_url in suppliers_category_url_list:
                temp_dict = {
                    'category_name': category_name,
                    'suppliers_category_url': 'http:' + item_url
                }
                suppliers_all_category_url_list.append(temp_dict)

        # print(suppliers_all_category_url_list)

        with open(suppliers_discovery_json_path, 'w', encoding='utf8') as fp:
            fp.write(json.dumps(suppliers_all_category_url_list))


def get_suppliers_discovery_json():
    """
    获取供应商频道json数据
    :return:
    """
    if not os.path.exists(suppliers_discovery_json_path):
        download_suppliers_discovery()

    with open(suppliers_discovery_json_path, 'r', encoding='utf8') as fp:
        suppliers_all_category_url_list = json.load(fp)

    return suppliers_all_category_url_list


def download_suppliers_category_group_list_html():
    """
    下载供应各商频道的组列表页面
    :return:
    """
    suppliers_all_category_url_list = get_suppliers_discovery_json()

    for item_category in suppliers_all_category_url_list:
        print(item_category)
        suppliers_category_name = item_category['category_name']
        suppliers_category_url = item_category['suppliers_category_url']

        suppliers_category_name_dir = os.path.join(suppliers_discovery_all_group_list_dir, suppliers_category_name)
        if not os.path.exists(suppliers_category_name_dir):
            os.mkdir(suppliers_category_name_dir)

        suppliers_category_file_name = str(suppliers_category_url).replace('http://', '').replace('/', '_')
        suppliers_category_name_url_file_path = os.path.join(suppliers_category_name_dir, suppliers_category_file_name)

        if not os.path.exists(suppliers_category_name_url_file_path):
            print(f'download OK : {suppliers_category_url}')
            result = request.get(suppliers_category_url, request_times=0.5)
            content = result.text

            with open(suppliers_category_name_url_file_path, 'w', encoding='utf8') as fp:
                fp.write(content)


def download_suppliers_category_url_html():
    """
    下载供应商品品类页面
    :return:
    """
    for item_dir_name in os.listdir(suppliers_discovery_all_group_list_dir):
        item_dir_path = os.path.join(suppliers_discovery_all_group_list_dir, item_dir_name)
        for item_file_name in os.listdir(item_dir_path):
            item_file_path = os.path.join(item_dir_path, item_file_name)
            # print(item_file_path)

            with open(item_file_path, 'r', encoding='utf8') as fp:
                content = fp.read()

            selector = etree.HTML(content)

            suppliers_class_1_name = selector.xpath(r'//div[@id="tag-tit"]/div[@id="tag"]/h1/text()')[0]
            print(f'- {suppliers_class_1_name}')

            suppliers_group_list_div = selector.xpath(r'//div[@class="list-group"]')[0]
            # print(suppliers_group_list_div)

            suppliers_class_2_div_list = suppliers_group_list_div.xpath(r'.//div[@class="section"]')
            # print(suppliers_class_2_div_list)

            for item_section in suppliers_class_2_div_list:
                suppliers_class_2_name = item_section.xpath(r'.//div[@class="heading"]/h2/text()')[0]
                print(f'-- {suppliers_class_2_name}')

                suppliers_class_2_category_div = item_section.xpath(r'.//div[@class="list-cat"]')[0]
                # print(suppliers_class_2_category_div)

                suppliers_class_3_li_list = suppliers_class_2_category_div.xpath(r'./ul/li')
                # print(suppliers_class_3_li_list)

                suppliers_class_3_name_last = None
                suppliers_class_3_url_last = None
                for item_class_3_view in suppliers_class_3_li_list:
                    # print(item_class_3_view)

                    suppliers_class_3_name_list = item_class_3_view.xpath(r'./h3/a/text()')
                    if len(suppliers_class_3_name_list) == 0:
                        suppliers_class_3_name = suppliers_class_3_name_last
                        suppliers_class_3_url = suppliers_class_3_url_last
                    else:
                        suppliers_class_3_name = suppliers_class_3_name_list[0]
                        suppliers_class_3_name_last = suppliers_class_3_name
                        suppliers_class_3_url = item_class_3_view.xpath(r'./h3/a/@href')[0]
                        suppliers_class_3_url_last = suppliers_class_3_url

                    print(f'--- {suppliers_class_3_name}')
                    print(suppliers_class_3_url)

                    suppliers_class_4_li_list = item_class_3_view.xpath(r'./ul/li')

                    for item_class_4_view in suppliers_class_4_li_list:
                        suppliers_class_4_name = item_class_4_view.xpath(r'./a/text()')[0]
                        suppliers_class_4_url = item_class_4_view.xpath(r'./a/@href')[0]

                        print(f'---- {suppliers_class_4_name}')
                        print(suppliers_class_4_url)


            break
        break


def start():
    """
    入口
    :return:
    """
    # download_suppliers_category_group_list_html()
    download_suppliers_category_url_html()


if __name__ == '__main__':
    start()
