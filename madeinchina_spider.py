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

home_path_dir = r'E:\madeinchina'
suppliers_discovery_path = os.path.join(home_path_dir, 'suppliers_discovery.html')
suppliers_discovery_json_path = os.path.join(home_path_dir, 'suppliers_discovery.json')

suppliers_discovery_all_group_list_dir = os.path.join(home_path_dir, 'all_group_list_dir')
suppliers_category_company_list_dir = os.path.join(home_path_dir, 'suppliers_list_dir')

suppliers_class_list_json_path = os.path.join(home_path_dir, 'suppliers_class_list.json')

if not os.path.exists(suppliers_category_company_list_dir):
    os.mkdir(suppliers_category_company_list_dir)

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
            result = request.get(suppliers_category_url, request_times=1)
            content = result.text

            with open(suppliers_category_name_url_file_path, 'w', encoding='utf8') as fp:
                fp.write(content)


def download_suppliers_category_url_html():
    """
    下载供应商品品类页面
    :return:
    """
    suppliers_class_list = list()

    # i = 0

    for item_dir_name in os.listdir(suppliers_discovery_all_group_list_dir):
        item_dir_path = os.path.join(suppliers_discovery_all_group_list_dir, item_dir_name)
        for item_file_name in os.listdir(item_dir_path):

            # i += 1
            # if i != 2:
            #     continue

            item_file_path = os.path.join(item_dir_path, item_file_name)
            print(item_file_path)

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

                # if suppliers_class_2_name == 'Special Apparel':
                #     print(etree.tostring(suppliers_class_2_category_div).decode())
                # print(etree.tostring(suppliers_class_2_category_div).decode())

                suppliers_class_3_li_list = suppliers_class_2_category_div.xpath(r'./ul/li')
                suppliers_class_3_li_list_2 = suppliers_class_2_category_div.xpath(r'./li')
                suppliers_class_3_li_list.extend(suppliers_class_3_li_list_2)
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

                    if len(suppliers_class_4_li_list) > 0:
                        # 有第四级别
                        for item_class_4_view in suppliers_class_4_li_list:
                            suppliers_class_4_name = item_class_4_view.xpath(r'./a/text()')[0]
                            suppliers_class_4_url = item_class_4_view.xpath(r'./a/@href')[0]

                            print(f'---- {suppliers_class_4_name}')
                            print(suppliers_class_4_url)

                            temp_dict = {
                                'suppliers_class_1_name': suppliers_class_1_name,
                                'suppliers_class_2_name': suppliers_class_2_name,
                                'suppliers_class_3_name': suppliers_class_3_name,
                                'suppliers_class_3_url': 'https:' + suppliers_class_3_url,
                                'suppliers_class_4_name': suppliers_class_4_name,
                                'suppliers_class_4_url': 'https:' + suppliers_class_4_url,
                            }
                            suppliers_class_list.append(temp_dict)
                    else:
                        # 无第四级别
                        temp_dict = {
                            'suppliers_class_1_name': suppliers_class_1_name,
                            'suppliers_class_2_name': suppliers_class_2_name,
                            'suppliers_class_3_name': suppliers_class_3_name,
                            'suppliers_class_3_url': 'https:' + suppliers_class_3_url,
                            'suppliers_class_4_name': '',
                            'suppliers_class_4_url': '',
                        }
                        suppliers_class_list.append(temp_dict)
            # break
        # break

    print(f'suppliers_class_list len:{len(suppliers_class_list)}')

    with open(suppliers_class_list_json_path, 'w', encoding='utf8') as fp:
        fp.write(json.dumps(suppliers_class_list))


def download_suppliers_list():
    """
    下载供应商列表
    :return:
    """
    with open(suppliers_class_list_json_path, 'r', encoding='utf8') as fp:
        data = json.load(fp)
    # print(data)
    for item_category in data:
        # print(item_category)
        suppliers_class_1_name = item_category.get('suppliers_class_1_name', '')
        suppliers_class_2_name = item_category.get('suppliers_class_2_name', '')
        suppliers_class_3_name = item_category.get('suppliers_class_3_name', '')
        suppliers_class_3_url = item_category.get('suppliers_class_3_url', '')
        suppliers_class_4_name = item_category.get('suppliers_class_4_name', '')
        suppliers_class_4_url = item_category.get('suppliers_class_4_url', '')

        # print(suppliers_class_1_name)
        # print(suppliers_class_2_name)
        # print(suppliers_class_3_name)
        # print(suppliers_class_3_url)
        # print(suppliers_class_4_name)
        # print(suppliers_class_4_url)

        if suppliers_class_4_name != '':
            url = suppliers_class_4_url
            dir_name = \
                f'{suppliers_class_1_name}_{suppliers_class_2_name}_{suppliers_class_3_name}_{suppliers_class_4_name}'
        else:
            url = suppliers_class_3_url
            dir_name = f'{suppliers_class_1_name}_{suppliers_class_2_name}_{suppliers_class_3_name}'

        dir_path = os.path.join(suppliers_category_company_list_dir, dir_name)

        if not os.path.exists(dir_path):
            os.mkdir(dir_path)

        while True:

            file_name_1 = url.replace('https://www.made-in-china.com/', '').replace('/', '_')
            file_name_2 = url.replace('https://', '').replace('/', '_')
            file_path_1 = os.path.join(dir_path, file_name_1)
            file_path_2 = os.path.join(dir_path, file_name_2)

            if os.path.exists(file_path_1) and os.path.getsize(file_path_1) > 10 * 2 << 10:
                print(file_path_1)
                with open(file_path_1, 'r', encoding='utf8') as fp:
                    content = fp.read()
            elif os.path.exists(file_path_2) and os.path.getsize(file_path_2) > 10 * 2 << 10:
                print(file_path_2)
                with open(file_path_2, 'r', encoding='utf8') as fp:
                    content = fp.read()
            else:
                print(file_path_1)
                result = request.get(url, sleep_time=2.0)
                content = result.text

                with open(file_path_1, 'w', encoding='utf8') as fp:
                    fp.write(content)

            selector = etree.HTML(content)

            next_url_list = selector.xpath(r'.//a[@class="next"]/@href')

            # 是否有下一页
            if len(next_url_list) > 0:
                url = f'https:{next_url_list[0]}'
            else:
                break


def start():
    """
    入口
    :return:
    """
    # download_suppliers_category_group_list_html()
    # download_suppliers_category_url_html()
    download_suppliers_list()


if __name__ == '__main__':
    start()
