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
from company_spider_9 import gevent_pool_requests
import os
from bs4 import BeautifulSoup
from lxml import etree
import json
import time
import hashlib

types_url = r'https://directory.rakuten.co.jp/'

data_home = r'E:\work_all\topease\rakuten_spider'

types_html_path = os.path.join(data_home, 'types.html')
all_types_json_path = os.path.join(data_home, 'all_types.json')
all_shop_info_json_path = os.path.join(data_home, 'all_shop_info.json')
shop_href_mapping_json_path = os.path.join(data_home, 'shop_href_mapping.json')
shop_info_json_path = os.path.join(data_home, 'shop_info.json')
all_products_json_path = os.path.join(data_home, 'all_products.json')

html_files_dir_path = os.path.join(data_home, 'html_files')
all_shop_info_dir_path = os.path.join(data_home, 'all_shop_info_dir')

if not os.path.exists(html_files_dir_path):
    os.mkdir(html_files_dir_path)

if not os.path.exists(all_shop_info_dir_path):
    os.mkdir(all_shop_info_dir_path)


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

        next_page_button_list = selector.xpath('//a[@class="item -next nextPage"]')
        this_page_text_list = selector.xpath('//a[@class="item -active currentPage"]/text()')

        if len(this_page_text_list) > 0:
            if len(next_page_button_list) == 0:
                break
            if this_page_text_list[0].strip() != str(page_index):
                if os.path.exists(html_file_path):
                    os.remove(html_file_path)
                break
        else:
            break


def parse_all_type_product_list_files():
    """
    解析所有类型的商品列表页面
    :return:
    """

    for type_first_dir in os.listdir(html_files_dir_path):
        type_first_dir_path = os.path.join(html_files_dir_path, type_first_dir)
        for type_second_dir in os.listdir(type_first_dir_path):

            print(type_first_dir, type_second_dir)

            type_second_dir_path = os.path.join(type_first_dir_path, type_second_dir)
            for item_file_name in os.listdir(type_second_dir_path):
                item_file_path = os.path.join(type_second_dir_path, item_file_name)

                try:
                    with open(item_file_path, 'r', encoding='utf8') as fp:
                        # content = fp.read()
                        selector = etree.HTML(fp.read())

                    product_div_list = selector.xpath('//div[@class="dui-container searchresults"]/div[@class="dui-cards searchresultitems"]/div[@class="dui-card searchresultitem"]')
                    # print(len(product_div_list))

                    for item_product_div in product_div_list:
                        try:
                            shop_a = item_product_div.xpath('.//div[@class="content merchant _ellipsis"]/a')[0]
                            shop_href = shop_a.xpath('./@href')[0]
                            shop_text = shop_a.xpath('./text()')[0]
                            # print(shop_href, shop_text)

                            product_title_a = item_product_div.xpath('.//div[@class="content title"]/h2/a')[0]
                            product_title_href = product_title_a.xpath('./@href')[0]
                            product_title_text = product_title_a.xpath('./@title')[0]
                            # print(product_title_href, product_title_text)

                            product_price_span = item_product_div.xpath('.//div[@class="content price"]/span')[0]
                            product_price_text = product_price_span.xpath('./text()')[0]
                            product_price_suffix = product_price_span.xpath('./small/text()')[0]
                            product_price = product_price_text + product_price_suffix
                            # print(product_price)

                            product_score_text_list = item_product_div.xpath('.//span[@class="score"]/text()')
                            product_legend_text_list = item_product_div.xpath('.//span[@class="legend"]/text()')

                            if len(product_score_text_list) > 0:
                                product_score_text = product_score_text_list[0]
                            else:
                                product_score_text = ''

                            if len(product_legend_text_list) > 0:
                                product_legend_text = product_legend_text_list[0][1:-2]
                            else:
                                product_legend_text = ''

                            # print(product_score_text, product_legend_text)

                            temp_product_dict = {'shop_name': shop_text,
                                                 'shop_href': shop_href,
                                                 'product_title': product_title_text,
                                                 'product_href': product_title_href,
                                                 'product_price': product_price,
                                                 'product_score': product_score_text,
                                                 'product_legend': product_legend_text,
                                                 'product_type_first': type_first_dir,
                                                 'product_type_second': type_second_dir}

                            shop_href_md5 = hashlib.md5(shop_href.encode('utf8')).hexdigest()

                            shop_href_md5_dir_path = os.path.join(all_shop_info_dir_path, shop_href_md5)

                            if not os.path.exists(shop_href_md5_dir_path):
                                os.mkdir(shop_href_md5_dir_path)
                                file_mark_path = os.path.join(shop_href_md5_dir_path, 'shop_href.txt')
                                with open(file_mark_path, 'w', encoding='utf8') as fp:
                                    fp.write(shop_href)

                            file_path = os.path.join(shop_href_md5_dir_path, 'products.txt')

                            with open(file_path, 'a', encoding='utf8') as fp:
                                fp.write(json.dumps(temp_product_dict) + '\n')

                            # print(temp_product_dict)
                            # print(shop_href)
                            # break
                        except Exception as e:
                            print('***** inner', e)
                            # raise e
                            continue
                except Exception as e:
                    print('===== outter', e)
                    # raise e
                    continue
                # break
            # break
        # break


def get_shop_info_href_list():
    """
    通过shop_href获取shop_info_href 的列表
    :return:
    """
    shop_info_href_list = list()
    for shop_href_md5 in os.listdir(all_shop_info_dir_path):
        # print(shop_href_md5)

        shop_href_md5_path = os.path.join(all_shop_info_dir_path, shop_href_md5)

        shop_info_html_path = os.path.join(shop_href_md5_path, 'shop_info.html')

        shop_href_txt_path = os.path.join(shop_href_md5_path, 'shop_href.txt')

        with open(shop_href_txt_path, 'r', encoding='utf8') as fp:
            shop_href = fp.read().strip()

        shop_info_href = shop_href + 'info.html'

        temp_tuple = (shop_info_html_path, shop_info_href)

        shop_info_href_list.append(temp_tuple)

        print(temp_tuple)

    print(shop_info_href_list[:10] if len(shop_info_href_list) > 10 else shop_info_href_list)
    return shop_info_href_list


def download_shop_info_file(shop_info_html_path_and_shop_info_href):
    """
    下载一个商家信息页面
    :param shop_info_html_path_and_shop_info_href: 应存储地址 + 详情页请求地址
    :return:
    """
    # shop_info_html_path: 应存储地址
    # shop_info_href: 详情页请求地址
    shop_info_html_path, shop_info_href = shop_info_html_path_and_shop_info_href

    if os.path.exists(shop_info_html_path):
        return

    info_result = while_requests_get(shop_info_href)

    with open(shop_info_html_path, 'w', encoding='EUC-JP') as fp:
        fp.write(info_result.content.decode('EUC-JP'))

    print(shop_info_html_path, 'save OK')


def download_shop_info_files():
    """
    下载所有商家信息页面
    :return:
    """
    error_count = 0
    for shop_href_md5 in os.listdir(all_shop_info_dir_path):
        try:
            shop_href_md5_path = os.path.join(all_shop_info_dir_path, shop_href_md5)
            print(shop_href_md5)

            shop_info_html_path = os.path.join(shop_href_md5_path, 'shop_info.html')
            if os.path.exists(shop_info_html_path):
                continue

            shop_href_txt_path = os.path.join(shop_href_md5_path, 'shop_href.txt')

            with open(shop_href_txt_path, 'r', encoding='utf8') as fp:
                shop_href = fp.read().strip()

            # result = while_requests_get(shop_href)
            #
            # selector = etree.HTML(result.text)
            # # <meta property="og:url" content="https://www.rakuten.co.jp/mono-b/"/>
            # shop_info_href_list = selector.xpath('//meta[@property="og:url"]/@content')
            #
            # if len(shop_info_href_list) > 0:
            #     shop_info_href = shop_info_href_list[0] + 'info.html'
            #     # print(shop_info_href)
            #     info_result = while_requests_get(shop_info_href)
            #
            #     with open(shop_info_html_path, 'w', encoding='EUC-JP') as fp:
            #         fp.write(info_result.content.decode('EUC-JP'))
            #
            #     print(shop_href_md5, 'save OK')

            shop_info_href = shop_href + 'info.html'
            info_result = while_requests_get(shop_info_href)

            with open(shop_info_html_path, 'w', encoding='EUC-JP') as fp:
                fp.write(info_result.content.decode('EUC-JP'))

            print(shop_href_md5, 'save OK')
            print()
        except Exception as e:
            print(e)
            error_count += 1
            print(error_count)
        # break
    print(error_count)


def parse_shop_info_to_json():
    """
    解析商家信息并存为json文件
    :return:
    """
    shop_info_json = list()
    for shop_info_md5 in os.listdir(all_shop_info_dir_path):
        try:
            shop_info_md5_path = os.path.join(all_shop_info_dir_path, shop_info_md5)
            # print(shop_info_md5_path)
            shop_info_html_path = os.path.join(shop_info_md5_path, 'shop_info.html')
            if os.path.exists(shop_info_html_path):
                with open(shop_info_html_path, 'r', encoding='EUC-JP') as fp:
                    content = fp.read()
                # print(content)

                soup = BeautifulSoup(content, 'lxml')

                # shop_info_list = selector.xpath('//tr/td[@valign="top"]/font/dl/dt/text()')
                shop_info_dl_list = soup.select('td[valign="top"] > font > dl')

                if len(shop_info_dl_list) > 0:
                    shop_info_dl = shop_info_dl_list[0]

                    shop_info_dl_dt_list = shop_info_dl.select('dt')
                    shop_info_dl_dd_list = shop_info_dl.select('dd')

                    if len(shop_info_dl_dt_list) >= 2 and len(shop_info_dl_dd_list) >= 1:
                        company_name = shop_info_dl.select('dt')[1].text.strip()
                        shop_info_dl_dd = shop_info_dl.select('dd')[0].text.strip()

                        # print(company_name)
                        # print(shop_info_dl_dd)

                        shop_info_list = [x.strip() for x in shop_info_dl_dd.split('\n')]

                        if len(shop_info_list) != 6:
                            continue

                        # print(shop_info_list)
                        company_address = shop_info_list[0]
                        company_tel = shop_info_list[1].split('  ')[0].split(':')[-1]
                        company_fax = shop_info_list[1].split('  ')[1].split(':')[-1]
                        company_info_001 = shop_info_list[2].split(':')
                        company_info_002 = shop_info_list[3].split(':')
                        company_info_003 = shop_info_list[4].split(':')
                        company_info_004 = shop_info_list[5].split(':')

                        temp_dict = {
                            'company_md5': shop_info_md5,
                            'company_name': check_str(company_name),
                            'company_address': check_str(company_address),
                            'company_tel': check_str(company_tel),
                            'company_fax': check_str(company_fax),
                            'company_representative': check_str(company_info_001[1]),
                            'company_operator': check_str(company_info_002[1]),
                            'company_shopowner': check_str(company_info_003[1]),
                            'company_email': check_str(company_info_004[1]),
                        }

                        shop_info_json.append(temp_dict)

                        # print(company_name)
                        # print(company_address)
                        # print(company_tel)
                        # print(company_fax)
                        # print(company_info_001)
                        # print(company_info_002)
                        # print(company_info_003)
                        # print(company_info_004)

                        print(temp_dict)
        except Exception as e:
            print(e)

    with open(shop_info_json_path, 'w', encoding='utf8') as fp:
        fp.write(json.dumps(shop_info_json))


def read_shop_info_json():
    """
    读取商家信息json
    :return:
    """
    with open(shop_info_json_path, 'r', encoding='utf8') as fp:
        data = json.loads(fp.read())
    return data


def read_products_json():
    """
    读取产品信息json
    :return:
    """
    with open(all_products_json_path, 'r', encoding='utf8') as fp:
        data = json.loads(fp.read())
    return data


def check_str(_str):
    """
    检查字符串 \n, \xa0, \xc2, \u3000 等特殊字符
    :param _str:
    :return:
    """
    problem_str_list = ['\n', '\xa0', '\xc2', '\u3000', '<br />', '&nbsp;']
    for item_problem in problem_str_list:
        _str = _str.replace(item_problem, ' ')

    strip_str_list = [',', ' ']
    for item_strip in strip_str_list:
        _str = _str.strip(item_strip)
    return _str


def merge_all_product_to_json():
    """
    合并所有产品并存为json文件
    :return:
    """

    with open(all_products_json_path, 'w', encoding='utf8') as fw:
        fw.write('[')

        for shop_info_md5 in os.listdir(all_shop_info_dir_path):
            print(shop_info_md5)
            shop_info_md5_path = os.path.join(all_shop_info_dir_path, shop_info_md5)
            products_txt_path = os.path.join(shop_info_md5_path, 'products.txt')

            with open(products_txt_path, 'r', encoding='utf8') as fp:
                for line in fp.readlines():
                    line_str = line.strip('\n')
                    line_dict = json.loads(line_str)
                    line_dict['shop_md5'] = shop_info_md5

                    fw.write(json.dumps(line_dict) + ',')

                    # break

            # break

        fw.seek(fw.tell() - 1, 0)
        fw.write(']')


if __name__ == '__main__':
    # download_types_html()
    # parse_types_file_to_types_json()
    ## multiprocessing_download_files(download_one_type_product_list, get_download_every_type_product_list(), 100) # 废弃
    # gevent_pool_requests(download_one_type_product_list, get_download_every_type_product_list(), gevent_pool_size=100)
    # parse_all_type_product_list_files()
    ## download_shop_info_files() # 废弃
    gevent_pool_requests(download_shop_info_file, get_shop_info_href_list(), gevent_pool_size=100)
    # parse_shop_info_to_json()
    # print(len(read_shop_info_json()))
    # merge_all_product_to_json()

