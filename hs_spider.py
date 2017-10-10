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
# 二类目录网页文件夹
chapter_href_html_dir = os.path.join(home_data, r'chapter_href_html_dir')
# 所有hs编码详情网页文件夹
all_hs_code_item_html_dir = os.path.join(home_data, r'all_hs_code_item_html_dir')
# 一类目录网页
catalog_data = os.path.join(home_data, r'catalog_page_data.html')
# 一类名称和二类名称及链接json
catalog_json = os.path.join(home_data, r'spider_hs_code.json')
# 包含二类下所有hs编码的json
catalog_json_2 = os.path.join(home_data, r'spider_hs_code_2.json')
# 包含二类下所有hs编码详情的json
catalog_json_3 = os.path.join(home_data, r'spider_hs_code_3.json')

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
        catalog_small_chapter_items = soup.select(
            'div#c' + catalog_chapter_index_str + 'list' + ' > ' + 'div.catechapter > a')
        if len(catalog_small_chapter_items) > 0:
            for index in range(len(catalog_small_chapter_items)):
                catalog_small_chapter_item_text = catalog_small_chapter_items[index].text.strip()
                catalog_small_chapter_item_href = str(catalog_small_chapter_items[index].get('href')).replace(r'//',
                                                                                                              'http://')

                spider_hs_code_json[catalog_chapter_index]['chapter_two_list'].append({})
                spider_hs_code_json[catalog_chapter_index]['chapter_two_list'][index][
                    'chapter_two_name'] = catalog_small_chapter_item_text
                spider_hs_code_json[catalog_chapter_index]['chapter_two_list'][index][
                    'chapter_two_href'] = catalog_small_chapter_item_href

    print(spider_hs_code_json)

    with open(catalog_json, 'w', encoding='utf8') as fp:
        fp.write(json.dumps(spider_hs_code_json))


def get_spider_hs_code_json():
    '''
    获取持久化的json文件
    :return:
    '''
    with open(catalog_json, 'r', encoding='utf8') as fp:
        spider_hs_code_json = json.loads(fp.read())
    return spider_hs_code_json


def download_chapter_two_href_html():
    '''
    下载子目录链接的网页
    :return:
    '''
    spider_hs_code_json = get_spider_hs_code_json()

    all_chapter_two_href_list = []

    for chapter_one_item in spider_hs_code_json:
        chapter_one_item_two_list = chapter_one_item['chapter_two_list']
        for chapter_two_item in chapter_one_item_two_list:
            all_chapter_two_href_list.append(chapter_two_item['chapter_two_href'])

    # print(all_chapter_two_href_list)

    for href in all_chapter_two_href_list:

        new_href = str(href).replace(r'http://', '').replace(r'/', '_')
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

        for this_page in range(1, page_number + 1):
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


def parse_all_chapter_two_html():
    '''
    解析所有的二级目录网页
    :return:
    '''
    all_chapter_hs_code_list = []

    # 章节文件夹列表遍历
    for item_dir in os.listdir(chapter_href_html_dir):
        # 各章节文件夹中的文件列表遍历
        item_dir_path = os.path.join(chapter_href_html_dir, item_dir)
        # 当前章节所有字段json
        this_chapter_hs_code_list = []

        for item_html in os.listdir(item_dir_path):
            item_html_path = os.path.join(item_dir_path, item_html)
            with open(item_html_path, 'r', encoding='utf8') as fp:
                item_html_text = fp.read()
            soup = BeautifulSoup(str(item_html_text), 'lxml')
            hs_td_select = soup.select('div.hssearchcon > table > tr > td')
            hs_td_ab_select = soup.select('td.tdtoth')

            for hs_td_ab_select_item in hs_td_ab_select:
                if hs_td_ab_select_item in hs_td_select:
                    hs_td_select.remove(hs_td_ab_select_item)

            if len(hs_td_select) % 4 != 0:
                print('parse td len error')
                return

            for hs_td_select_index in range(int(len(hs_td_select) / 4)):
                hs_code_all = hs_td_select[hs_td_select_index * 4 + 0].text.strip()
                type_name = ''.join(hs_td_select[hs_td_select_index * 4 + 1].text.strip().split('\n')).replace(' ', '')
                example_times = hs_td_select[hs_td_select_index * 4 + 2].text.strip()
                details = hs_td_select[hs_td_select_index * 4 + 3].select('a')[0].get('href').replace('//', '').strip()

                hs_code_all_list = hs_code_all.split('\n')

                hs_code = hs_code_all_list[0].replace(' ', '')
                hs_code_invalid = ''
                hs_code_recommend = ''

                if len(hs_code_all_list) == 3:
                    hs_code = hs_code_all_list[0].replace(' ', '')
                    hs_code_invalid = hs_code_all_list[1].replace('(', '').replace(')', '').replace(' ', '').replace(
                        '\xa0', '')
                    hs_code_recommend = hs_code_all_list[2]
                elif len(hs_code_all_list) > 3 or len(hs_code_all_list) == 2:
                    print('len(hs_code_all_list) error')

                # print(hs_code, hs_code_invalid, hs_code_recommend, type_name, example_times, details)

                hs_code_item_dict = {
                    'hs_code': hs_code,
                    'hs_code_invalid': hs_code_invalid,
                    'hs_code_recommend': hs_code_recommend,
                    'type_name': type_name,
                    'example_times': example_times,
                    'details': details
                }

                this_chapter_hs_code_list.append(hs_code_item_dict)

                # print(item_html)
                # print(this_chapter_hs_code_list)
                # break

        all_chapter_hs_code_list.append(this_chapter_hs_code_list)
        # break

    with open(catalog_json_2, 'w', encoding='utf8') as fp:
        fp.write(json.dumps(all_chapter_hs_code_list))


def get_all_chapter_hs_code_list_json():
    '''
    获取二级目录详情的json文件
    :return:
    '''
    with open(catalog_json_2, 'r', encoding='utf8') as fp:
        chapter_hs_code_list_stream = fp.read()
    return json.loads(chapter_hs_code_list_stream)


def download_all_hs_code_item_html():
    '''
    下载所有的hs详细内容的html网页
    :return:
    '''
    all_chapter_hs_code_json = get_all_chapter_hs_code_list_json()

    all_chapter_hs_code_href_list = []

    for item_list in all_chapter_hs_code_json:
        for item in item_list:
            detail_href = 'http://' + item['details']
            all_chapter_hs_code_href_list.append(detail_href)

    # count = 0
    for href in all_chapter_hs_code_href_list:
        file_name = str(href).replace(r'http://', '').replace(r'/', '_') + '.html'
        file_path = os.path.join(all_hs_code_item_html_dir, file_name)

        if os.path.exists(file_path):
            continue

        result = requests.get(href, headers=headers)
        with open(file_path, 'w', encoding='utf8') as fp:
            fp.write(result.text)

        time.sleep(0.1)
        # count += 1
        # if count == 5:
        #     break
        print(href)


def parse_all_hs_code_desc_html():
    '''
    解析所有的hs编码的详情页
    :return:
    '''

    count = 0

    for file_path in os.listdir(all_hs_code_item_html_dir):
        # if file_path != 'www.365area.com_hscode_detail_0106199090.html':
        if file_path != 'www.365area.com_hscode_detail_3101001990.html':
            continue
        with open(os.path.join(all_hs_code_item_html_dir, file_path), 'r', encoding='utf8') as fp:
            soup = BeautifulSoup(fp, 'lxml')
            # print(soup)

            chapter_and_class_select = soup.select('div.clashow > a')
            # print(chapter_and_class_select)
            class_name = chapter_and_class_select[0].text
            chapter_name = chapter_and_class_select[1].text

            hs_code_desc_select = soup.select('div.scx_item > div.row_0')

            key1 = hs_code_desc_select[0].select('div.odd')[0].text.replace('\n', '').strip()
            key2 = hs_code_desc_select[1].select('div.odd')[0].text.replace('\n', '').strip()
            key3 = hs_code_desc_select[2].select('div.odd')[0].text.replace('\n', '').strip()
            key4 = hs_code_desc_select[3].select('div.odd')[0].text.replace('\n', '').strip()
            key5 = hs_code_desc_select[3].select('div.odd')[1].text.replace('\n', '').strip()
            key6 = hs_code_desc_select[4].select('div.odd')[0].text.replace('\n', '').strip()
            key7 = hs_code_desc_select[4].select('div.odd')[1].text.replace('\n', '').strip()
            key8 = hs_code_desc_select[4].select('div.odd')[2].text.replace('\n', '').strip()
            key9 = hs_code_desc_select[5].select('div.odd')[0].text.replace('\n', '').strip()
            key10 = hs_code_desc_select[5].select('div.odd')[1].text.replace('\n', '').strip()
            key11 = hs_code_desc_select[5].select('div.odd')[2].text.replace('\n', '').strip()
            key12 = hs_code_desc_select[6].select('div.odd')[0].text.replace('\n', '').strip()
            key13 = hs_code_desc_select[6].select('div.odd')[1].text.replace('\n', '').strip()
            key14 = hs_code_desc_select[7].select('div.odd')[0].text.replace('\n', '').strip()
            key15 = hs_code_desc_select[8].select('div.odd')[0].text.replace('\n', '').strip()
            key16 = hs_code_desc_select[9].select('div.odd')[0].text.replace('\n', '').strip()

            value1 = hs_code_desc_select[0].select('div.even > b')[0].text.replace('\n', '').strip()
            value2 = hs_code_desc_select[1].select('div.even')[0].text.replace('\n', '').strip()
            value3 = hs_code_desc_select[2].select('div.even')[0].text.replace('\n', '').strip()
            value4 = hs_code_desc_select[3].select('div.even1')[0].text.replace('\n', '').strip()
            value5 = hs_code_desc_select[3].select('div.even1')[1].text.replace('\n', '').strip()
            value6 = hs_code_desc_select[4].select('div.even1')[0].text.replace('\n', '').strip()
            value7 = hs_code_desc_select[4].select('div.even1')[1].text.replace('\n', '').strip()
            value8 = hs_code_desc_select[4].select('div.even1')[2].text.replace('\n', '').strip()
            value9 = hs_code_desc_select[5].select('div.even1')[0].text.replace('\n', '').strip()
            value10 = hs_code_desc_select[5].select('div.even1')[1].text.replace('\n', '').strip()
            value11 = hs_code_desc_select[5].select('div.even1')[2].text.replace('\n', '').strip()
            value12 = hs_code_desc_select[6].select('div.even1')[0].text.replace('\n', '').strip()
            value13 = hs_code_desc_select[6].select('div.even1')[1].text.replace('\n', '').strip()
            value14 = hs_code_desc_select[7].select('div.even')[0].text.replace('\n', '').strip()
            value15 = hs_code_desc_select[8].select('div.even')[0].text.replace('\n', '').strip()
            value16 = hs_code_desc_select[9].select('div.even')[0].text.replace('\n', '').strip()

            print(key1,": ",value1)
            print(key2,": ",value2)
            print(key3,": ",value3)
            print(key4,": ",value4)
            print(key5,": ",value5)
            print(key6,": ",value6)
            print(key7,": ",value7)
            print(key8,": ",value8)
            print(key9,": ",value9)
            print(key10,": ",value10)
            print(key11,": ",value11)
            print(key12,": ",value12)
            print(key13,": ",value13)
            print(key14,": ",value14)
            print(key15,": ",value15)
            print(key16,": ",value16)

            print()

            # 海关监管条件key & HS法定检验检疫key
            jgtjup_select = soup.select('div#jgtjup > span')
            key17 = jgtjup_select[0].text.replace('\n', '')
            key18 = jgtjup_select[1].text.replace('\n', '')

            print(key17,key18)

            # 海关监管条件
            jg_haiguan_select = soup.select('div.jgleft')
            print(jg_haiguan_select)
            # jg_haiguan_select[0].select('td')[0].text.strip()


            # HS法定检验检疫
            jg_jianyan_select = soup.select('div.jgright')
            # print(jg_jianyan_select)




            # count += 1
            # if count == 500:
            # break
        break


if __name__ == "__main__":
    # get_hs_code_catalog()
    # parse_catalog()
    # download_chapter_two_href_html()
    # parse_all_chapter_two_html()
    # download_all_hs_code_item_html()
    parse_all_hs_code_desc_html()
