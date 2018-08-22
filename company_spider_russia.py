#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/8/20 10:49
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : company_spider_russia.py
# @Software: PyCharm

from gevent import pool, monkey; monkey.patch_all()
from gevent import sleep as gevent_sleep
import requests
import os
from lxml import etree
import settings
import json
import time
import traceback
import re
import random
import urllib
import pymongo
import xlwt

area_and_region_list = [('www', 'spb'), ('msk', 'msk')]

choice_area_url_and_region = area_and_region_list[-1]

home_url = settings.company_home_url_russia.replace('*', choice_area_url_and_region[0])

print(home_url)

search_text_list = ['Магазины - обувь']

search_text = search_text_list[-1]

sess = requests.session()


def while_session_request(page_url, times=5000, sleep_time=0.2, new_sess=False, method_get=True, data=None):
    """
    循环请求
    :return:
    """
    while_times = 0
    while True:
        try:
            if new_sess:
                global sess
                sess = requests.session()

            gevent_sleep(random.randint(5, 8) * sleep_time)
            if method_get:
                result = sess.get(page_url, headers=get_header(), data=data, timeout=5)
            else:
                result = sess.post(page_url, headers=get_header(), data=data, timeout=5)
            # result = requests.get(page_url, headers=headers, proxies=proxies, timeout=5)
        except Exception as e:
            if while_times < times:
                while_times += 1
                print('**********', '尝试重新链接', while_times, '次:', page_url.replace(home_url, ''))
                continue
            else:
                raise e
        else:
            return result


def get_header():
    """
    获取header
    :return:
    """
    return {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:6.0) Gecko/20100101 Firefox/6.0',
            'Connection': 'keep-alive',
            'Host': f'{choice_area_url_and_region[0]}.yp.ru',
            # 'Origin': f'https://{choice_area_url_and_region[0]}.yp.ru',
            'Referer': f'https://{choice_area_url_and_region[0]}.yp.ru/'
            }


def request_what():
    """
    what请求
    :return:
    """
    data = {
        'text': 'Магазины - обувь',
        'region': choice_area_url_and_region[-1]
    }
    # sess.post(f'{home_url}/autocomplete/what/', headers=get_header(), data=data, timeout=10)
    while_session_request(f'{home_url}/autocomplete/what/', data=data, sleep_time=0, method_get=False)


def get_keyword_list():
    """
    获取该关键词列表
    :return:
    """
    next_page_href = '/list/magaziny_obuv/'

    collection = get_mongodb_collection()

    has_next_page = True

    while has_next_page:
        current_page_number_str = get_current_page_number(next_page_href)
        print(f'current page number: {current_page_number_str}')

        # result = sess.get(f'{home_url}{next_page_href}', headers=get_header(), timeout=10)
        result = while_session_request(f'{home_url}{next_page_href}', sleep_time=0)
        time.sleep(random.randint(1, 3) * 0.1)

        selector = etree.HTML(result.text)

        # 获取总数据数量
        if current_page_number_str == '1':
            total_data_size_text_list = selector.xpath('//div[@id="search_results"]/div/p[@class="light_grey"]/text()')
            # print(total_data_size_text_list)
            if len(total_data_size_text_list) > 0:
                total_data_size_text = total_data_size_text_list[0]
                # print(total_data_size_text)
                re_result = re.search(r'\d+\s*\d*', total_data_size_text).group()
                re_result = re_result.replace(' ', '').strip()
                print(f'total data size: {re_result}')

        # 获取下一页href
        next_page_href_list = selector.xpath('//li[@class="ui_next_page"]/a/@href')
        # print(next_page_href_list)
        if len(next_page_href_list) > 0:
            next_page_href = next_page_href_list[0]
            has_next_page = True
            # has_next_page = False  # 临时中断
        else:
            has_next_page = False

        # 获取数据列表
        data_div_list = selector.xpath('//div[@id="search_items_wrapper"]/div[@reccode]')
        for data_div in data_div_list:

            # 数据id(reccode)
            company_id_list = data_div.xpath('./@reccode')
            company_id = str(company_id_list[0]).strip() if len(company_id_list) > 0 else '-1'
            print(company_id)

            db_result = collection.find_one({'company_id': company_id})
            if db_result is not None:
                continue

            data_info_div_list = data_div.xpath('./div[@class="text_info"]')
            if len(data_info_div_list) > 0:
                data_info_div = data_info_div_list[0]

                company_name_list = data_info_div.xpath('./p[contains(@class,"company_name")]/a')
                company_name = company_name_list[0].xpath('string()').strip() if len(company_name_list) > 0 else ''

                company_url_list = data_info_div.xpath('./p[contains(@class,"company_url")]/a/@href')
                company_url = company_url_list[0].strip() if len(company_url_list) > 0 else ''

                company_address_list = data_info_div.xpath('./p[contains(@class,"company_address")]/text()')
                company_address = ''.join(company_address_list).replace(r'\r\n', '').strip() \
                    if len(company_address_list) > 0 else ''

                company_phone_a_list = data_info_div.xpath('./a[contains(@class,"company_phone")]/@data-uiajaxdata')
                company_phone_id_text = company_phone_a_list[0] if len(company_phone_a_list) > 0 else ''
                if company_phone_id_text.startswith('contact_id='):
                    company_phone_id = company_phone_id_text.split('=')[-1]
                    company_phone = get_phone_by_phone_id(company_phone_id)
                else:
                    company_phone = ''

                temp_dict = {
                    'company_id': company_id,
                    'company_name': company_name,
                    'company_url': company_url,
                    'company_address': company_address,
                    'company_phone': company_phone,
                    'create_time': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
                    'create_tag': search_text
                }

                print(temp_dict)
                db_result = collection.insert_one(temp_dict)
                print(db_result.inserted_id)
                print('===================')


def get_phone_by_phone_id(company_phone_id):
    """
    通过phoneId获取phone number
    :param company_phone_id:
    :return:
    """
    data = {
        'contact_id': company_phone_id
    }
    # result = sess.post(f'{home_url}/ajax/GetPhoneByContactId', headers=get_header(), data=data, timeout=10)
    result = while_session_request(f'{home_url}/ajax/GetPhoneByContactId', sleep_time=0.03, data=data, method_get=False)
    # time.sleep(random.randint(1, 3) * 0.1)
    return result.text


def get_current_page_number(next_page_href: str):
    """
    获取当前页的页数
    :return:
    """
    if next_page_href.find('page') > 0:
        return next_page_href.split('/')[-2]
    else:
        return '1'


def get_mongodb_collection():
    """
    获取MongoDB
    :return:
    """
    client = pymongo.MongoClient(host='localhost', port=27017)
    db = client.fastfishdata
    collection = db.yp_russia
    return collection


def create_excel_file(new_excel_name, is_ignore_no_url=True):
    """
    生成excel文件
    :param new_excel_name
    :return:
    """
    wbk = xlwt.Workbook()
    sheet = wbk.add_sheet('sheet 1')

    collection = get_mongodb_collection()
    res = collection.find({})

    create_row(sheet, 0, ['company_name', 'company_url', 'company_address', 'company_phone', 'create_tag'])

    data_index = 1

    for i, res_item in enumerate(res):
        if is_ignore_no_url:
            if len(res_item['company_url'].strip()) == 0:
                continue
        create_row(sheet, data_index, [res_item['company_name'],
                                       res_item['company_url'],
                                       res_item['company_address'],
                                       res_item['company_phone'],
                                       res_item['create_tag']])
        data_index += 1

    wbk.save(rf'C:\Users\topeasecpb\Desktop\{new_excel_name}.xls')


def create_row(sheet, row_index, data_list):
    """
    excel文件创建一行数据
    :param sheet: sheet表
    :param data_list: 一行数据
    :param row_index: 行索引
    :return:
    """
    for i in range(len(data_list)):
        sheet.write(row_index, i, data_list[i])


def get_russia_company_data():
    """
    获取俄罗斯数据
    :return:
    """
    request_what()
    get_keyword_list()


def start():
    """
    执行入口
    :return:
    """
    get_russia_company_data()
    create_excel_file(f'russia_{choice_area_url_and_region[0]}_shoes_shop')


if __name__ == '__main__':
    start()
