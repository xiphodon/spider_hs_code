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

home_url = settings.company_home_url_russia

search_text_list = ['Магазины - обувь']

search_text = search_text_list[-1]

sess = requests.session()


def while_session_get(page_url, times=5000, sleep_time=0.2, new_sess=False, method_get=True, data=None):
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
            'Host': 'www.yp.ru',
            # 'Origin': 'https://www.yp.ru',
            'Referer': 'https://www.yp.ru/'
            }


def request_what():
    """
    what请求
    :return:
    """
    data = {
        'text': 'Магазины - обувь',
        'region': 'spb'
    }
    sess.post(f'{home_url}/autocomplete/what/', headers=get_header(), data=data, timeout=5)


def get_keyword_list():
    """
    获取该关键词列表
    :return:
    """
    next_page_href = '/list/magaziny_obuv/'

    has_next_page = True

    while has_next_page:
        current_page_number_str = get_current_page_number(next_page_href)
        print(f'current page number: {current_page_number_str}')

        result = sess.get(f'{home_url}{next_page_href}', headers=get_header(), timeout=5)
        time.sleep(random.randint(1, 5) * 0.1)

        selector = etree.HTML(result.text)

        # 获取总数据数量
        if current_page_number_str == '1':
            total_data_size_text_list = selector.xpath('//div[@id="search_results"]/div/p[@class="light_grey"]/text()')
            # print(total_data_size_text_list)
            if len(total_data_size_text_list) > 0:
                total_data_size_text = total_data_size_text_list[0]
                # print(total_data_size_text)
                re_result = re.search(r'\d+', total_data_size_text).group()
                print(f'total data size: {re_result}')

        # 获取下一页href
        next_page_href_list = selector.xpath('//li[@class="ui_next_page"]/a/@href')
        # print(next_page_href_list)
        if len(next_page_href_list) > 0:
            next_page_href = next_page_href_list[0]
            # has_next_page = True
            has_next_page = False  # 临时中断
        else:
            has_next_page = False

        # 获取数据列表
        data_div_list = selector.xpath('//div[@id="search_items_wrapper"]/div[@reccode]')
        for data_div in data_div_list:
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

                print(company_name)
                print(company_url)
                print(company_address)
                print('===================')


def get_current_page_number(next_page_href: str):
    """
    获取当前页的页数
    :return:
    """
    if next_page_href.find('page') > 0:
        return next_page_href.split('/')[-2]
    else:
        return '1'


def start():
    """
    执行入口
    :return:
    """
    request_what()
    get_keyword_list()


if __name__ == '__main__':
    start()
