#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/3/5 11:13
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : yuguo_spider_8.py
# @Software: PyCharm

# yuguowang spider

from lxml import etree
import json
import os
import requests
import random
from multiprocessing import Pool


home_url = 'http://www.cifnews.com/'
# 出口电商
url_1 = home_url + 'tag/82'
# 进口电商
url_2 = home_url + 'tag/366'
# b2b电商
url_3 = home_url + 'tag/394'
# 市场观察
url_4 = home_url + 'tag/393'


headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:49.0) Gecko/20100101 Firefox/49.0',
    'Connection': 'keep-alive'
}


def while_requests_get(page_url):
    """
    循环请求
    :return:
    """
    while_times = 0
    while True:
        try:
            result = requests.get(page_url, headers=headers, timeout=5)
            # result = requests.get(page_url, headers=headers, proxies=proxies, timeout=5)
        except Exception as e:
            if while_times < 100:
                while_times += 1
                print('**********', '尝试重新链接', while_times, '次:', page_url)
                continue
            else:
                raise e
        else:
            return result


def get_news_list_content_one_page(url, page=1):
    """
    获取某页的新闻列表内容
    :param url: 地址
    :param page: 页数
    :return:
    """
    this_page_url = url + '/?page=' + str(page)
    result = while_requests_get(this_page_url)
    return result.text


def parse_news_list_page(page_content):
    """
    解析新闻列表页
    :param page_content: 页面内容
    :return:
    """
    selector = etree.HTML(page_content)
    print(selector)


if __name__ == '__main__':
    get_news_list_content_one_page(url_1, 1)


