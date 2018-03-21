#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/3/20 14:01
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : gevent_demo.py
# @Software: PyCharm

from lxml import etree
import requests
from gevent import pool, monkey
monkey.patch_all()


headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:49.0) Gecko/20100101 Firefox/49.0',
    'Connection': 'keep-alive'
}


def while_requests_get(page_url, times=100):
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
            if while_times < times:
                while_times += 1
                print('**********', '尝试重新链接', while_times, '次:', page_url)
                continue
            else:
                raise e
        else:
            return result


def parse_head_page(content):
    """
    解析首页
    :param content:
    :return:
    """
    selector = etree.HTML(content)

    urls = selector.xpath('//div/h3/a/@href')

    return urls


def requests_urls(urls):
    """
    多协程请求
    :param urls:
    :return:
    """
    gevent_pool = pool.Pool(50)
    result_list = gevent_pool.map(while_requests_get, urls)
    return result_list


def start():
    """
    开始
    :return:
    """
    print('request head page ...')
    result = while_requests_get(r'https://www.baidu.com/baidu?wd=%E7%AB%AF%E8%84%91')

    print('get head page, parsing ...')
    urls = parse_head_page(result.text)

    print('head page parse OK, requests urls ...')
    result_list = requests_urls(urls)

    print('requests urls OK')
    print(result_list)


if __name__ == "__main__":
    start()
