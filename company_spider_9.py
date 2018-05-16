#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/5/15 14:43
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : company_spider_9.py
# @Software: PyCharm

import requests
from gevent import pool, monkey
monkey.patch_all()
import os
from lxml import etree
import json

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:49.0) Gecko/20100101 Firefox/49.0',
    'Connection': 'keep-alive'
}

search_text = 'pump'
search_type_dict = {'product': 'PRODUCT',
                    'supplier': 'SUPPLIER'}
search_type = search_type_dict['supplier']

home_url = r'https://us.kompass.com'
search_url = r'https://us.kompass.com/searchCompanies?' \
             r'acClassif=&localizationCode=&localizationLabel=&localizationType=&text=' + search_text + \
             r'&searchType=' + search_type

home_path = r'E:\work_all\topease\company_spider_9'

company_list_pages_dir_path = os.path.join(home_path, 'company_list_dir')
company_list_pages_product_dir_path = os.path.join(company_list_pages_dir_path, search_text)

company_detail_dir_path = os.path.join(home_path, 'company_detail_dir')
company_detail_product_dir_path = os.path.join(company_detail_dir_path, search_text)

first_html_path = os.path.join(company_list_pages_product_dir_path, search_text + r'_' + search_type + r'_0.html')


page_total = -1
overwrite = False

sess = requests.session()

if not os.path.exists(company_list_pages_product_dir_path):
    os.mkdir(company_list_pages_product_dir_path)

if not os.path.exists(company_detail_product_dir_path):
    os.mkdir(company_detail_product_dir_path)


def while_session_get(page_url, times=5000):
    """
    循环请求
    :return:
    """
    while_times = 0
    while True:
        try:
            result = sess.get(page_url, headers=headers, timeout=30)
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


def download_frist_html():
    """
    下载首次请求的html页面
    :return:
    """
    result = while_session_get(search_url)

    with open(first_html_path, 'w', encoding='utf8') as fp:
        fp.write(result.text)

    print('first html download ---- OK')


def parse_first_html():
    """
    解析首次请求到的页面
    :return:
    """
    with open(first_html_path, 'r', encoding='utf8') as fp:
        content = fp.read()

    selector = etree.HTML(content)

    total_page_li = selector.xpath('//ul[@class="pagination paginatorDivId"]/li')

    global page_total

    if len(total_page_li) >= 2:
        page_total = int(total_page_li[-2].xpath('./a/text()')[0])
    else:
        page_total = 1

    print(page_total)


def download_company_list_pages():
    """
    获取该商品的所有公司列表(废弃)
    :return:
    """
    url_page_postfix = r'/searchCompanies/scroll?tab=cmp&pageNbre='

    for i in range(2, page_total):
        url_item = home_url + url_page_postfix + str(i)
        this_page_html_path = os.path.join(company_list_pages_product_dir_path, search_text + r'_' + search_type + r'_' + str(i) + r'.html')

        if (not overwrite) and os.path.exists(this_page_html_path):
            print('page:' + str(i) + '-------- exist')
            continue

        result = while_session_get(url_item)

        with open(this_page_html_path, 'w', encoding='utf8') as fp:
            fp.write(result.text)

        print('page:' + str(i) + '-------- download OK')


def download_this_page_company_list(url):
    """
    下载当前页公司列表
    :return:
    """
    this_page_str = url[str(url).rfind('=') + 1:]
    this_page_html_path = os.path.join(company_list_pages_product_dir_path, search_text + r'_' + search_type + r'_' +
                                       this_page_str + r'.html')

    if os.path.exists(this_page_html_path):
        print('page:' + this_page_str + '-------- exist')
    else:
        result = while_session_get(url)

        with open(this_page_html_path, 'w', encoding='utf8') as fp:
            fp.write(result.text)

        print('page:' + this_page_str + '-------- download OK')


def gevent_pool_requests():
    """
    多协程请求
    :return:
    """
    url_page_postfix = r'/searchCompanies/scroll?tab=cmp&pageNbre='
    urls = [home_url + url_page_postfix + str(i) for i in range(1, page_total + 1)]

    gevent_pool = pool.Pool(200)
    result_list = gevent_pool.map(download_this_page_company_list, urls)
    return result_list


def start():
    """
    开始采集
    :return:
    """
    download_frist_html()
    parse_first_html()

    # download_company_list_pages()
    gevent_pool_requests()


if __name__ == '__main__':
    start()
