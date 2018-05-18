#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/5/15 14:43
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : company_spider_9.py
# @Software: PyCharm

from gevent import pool, monkey
monkey.patch_all()
import requests
import os
from lxml import etree
import json
import time

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:49.0) Gecko/20100101 Firefox/49.0',
    'Connection': 'keep-alive'
}

search_text_list = ['pump', 'fabric', 'glass']
search_text = 'fabric'

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


pool_size = 1500
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
        try:
            page_total = int(total_page_li[-1].xpath('./a/text()')[0])
        except Exception as e:
            page_total = int(total_page_li[-2].xpath('./a/text()')[0])
            print(e)
    else:
        page_total = 1

    print(page_total)


# def download_company_list_pages():
#     """
#     获取该商品的所有公司列表(废弃)
#     :return:
#     """
#     url_page_postfix = r'/searchCompanies/scroll?tab=cmp&pageNbre='
#
#     for i in range(2, page_total):
#         url_item = home_url + url_page_postfix + str(i)
#         this_page_html_path = os.path.join(company_list_pages_product_dir_path, search_text + r'_' + search_type +
#                                            r'_' + str(i) + r'.html')
#
#         if (not overwrite) and os.path.exists(this_page_html_path):
#             print('page:' + str(i) + '-------- exist')
#             continue
#
#         result = while_session_get(url_item)
#
#         with open(this_page_html_path, 'w', encoding='utf8') as fp:
#             fp.write(result.text)
#
#         if os.path.getsize(this_page_html_path) < 125 * 1024:
#             print('page:' + str(i) + '-------- this page html size < 125k')
#         else:
#             print('page:' + str(i) + '-------- download OK')


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

        if os.path.getsize(this_page_html_path) < 125 * 1024:
            print('page:' + this_page_str + '-------- this page html size < 125k')
        else:
            print('page:' + this_page_str + '-------- download OK')


def gevent_pool_requests(func, urls):
    """
    多协程请求
    :param func:
    :param urls:
    :return:
    """
    gevent_pool = pool.Pool(pool_size)
    result_list = gevent_pool.map(func, urls)
    return result_list


def download_all_company_list_htmls(while_times=3):
    """
    下载所有的公司列表页
    :return:
    """
    url_page_postfix = r'/searchCompanies/scroll?tab=cmp&pageNbre='
    urls = [home_url + url_page_postfix + str(i) for i in range(1, page_total + 1)]

    for i in range(while_times):
        print('第' + str(i + 1) + '轮爬取')
        gevent_pool_requests(download_this_page_company_list, urls)


def download_this_page_company_detail(url):
    """
    下载当前页公司详情页
    :param url:
    :return:
    """
    try:
        # print(url)
        url_split_list = str(url).split(r'/')
        company_id_str = url_split_list[-2]
        company_name_str = url_split_list[-3]
        company_detail_name = company_id_str + r'_' + company_name_str + r'_c' + r'.html'
        company_detail_path = os.path.join(company_detail_product_dir_path, company_detail_name)

        min_file_size = 106 * 1024
        if os.path.exists(company_detail_path) and os.path.getsize(company_detail_path) > min_file_size:
            print('page:' + company_detail_name + '-------- exists')
        else:
            result = while_session_get(url)

            with open(company_detail_path, 'w', encoding='utf8') as fp:
                fp.write(result.text)

            if os.path.getsize(company_detail_path) < min_file_size:
                print('page:' + company_detail_name + '-------- this page html size < ' + str(min_file_size/1024)
                      + 'k  !!!!!!!!!!')
            else:
                print('page:' + company_detail_name + '-------- download OK')
    except Exception as e:
        print(e)


def download_all_company_detail_htmls(while_times=3):
    """
    下载所有的公司详情页
    while_times：采集轮次数
    :return:
    """
    count = 0
    pages = 0
    all_company_detail_urls_list = list()
    for item_file_name in os.listdir(company_list_pages_product_dir_path):
        # 单个公司列表页面

        file_path = os.path.join(company_list_pages_product_dir_path, item_file_name)

        if file_path == first_html_path:
            print('跳过页码页：', file_path)
            continue

        company_detail_urls = get_company_detail_urls_by_company_list_page(file_path)

        pages += 1
        count += len(company_detail_urls)

        all_company_detail_urls_list.extend(company_detail_urls)
        print('\r正在收集公司详情页url,pages:' + str(pages) + ',count:' + str(count), end='')

    print('\n', pages, count)

    for i in range(while_times):
        print('第' + str(i + 1) + '轮爬取')
        gevent_pool_requests(download_this_page_company_detail, all_company_detail_urls_list)


def get_company_detail_urls_by_company_list_page(company_list_page_path):
    """
    通过公司列表页获取该页中所有跳转公司详情的url
    :param company_list_page_path:
    :return:
    """
    with open(company_list_page_path, 'r', encoding='utf8') as fp:
        content = fp.read()

    selector = etree.HTML(content)

    this_page_company_detail_urls = selector.xpath('//div[@class="infos"]/div[@class="details"]/h2/a/@href')

    this_page_company_detail_urls = [home_url + str(i).replace('\n', '') for i in this_page_company_detail_urls
                                     if str(i).startswith(r'/c/')]

    return this_page_company_detail_urls


def start():
    """
    开始采集
    :return:
    """

    # 1.下载公司列表页
    # download_frist_html()
    # parse_first_html()
    # # download_company_list_pages() # 废弃
    # download_all_company_list_htmls()

    # 2.下载公司详情页
    download_all_company_detail_htmls()


if __name__ == '__main__':
    start()
