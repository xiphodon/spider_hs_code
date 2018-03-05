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
import pymongo
import hashlib
import time

images_path = r'E:\work_all\topease\fastfishdata\news_images'

home_url = r'http://www.cifnews.com'
# 出口电商
url_1 = home_url + r'/tag/82'
# 进口电商
url_2 = home_url + r'/tag/366'
# b2b电商
url_3 = home_url + r'/tag/394'
# 市场观察
url_4 = home_url + r'/tag/393'

page_size = -1

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


def get_news_list_content_one_page(url):
    """
    获取某页的新闻列表内容
    :param url: 地址
    :return:
    """
    result = while_requests_get(url)
    return result.text


def get_news_detail_page(url):
    """
    获取资讯详情页
    :param url: 地址
    :return:
    """
    result = while_requests_get(url)

    selector = etree.HTML(result.text)

    update_time = selector.xpath('//div[@class="leftcont"]/div[1]/span[2]/text()')[0]
    news_content_list = list(map(lambda x: x.xpath('string()'),
                                 selector.xpath('//div[@class="leftcont"]/div[@class="fetch-read fetch-view"]/p')))
    news_content = ''.join(news_content_list).replace('\r', '').replace('\n', '').replace('\t', '')

    return update_time, news_content


def parse_news_list_page(page_content, this_page_url_main):
    """
    解析新闻列表页
    :param page_content: 页面内容
    :param this_page_url_main: 页面源自url
    :return:
    """
    selector = etree.HTML(page_content)

    news_selector = selector.xpath('//div[@class="left_cont"]')[0]

    news_image_src_list = news_selector.xpath('.//dl/dt[@class="dimg"]/a/img/@src')
    news_detail_href_list = news_selector.xpath('.//dl/dd/a[@target="_blank"]/@href')
    news_title_text_list = news_selector.xpath('.//dl/dd/a[@target="_blank"]/h2/text()')
    news_desc_list = news_selector.xpath('.//dl/dd[@class="dcont"]/div[@class="cont"]/a[@target="_blank"]/text()')
    news_from_list = news_selector.xpath('.//dl/dd[@class="dcont"]/div[@class="tag-list"]')

    # print(len(news_image_src_list))
    # print(len(news_detail_href_list))
    # print(len(news_title_text_list))
    # print(len(news_desc_list))
    # print(len(news_from_list))

    for news_image_src, news_detail_href, news_title_text, news_desc, news_from in \
            zip(news_image_src_list, news_detail_href_list, news_title_text_list, news_desc_list, news_from_list):

        news_detail_href = home_url + news_detail_href
        news_from_item_list = news_from.xpath('.//a/text()')

        news_image_src = news_image_src[:str(news_image_src).rfind('!')]

        # 下载图片
        news_image_md5_name = hashlib.md5(news_image_src.encode('utf8')).hexdigest() + str(int(time.time())) + '.jpg'
        news_image_path = os.path.join(images_path, news_image_md5_name)

        ir = requests.get(news_image_src)
        if ir.status_code == 200:
            with open(news_image_path, 'wb') as fp:
                fp.write(ir.content)

        # 下载详情页
        update_time, news_content = get_news_detail_page(news_detail_href)

        news_item = {
            'news_image_src': news_image_src,
            'news_detail_href': news_detail_href,
            'news_title': news_title_text,
            'news_desc': news_desc,
            'news_from': news_from_item_list,
            'mark': this_page_url_main,
            'news_image_path': news_image_path,
            'update_time': update_time,
            'news_content': news_content
        }

        collection = get_mongodb_collection()

        result = collection.insert_one(news_item)
        # print(result.inserted_id)

        # print(news_item)
        # break


def set_page_size(url):
    """
    获取并设置总页数
    :return:
    """
    result = while_requests_get(url)
    selector = etree.HTML(result.text)
    # 获取资讯页数
    global page_size
    if page_size == -1:
        page_size = int(selector.xpath('//div[@class="page"]/a/text()')[-2])
        # print(page_size)


def start_url_1_news(url):
    """
    多进程爬取
    :param url:
    :return:
    """
    try:
        content = get_news_list_content_one_page(url)
        parse_news_list_page(content, url_1)
        print(url)
    except Exception as e:
        print(e)


def get_url_1_news():
    """
    获取出口电商资讯
    :return:
    """
    set_page_size(url_1)

    temp_list = list()
    temp_list.append(url_1)
    url_list_main = temp_list * page_size

    url_list = list(map(lambda x: x[0] + '?page=' + str(x[1]), zip(url_list_main, range(1, page_size+1))))

    multiprocessing_download_files(start_url_1_news, url_list)


def start_url_2_news(url):
    """
    多进程爬取
    :param url:
    :return:
    """
    try:
        content = get_news_list_content_one_page(url)
        parse_news_list_page(content, url_2)
        print(url)
    except Exception as e:
        print(e)


def get_url_2_news():
    """
    获取进口电商资讯
    :return:
    """
    set_page_size(url_2)

    temp_list = list()
    temp_list.append(url_2)
    url_list_main = temp_list * page_size

    url_list = list(map(lambda x: x[0] + '?page=' + str(x[1]), zip(url_list_main, range(1, page_size+1))))

    multiprocessing_download_files(start_url_2_news, url_list)


def start_url_3_news(url):
    """
    多进程爬取
    :param url:
    :return:
    """
    try:
        content = get_news_list_content_one_page(url)
        parse_news_list_page(content, url_3)
        print(url)
    except Exception as e:
        print(e)


def get_url_3_news():
    """
    获取b2b电商资讯
    :return:
    """
    set_page_size(url_3)

    temp_list = list()
    temp_list.append(url_3)
    url_list_main = temp_list * page_size

    url_list = list(map(lambda x: x[0] + '?page=' + str(x[1]), zip(url_list_main, range(1, page_size+1))))

    multiprocessing_download_files(start_url_3_news, url_list)


def start_url_4_news(url):
    """
    多进程爬取
    :param url:
    :return:
    """
    try:
        content = get_news_list_content_one_page(url)
        parse_news_list_page(content, url_4)
        print(url)
    except Exception as e:
        print(e)


def get_url_4_news():
    """
    获取市场观察资讯
    :return:
    """
    set_page_size(url_4)

    temp_list = list()
    temp_list.append(url_4)
    url_list_main = temp_list * page_size

    url_list = list(map(lambda x: x[0] + '?page=' + str(x[1]), zip(url_list_main, range(1, page_size+1))))

    multiprocessing_download_files(start_url_4_news, url_list)


def multiprocessing_download_files(download_file_func, url_list, pool_num=20):
    """
    多进程下载页面
    :return:
    """
    print(len(url_list))

    pool = Pool(pool_num)
    pool.map(download_file_func, url_list)
    pool.close()
    pool.join()


def get_mongodb_collection():
    """
    获取MongoDB
    :return:
    """
    client = pymongo.MongoClient(host='localhost', port=27017)
    db = client.fastfishdata
    collection = db.news
    return collection


if __name__ == '__main__':
    # get_url_1_news()
    # get_url_2_news()
    # get_url_3_news()
    get_url_4_news()
