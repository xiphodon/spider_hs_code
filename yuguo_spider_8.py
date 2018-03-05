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


def get_news_list_content_one_page(url, page=1):
    """
    获取某页的新闻列表内容
    :param url: 地址
    :param page: 页数
    :return:
    """
    this_page_url = url + '/?page=' + str(page)
    result = while_requests_get(this_page_url)
    print(this_page_url, page_size)
    return result.text


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

        news_from_item_list = news_from.xpath('.//a/text()')

        news_image_md5_name = hashlib.md5(news_image_src.encode('utf8')).hexdigest() + str(time.time()) + '.jpg'
        news_image_path = os.path.join(images_path, news_image_md5_name)

        ir = requests.get(news_image_src)
        if ir.status_code == 200:
            with open(news_image_path, 'wb') as fp:
                fp.write(ir.content)

        news_item = {
            'news_image_src': news_image_src,
            'news_detail_href': home_url + news_detail_href,
            'news_title': news_title_text,
            'news_desc': news_desc,
            'news_from': news_from_item_list,
            'mark': this_page_url_main,
            'news_image_path': news_image_path
        }

        collection = get_mongodb_collection()

        result = collection.insert_one(news_item)
        # print(result.inserted_id)

        # print(news_item)
        # break

    # 获取资讯页数
    global page_size
    if page_size == -1:
        page_size = int(selector.xpath('//div[@class="page"]/a/text()')[-2])
        # print(page_size)


def get_url_1_news():
    """
    获取出口电商资讯
    :return:
    """
    page_no = 1
    while True:
        content = get_news_list_content_one_page(url_1, page_no)
        parse_news_list_page(content, url_1)
        page_no += 1
        if page_no > page_size:
            break
        else:
            break


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
    get_url_1_news()


