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
# from multiprocessing.dummy import Pool, Lock
import pymongo
import hashlib
import time
import pymssql
import settings

json_base_path = r'E:\work_all\topease\fastfishdata'

images_path = r'E:\work_all\topease\fastfishdata\news_images'
inset_images_path = r'E:\work_all\topease\fastfishdata\news_inset_images'

home_url = r'http://www.cifnews.com'
# 出口电商
url_1 = home_url + r'/tag/82'
# 进口电商
url_2 = home_url + r'/tag/366'
# b2b电商
url_3 = home_url + r'/tag/394'
# 市场观察
url_4 = home_url + r'/tag/393'

desc_dict = {
    '82': '出口电商',
    '366': '进口电商',
    '394': 'B2B电商',
    '393': '市场观察'
}

page_size = -1

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:49.0) Gecko/20100101 Firefox/49.0',
    'Connection': 'keep-alive'
}

# 忽略下载的图片地址
ignore_img_src_list = ['http://pic.cifnews.com/upload/201704/13/201704131357564290.png',
                       'http://static.cifnews.com/yuguo/image/appdownload.png?v=1.0']
#
# 已下载过的图片地址
# img_src_download_dict = dict()
# 已下载过的资讯详情页面
# news_detail_page_href_set = set()
# # 互斥锁
# mutex = Lock()


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


def get_news_list_content_one_page(url):
    """
    获取某页的新闻列表内容
    :param url: 地址
    :return:
    """
    result = while_requests_get(url)
    return result.text


def get_mongodb_collection():
    """
    获取MongoDB
    :return:
    """
    client = pymongo.MongoClient(host='localhost', port=27017)
    db = client.fastfishdata
    collection = db.news
    return collection


def copy_data_from_mongodb_to_sqlserver():
    """
    从mongodb拷贝数据到sqlserver
    :return:
    """
    # sqlserver
    conn = pymssql.connect(host=settings.host, user=settings.user, password=settings.password,
                           database=settings.database, charset=settings.charset)
    cur = conn.cursor()
    if not cur:
        raise (NameError, "数据库连接失败")

    # mongodb
    collection = get_mongodb_collection()
    res = collection.find({})

    for item in res:
        # print(item)
        news_image_src = item['news_image_src'].replace("'", "''")
        news_detail_href = item['news_detail_href'].replace("'", "''")
        news_title = item['news_title'].replace("'", "''")
        news_desc = check_str(item['news_desc']).replace("'", "''")
        news_from = ','.join(item['news_from']).replace("'", "''")
        mark = item['mark']
        news_image_path = item['news_image_path'].replace("'", "''")
        update_time = item['update_time'].replace("'", "''")
        news_content = check_str(item['news_content']).replace("'", "''")

        mark = mark[str(mark).rfind(r'/') + 1:].replace("'", "''")
        mark_desc = desc_dict[mark]

        sql_str = "insert into news(news_image_src,news_detail_href,news_title,news_desc,news_from," \
                  "mark,news_image_path,update_time,news_content,mark_desc,click)" \
                  " values('%s','%s',N'%s',N'%s','%s','%s',N'%s',N'%s',N'%s',N'%s',N'%d')" \
                  % (news_image_src, news_detail_href, news_title, news_desc, news_from, mark,
                     news_image_path, update_time, news_content, mark_desc, 0)

        print(sql_str)
        print(len(news_desc), len(news_title))
        cur.execute(sql_str.encode('utf8'))
        conn.commit()

        # break


def check_str(_str):
    """
    检查字符串 \n, \xa0, \xc2, \u3000 等特殊字符
    :param _str:
    :return:
    """
    problem_str_list = ['\r\n', '\n', '\xa0', '\xc2', '\u3000', '<br />', '&nbsp;']
    for item_problem in problem_str_list:
        _str = _str.replace(item_problem, ' ')

    strip_str_list = [',', ' ']
    for item_strip in strip_str_list:
        _str = _str.strip(item_strip)
    return _str


def get_news_detail_page(url):
    """
    获取资讯详情页
    :param url: 地址
    :return:
    """
    result = while_requests_get(url)

    selector = etree.HTML(result.text)

    update_time = selector.xpath('//div[@class="leftcont"]/div[1]/span[2]/text()')[0]

    selector_p = selector.xpath('//div[@class="leftcont"]/div[@class="fetch-read fetch-view"]/p')

    news_content_list = list()

    for item_p in selector_p:
        img_src_list = item_p.xpath('.//img/@src')
        # print(img_src_list)

        item_str = ''

        if len(img_src_list) > 0:
            # 有插图
            for img_src in img_src_list:

                if img_src not in ignore_img_src_list:
                    # 下载插图
                    inset_image_path = download_img(img_src, inset_images_path)

                    item_str += "<img class='news_insert_img' src='%s'>" % (inset_image_path,)
                    # print(img_src, inset_image_path)

        else:
            # 无插图直接取文本
            item_str = item_p.xpath('string()')

        item_str = item_str.replace('\r', '').replace('\n', '').replace('\t', '')

        if len(item_str) != 0:
            item_str = r"<p>" + item_str + r"</p>"
            news_content_list.append(item_str)

    news_content = ''.join(news_content_list)

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

    collection = get_mongodb_collection()

    for news_image_src, news_detail_href, news_title_text, news_desc, news_from in \
            zip(news_image_src_list, news_detail_href_list, news_title_text_list, news_desc_list, news_from_list):

        news_image_src = news_image_src[:str(news_image_src).rfind('!')]
        # 下载图片
        news_image_path = download_img(news_image_src, images_path)

        if not str(news_detail_href).startswith('http'):
            news_detail_href = home_url + news_detail_href

        # if news_detail_href not in news_detail_page_href_set:
            # 下载详情页
            # mutex.acquire()
            # news_detail_page_href_set.add(news_detail_href)
            # mutex.release()
        update_time, news_content = get_news_detail_page(news_detail_href)
        # else:
        #     continue

        news_from_item_list = news_from.xpath('.//a/text()')

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

        result = collection.insert_one(news_item)
        print(result.inserted_id)

        print(news_item)
        #
        # print(img_src_download_dict)
        # print(news_detail_page_href_set)
        # break


def download_img(img_src, images_dir_path):
    """
    下载图片
    :return: 图片路径
    """
    # if img_src not in img_src_download_dict:

    temp_img_src = str(img_src).split(r"?")[0]
    img_format_index = str(temp_img_src).rfind(r".")
    img_format = img_src[img_format_index:]

    if len(img_format) <= 2:
        img_format = '.jpg'

    news_image_md5_name = hashlib.md5(img_src.encode('utf8')).hexdigest() + str(
        int(time.time())) + img_format
    news_image_path = os.path.join(images_dir_path, news_image_md5_name)

    ir = requests.get(img_src)
    if ir.status_code == 200:
        with open(news_image_path, 'wb') as fp:
            fp.write(ir.content)

    # mutex.acquire()
    # img_src_download_dict[img_src] = news_image_path
    # mutex.release()

    return news_image_path
    #
    # else:
    #     return img_src_download_dict[img_src]


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
        # page_size = 2
        # print(page_size)


def save_temp_download_json(file_dir):
    """
    缓存下载数据
    :param file_dir:
    :return:
    """
    # json_dir_path = os.path.join(json_base_path, file_dir)
    #
    # if not os.path.exists(json_dir_path):
    #     os.mkdir(json_dir_path)
    #
    # img_src_download_dict_json_path = os.path.join(json_dir_path, 'img_src_download_dict.json')
    # with open(img_src_download_dict_json_path, 'w', encoding='utf8') as fp:
    #     fp.write(json.dumps(img_src_download_dict))
    #
    # print(len(img_src_download_dict), img_src_download_dict)
    #
    # news_detail_page_href_set_json_path = os.path.join(json_dir_path, 'news_detail_page_href_set.json')
    # with open(news_detail_page_href_set_json_path, 'w', encoding='utf8') as fp:
    #     fp.write(json.dumps(list(news_detail_page_href_set)))
    #
    # print(len(news_detail_page_href_set), news_detail_page_href_set)


def read_temp_download_json(file_dir):
    """
    读取缓存数据
    :param file_dir:
    :return:
    """
    json_dir_path = os.path.join(json_base_path, file_dir)

    if os.path.exists(json_dir_path):

        img_src_download_dict_json_path = os.path.join(json_dir_path, 'img_src_download_dict.json')

        if os.path.exists(img_src_download_dict_json_path):
            with open(img_src_download_dict_json_path, 'r', encoding='utf8') as fp:
                # global img_src_download_dict
                img_src_download_dict = json.loads(fp.read())

            print(len(img_src_download_dict), img_src_download_dict)

        news_detail_page_href_set_json_path = os.path.join(json_dir_path, 'news_detail_page_href_set.json')

        if os.path.exists(news_detail_page_href_set_json_path):
            with open(news_detail_page_href_set_json_path, 'r', encoding='utf8') as fp:
                # global news_detail_page_href_set
                news_detail_page_href_set = set(json.loads(fp.read()))

            print(len(news_detail_page_href_set), news_detail_page_href_set)


def read_save_temp_data(file_dir):
    """
    读存缓存数据
    :param file_dir: 读存路径
    :return:
    """
    def inner_run(_func):
        read_temp_download_json(file_dir)
        _func()
        save_temp_download_json(file_dir)

    return inner_run


def multiprocessing_download_files(download_file_func, url_list, pool_num=50):
    """
    多进程下载页面
    :return:
    """
    print(len(url_list))

    pool = Pool(pool_num)
    pool.map(download_file_func, url_list)
    pool.close()
    pool.join()


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


# @read_save_temp_data('url_1')
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


if __name__ == '__main__':
    # read_save_temp_data('url_1')(get_url_1_news)
    # get_url_1_news()
    # get_url_2_news()
    # get_url_3_news()
    # get_url_4_news()
    copy_data_from_mongodb_to_sqlserver()
