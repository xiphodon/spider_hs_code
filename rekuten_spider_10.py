#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/7/26 10:35
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : rekuten_spider_10.py
# @Software: PyCharm

# 乐天 定向采集

from gevent import pool, monkey; monkey.patch_all()
import requests
import os
from lxml import etree
import json
import traceback
import time
import re


class RekutenSpiderSettings:
    """
    乐天爬虫配置
    """
    def __init__(self, key_word_list):
        if not isinstance(key_word_list, list):
            raise ValueError("must be list instance, eg:['リキッド', '電子タバコ', 'vape']")

        self._set_request_setting(key_word_list)
        self._set_data_setting()

    def _set_request_setting(self, key_word_list):
        """
        设置请求配置
        :return:
        """
        self.home_url = r'https://search.rakuten.co.jp/'
        self.home_url_www = r'https://www.rakuten.co.jp/'
        # self.key_word_list = ['リキッド', '電子タバコ', 'vape']
        self.key_word_list = key_word_list
        self.key_word_str = '+'.join(self.key_word_list)
        # https://search.rakuten.co.jp/search/mall/リキッド+電子タバコ+vape/?p=5
        self.request_url = f'{self.home_url}search/mall/{self.key_word_str}/?p=1'

    def _set_data_setting(self):
        """
        设置数据配置
        :return:
        """
        self.home_path = r'E:\work_all\topease\takuten_spider_key'
        self.key_dir_path = os.path.join(self.home_path, self.key_word_str)
        self.key_pages_html_dir_path = os.path.join(self.key_dir_path, 'pages_html_dir')
        self.key_shop_info_html_dir_path = os.path.join(self.key_dir_path, 'shop_info_html_dir')
        self.key_json_dir_path = os.path.join(self.key_dir_path, 'json_dir')

        self.make_dir(self.home_path)
        self.make_dir(self.key_dir_path)
        self.make_dir(self.key_pages_html_dir_path)
        self.make_dir(self.key_shop_info_html_dir_path)
        self.make_dir(self.key_json_dir_path)

    @staticmethod
    def make_dir(dir_path):
        """
        创建文件夹
        :param dir_path:
        :return:
        """
        if not os.path.exists(dir_path):
            os.mkdir(dir_path)


class WhileRequests:
    """
    循环请求
    """
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:49.0) Gecko/20100101 Firefox/49.0',
            'Connection': 'keep-alive'
        }

    def get(self, url, request_times=10000):
        """
        get请求
        :param url:
        :param request_times:
        :return:
        """
        while_times = 0
        while True:
            try:
                result = requests.get(url, headers=self.headers, timeout=10)
                # result = requests.get(url, headers=headers, proxies=proxies, timeout=5)
            except Exception as e:
                if while_times < request_times:
                    while_times += 1
                    print('**********', '尝试重新链接', while_times, '次:', url)
                    continue
                else:
                    raise e
            else:
                return result


class RekutenSpiderKey:
    """
    乐天爬虫（定向关键词爬取）
    """

    def __init__(self):
        key_word_list = ['リキッド', '電子タバコ', 'vape']
        self.settings = RekutenSpiderSettings(key_word_list)
        self.request = WhileRequests()
        self.html_page_number = -1
        self.shop_name_url_json_path = None

    def run(self):
        """
        启动爬虫
        :return:
        """
        self.check_and_download_page_htmls()
        self.gevent_parse_page_htmls()
        self.check_and_download_shop_info_htmls()

    def check_and_download_page_htmls(self):
        """
        检查并下载分页html页面
        :return:
        """
        if len(os.listdir(self.settings.key_pages_html_dir_path)) < 150:
            self.gevent_download_page_html()

    def download_html(self, url, save_path, encoding='utf8'):
        """
        下载html页面
        :return:
        """
        if not os.path.exists(save_path):
            result = self.request.get(url)

            html_content = result.content.decode(encoding)
            with open(save_path, mode='w', encoding=encoding) as fp:
                fp.write(html_content)
            print(f'{url} ---- download OK')
        else:
            print(f'{url} ----  exists')

    def download_page_html(self, url):
        """
        下载html分页页面
        :return:
        """
        try:
            file_name = self.get_page_html_file_name_by_url(url)
            file_path = os.path.join(self.settings.key_pages_html_dir_path, file_name)
            self.download_html(url, file_path)
            if os.path.getsize(file_path) < 1024 * 5:
                os.remove(file_path)
                print(f'remove {file_path}')
                return url
        except Exception as e:
            print(e)
            return url

    def _parse_first_page(self, first_page_url):
        """
        解析分页的首页，获取总页码数
        :return:
        """
        file_name = self.get_page_html_file_name_by_url(first_page_url)
        file_path = os.path.join(self.settings.key_pages_html_dir_path, file_name)
        if not os.path.exists(file_path):
            self.download_html(first_page_url, file_path)

        with open(file_path, mode='r', encoding='utf8') as fp:
            content = fp.read()

        tree = etree.HTML(content)
        page_str_list = tree.xpath('//span[@class="count _medium"]/text()')
        # print(page_str_list)
        if len(page_str_list) > 0:
            page_text_str = str(page_str_list[0]).replace('\n', '')
            re_result = re.search(r'〜(.*?)件.*?（(.*?)件）', page_text_str, re.M | re.I)
            data_num_str = re_result.group(2).replace(',', '')
            data_num_int = int(data_num_str)
            page_size_str = re_result.group(1).replace(',', '')
            page_size_int = int(page_size_str)

            page_num_int = data_num_int//page_size_int + 1
            self.html_page_number = min(page_num_int, 150)

        else:
            raise IndexError('not found page_number')

    def get_page_num(self):
        """
        获取总页码数
        :return:
        """
        url = self.settings.request_url
        self._parse_first_page(url)
        return self.html_page_number

    def get_page_html_file_name_by_url(self, url):
        """
        通过分页html的url起名字
        :param url:
        :return:
        """
        url_str = str(url)
        if url_str.find('=') > 0:
            url_page_num_str = url_str.split('=')[-1]
            try:
                url_page_num_int = int(url_page_num_str)
                file_name = f'{self.settings.key_word_str}_{url_page_num_int}.html'
                return file_name
            except ValueError:
                raise ValueError("url page number error")
        else:
            raise ValueError("url no find '='")

    def get_shop_info_html_file_name_by_url(self, url):
        """
        通过商店详情页的url起名字
        :param url:
        :return:
        """
        url_str = str(url)
        url_shop_name = url_str.replace(self.settings.home_url_www, '').replace(r'/', '')
        file_name = f'{url_shop_name}_info.html'
        return file_name

    @staticmethod
    def gevent_pool_requests(func, task_list, gevent_pool_size=100):
        """
        多协程请求
        :param func:
        :param task_list:
        :param gevent_pool_size:
        :return:
        """
        gevent_pool = pool.Pool(gevent_pool_size)
        result_list = gevent_pool.map(func, task_list)
        return result_list

    def gevent_download_page_html(self):
        """
        多协程下载分页页面
        :return:
        """
        page_number = self.get_page_num()
        url = self.settings.request_url
        url_pre = url[:url.rfind('=') + 1]

        page_url_list = [f'{url_pre}{i}' for i in range(1, page_number + 1)]

        for i in range(5):
            print(f'第{i+1}轮采集，本轮欲采集{len(page_url_list)}条')
            result_list = self.gevent_pool_requests(self.download_page_html, page_url_list)
            result_list = [j for j in result_list if j]
            if len(result_list) > 0:
                page_url_list = result_list
            else:
                break

    def parse_page_html(self, file_path):
        """
        解析分页html文件
        :param file_path:
        :return:
        """
        content = self.read_file(file_path)

        tree = etree.HTML(content)
        product_shop_a_list = tree.xpath('//div[contains(@class, "dui-cards")]/div[contains(@class, "dui-card")]' +
                                         '/div[contains(@class, "merchant")]/a')

        shop_list = list()
        for shop_item in product_shop_a_list:
            shop_name_list = shop_item.xpath('./text()')
            shop_url_list = shop_item.xpath('./@href')

            if len(shop_name_list) == len(shop_url_list) > 0:
                shop_name = shop_name_list[0]
                shop_url = shop_url_list[0]
                # 忽略广告跳转的url
                if str(shop_url).find('?') > 0:
                    continue
                else:
                    shop_list.append((shop_name, shop_url))
        print(f'{file_path} parsed OK')
        return shop_list

    def gevent_parse_page_htmls(self):
        """
        多协程解析分页网页
        :return:
        """
        shop_name_url_json_path = os.path.join(self.settings.key_json_dir_path, 'shop_name_url.json')
        self.shop_name_url_json_path = shop_name_url_json_path

        if not os.path.exists(shop_name_url_json_path):
            dir_path = self.settings.key_pages_html_dir_path
            file_path_list = [os.path.join(dir_path, file_name) for file_name in os.listdir(dir_path)]

            result_list = self.gevent_pool_requests(self.parse_page_html, file_path_list)

            # 分页list合并
            shop_tuple_list = list()
            for item_page_shop_list in result_list:
                if item_page_shop_list:
                    shop_tuple_list.extend(item_page_shop_list)

            # 去重并生成list(<dict>)
            shop_list = [{'shop_name': shop_name, 'shop_url': shop_url} for shop_name, shop_url in set(shop_tuple_list)]

            self.save_to_json_file(shop_list, shop_name_url_json_path)
        else:
            print(f'{shop_name_url_json_path} is exists')

    def check_and_download_shop_info_htmls(self):
        """
        检查并下载商店详情页
        :return:
        """
        if os.path.exists(self.shop_name_url_json_path):
            shop_name_url_json = self.read_json_file(self.shop_name_url_json_path)

            if len(shop_name_url_json) != len(os.listdir(self.settings.key_shop_info_html_dir_path)):
                self.gevent_download_shop_info_html(shop_name_url_json)
            else:
                print(f'all shop info htmls is exists')
        else:
            print(f'shop_name_url json file is no exists, download and parse pages htmls first')

    def gevent_download_shop_info_html(self, shop_name_url_json):
        """
        多协程下载商店详情页
        :param shop_name_url_json:
        :return:
        """
        for i in range(5):
            print(f'第{i+1}轮采集，本轮欲采集{len(shop_name_url_json)}条')
            result_list = self.gevent_pool_requests(self.download_shop_info_html, shop_name_url_json)
            result_list = [j for j in result_list if j]
            if len(result_list) > 0:
                shop_name_url_json = result_list
            else:
                break

    def download_shop_info_html(self, item_shop_name_url_dict):
        """
        下载商店详情页
        :param item_shop_name_url_dict:
        :return:
        """
        try:
            # shop_name = dict(item_shop_name_url_dict).get('shop_name', '')
            shop_url = dict(item_shop_name_url_dict).get('shop_url', '')
            shop_info_url = f'{shop_url}info.html'

            file_name = self.get_shop_info_html_file_name_by_url(shop_url)
            file_path = os.path.join(self.settings.key_shop_info_html_dir_path, file_name)

            self.download_html(shop_info_url, file_path, encoding='EUC-JP')

            if os.path.getsize(file_path) < 1024 * 5:
                os.remove(file_path)
                print(f'remove {file_path}')
                return item_shop_name_url_dict
        except Exception as e:
            print(e)
            print(traceback.format_exc())
            return item_shop_name_url_dict

    @staticmethod
    def save_to_json_file(data, json_path):
        """
        持久化为json文件
        :param data:
        :param json_path:
        :return:
        """
        with open(json_path, mode='w', encoding='utf8') as fp:
            fp.write(json.dumps(data))

    @staticmethod
    def read_json_file(json_path):
        """
        读取json文件
        :param json_path:
        :return:
        """
        with open(json_path, mode='r', encoding='utf8') as fp:
            json_file = json.load(fp)
        return json_file

    @staticmethod
    def read_file(file_path, encoding='utf8'):
        """
        读取文件
        :param file_path:
        :param encoding:
        :return:
        """
        with open(file_path, mode='r', encoding=encoding) as fp:
            content = fp.read()
        return content


class Main:
    """
    程序入口
    """
    def __init__(self):
        self.spider = RekutenSpiderKey()

    def start(self):
        """
        入口方法
        :return:
        """
        self.spider.run()


if __name__ == '__main__':
    Main().start()
