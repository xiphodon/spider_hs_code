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
import time
import re


class RekutenSpiderSettings:
    """
    乐天爬虫配置
    """
    def __init__(self, key_word_list):
        if not isinstance(key_word_list, list):
            raise ValueError("must be list instance, eg:['リキッド', '電子タバコ', 'vape']")

        self._set_request_setting_(key_word_list)
        self._set_data_setting_()

    def _set_request_setting_(self, key_word_list):
        """
        设置请求配置
        :return:
        """
        self.home_url = r'https://search.rakuten.co.jp/'
        # self.key_word_list = ['リキッド', '電子タバコ', 'vape']
        self.key_word_list = key_word_list
        self.key_word_str = '+'.join(self.key_word_list)
        # https://search.rakuten.co.jp/search/mall/リキッド+電子タバコ+vape/?p=5
        self.request_url = f'{self.home_url}search/mall/{self.key_word_str}/?p=1'

    def _set_data_setting_(self):
        """
        设置数据配置
        :return:
        """
        self.home_path = r'E:\work_all\topease\takuten_spider_key'
        self.key_dir_path = os.path.join(self.home_path, self.key_word_str)
        self.key_html_path = os.path.join(self.key_dir_path, 'html_dir')
        self.key_json_path = os.path.join(self.key_dir_path, 'json_dir')

        self.make_dir(self.home_path)
        self.make_dir(self.key_dir_path)
        self.make_dir(self.key_html_path)
        self.make_dir(self.key_json_path)

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

    def run(self):
        """
        启动爬虫
        :return:
        """
        self.gevent_download_page_html()

    def download_html(self, url, save_path):
        """
        下载html页面
        :return:
        """
        if not os.path.exists(save_path):
            result = self.request.get(url)
            # html_context = bytes(result.text, 'iso-8859-1').decode('utf-8')
            html_content = result.text
            with open(save_path, mode='w', encoding='utf8') as fp:
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
            file_name = self.get_page_html_path_by_url(url)
            file_path = os.path.join(self.settings.key_html_path, file_name)
            self.download_html(url, file_path)
            if os.path.getsize(file_path) < 1024 * 5:
                os.remove(file_path)
                print(f'remove {file_path}')
                return url
        except Exception as e:
            print(e)
            return url

    def _parse_first_page_(self, first_page_url):
        """
        解析分页的首页，获取总页码数
        :return:
        """
        file_name = self.get_page_html_path_by_url(first_page_url)
        file_path = os.path.join(self.settings.key_html_path, file_name)
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
        self._parse_first_page_(url)
        return self.html_page_number

    def get_page_html_path_by_url(self, url):
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

    @staticmethod
    def gevent_pool_requests(func, urls, gevent_pool_size=100):
        """
        多协程请求
        :param func:
        :param urls:
        :param gevent_pool_size:
        :return:
        """
        gevent_pool = pool.Pool(gevent_pool_size)
        result_list = gevent_pool.map(func, urls)
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

        for i in range(50):
            print(f'第{i+1}轮采集')
            result_list = self.gevent_pool_requests(self.download_page_html, page_url_list)
            result_list = [j for j in result_list if j]
            if len(result_list) > 0:
                page_url_list = result_list
            else:
                break


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
