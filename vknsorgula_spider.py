#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/11/11 7:29
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : vknsorgula_spider.py
# @Software: PyCharm
import asyncio
import json
from pathlib import Path
from gevent import pool, monkey;
monkey.patch_all()

from WhileRequests import WhileRequests
from base_spider import BaseSpider, DataProgress
import pandas as pd
from lxml import etree

import html
from urllib import parse


def r(e, t):
    """
    取两个字符，并转为16进制数字
    :param e:
    :param t:
    :return:
    """
    _r = e[t: t + 2]
    _ri = int(_r, 16)
    # print(_r, _ri)
    return _ri


def de_email(n, c):
    """
    解密邮件
    :param n:
    :param c:
    :return:
    """
    o = ''
    a = r(n, c)
    for i in range(2, len(n), 2):
        m = r(n, i) ^ a
        o += chr(m)
    t = parse.unquote(html.escape(o))
    # print(t)
    return t


class VknSpider(BaseSpider):
    """
    vkn 爬虫
    """
    data_dir_path = Path(r'E:\workspace_all\workspace_py\hs_code_spider\data\土耳其')
    json_dir_path = data_dir_path / '土耳其.json'

    home_dir = Path(r'E:\vkn')

    def __init__(self):
        """
        初始化
        """
        super(BaseSpider, self).__init__()
        self.data = list()
        self.home_url = 'https://vknsorgula.net/single.php'
        self.dp = DataProgress()
        self.requests = WhileRequests()

    def load_data(self):
        """
        加载数据
        :return:
        """
        if self.json_dir_path.exists():
            with open(self.json_dir_path.as_posix(), 'r', encoding='utf8') as fp:
                self.data = json.load(fp)
                print(len(self.data))
                return

        # data_list = list()
        data_set = set()
        for file_path in self.data_dir_path.iterdir():
            df = pd.read_csv(file_path.as_posix(), dtype='str')
            # print(df.head(100))
            item_list = df.iloc[:, 1].tolist()
            data_set.update((i for i in item_list if not pd.isna(i)))

        data = list(data_set)
        with open(self.json_dir_path.as_posix(), 'w', encoding='utf8') as fp:
            json.dump(data, fp)
        self.data = data

    def download_pages(self):
        """
        下载页面
        :return:
        """
        dp = DataProgress()
        data_len = len(self.data)
        for i, item in enumerate(self.data, start=1):
            dp.print_data_progress(i, data_len)
            file_name = f'{item}.json'
            file_path = self.home_dir / file_name
            if file_path.exists():
                continue

            result = self.requests.get(url=f'{self.home_url}?vkn={item}')

            # print(result.text)
            selector = etree.HTML(result.text)
            no = self.data_list_get_first(selector.xpath('.//div[@class="section-title"]/h1[@class="mb-5"]/text()'), '')
            name = self.data_list_get_first(selector.xpath('.//div[@class="section-title"]/h2[@class="mb-5"]/text()'))
            date_time = self.data_list_get_first(selector.xpath('.//div[@class="section-title"]/h4[@class="mb-5"]/text()'))
            email = self.data_list_get_first(selector.xpath('.//div[@class="section-title"]/h4[@class="mb-5"]/a/@data-cfemail'))
            if email:
                email = de_email(email, 0)
            item_data = {
                'no': no,
                'name': name,
                'date_time': date_time,
                'email': email
            }
            # print(item_data)

            with open(file_path, 'w', encoding='utf8') as fp:
                json.dump(item_data, fp)
            # return

    def download_page(self, data):
        """
        多协程下载页面
        :param data:
        :return:
        """
        i, item, total_len, dp = data
        dp.print_data_progress(i, total_len)

        file_name = f'{item}.json'
        file_path = self.home_dir / file_name
        if file_path.exists():
            return

        result = self.requests.get(url=f'{self.home_url}?vkn={item}')

        # print(result.text)
        selector = etree.HTML(result.text)
        no = self.data_list_get_first(selector.xpath('.//div[@class="section-title"]/h1[@class="mb-5"]/text()'), '')
        name = self.data_list_get_first(selector.xpath('.//div[@class="section-title"]/h2[@class="mb-5"]/text()'))
        date_time = self.data_list_get_first(selector.xpath('.//div[@class="section-title"]/h4[@class="mb-5"]/text()'))
        email = self.data_list_get_first(
            selector.xpath('.//div[@class="section-title"]/h4[@class="mb-5"]/a/@data-cfemail'))
        if email:
            email = de_email(email, 0)
        item_data = {
            'no': no,
            'name': name,
            'date_time': date_time,
            'email': email
        }
        # print(item_data)

        with open(file_path, 'w', encoding='utf8') as fp:
            json.dump(item_data, fp)

    def gevent_pool_download_pages(self, pool_size: int = 20):
        """
        多协程下载页面
        :return:
        """
        self.load_data()
        data_list = self.data

        dp = DataProgress()

        gevent_pool = pool.Pool(pool_size)
        result_list = gevent_pool.map(self.download_page,
                                      [(i, item, len(data_list), dp) for i, item in enumerate(data_list, start=1)])
        return result_list

    def merge_data(self):
        """
        合并数据
        :return:
        """
        df = pd.DataFrame(columns=['no', 'name', 'date_time', 'email'])
        for i, file_path in enumerate(self.home_dir.iterdir()):
            if file_path.suffix != '.json':
                continue
            with open(file_path.as_posix(), 'r', encoding='utf8') as fp:
                item_data = json.load(fp)
            no = str(item_data['no'])
            if no.strip() == '':
                continue
            print(i)
            df = df.append([item_data], ignore_index=True)
            # if i > 100:
            #     break

        df.to_csv(r'E:\vkn\data.csv')


if __name__ == '__main__':
    vs = VknSpider()
    # vs.download_pages()
    # vs.gevent_pool_download_pages(pool_size=40)
    vs.merge_data()


