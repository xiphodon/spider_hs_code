#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/1/17 16:26
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : WhileRequests.py
# @Software: PyCharm

import requests
import time


class WhileRequests:
    """
    循环请求
    """
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/68.0.3440.106 Safari/537.36',
            'Connection': 'keep-alive'
        }

    def get(self, url, request_times=10000, sleep_time=0):
        """
        get请求
        :param url:
        :param request_times:
        :param sleep_time:
        :return:
        """
        while_times = 0
        while True:
            try:
                time.sleep(sleep_time)
                result = requests.get(url, headers=self.headers, timeout=15)
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