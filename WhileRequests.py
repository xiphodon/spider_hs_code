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

    def __init__(self, headers=None):
        # self.headers = {
        #     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
        #                   'Chrome/68.0.3440.106 Safari/537.36',
        #     'Connection': 'keep-alive'
        # }

        if headers is None:
            self.headers = {
                'User-Agent': 'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)',
                'Connection': 'keep-alive'
            }
        else:
            self.headers = headers

    def _request(self, url, request_times=100, sleep_time=0, timeout=15, error_retry_sleep=0.5, method='GET', data=None,
                 json='', proxies=None):
        """
        _request
        :param url:
        :param request_times:
        :param sleep_time:
        :param timeout:
        :param error_retry_sleep:
        :param proxies:
        :return:
        """
        while_times = 0
        result = None
        while True:
            try:
                time.sleep(sleep_time)
                if method == "GET":
                    result = requests.get(url, headers=self.headers, timeout=timeout, proxies=proxies)
                elif method == "POST":
                    result = requests.post(url, headers=self.headers, data=data, json=json, timeout=timeout, proxies=proxies)
                # result = requests.get(url, headers=headers, proxies=proxies, timeout=5)
            except Exception as e:
                if while_times < request_times:
                    while_times += 1
                    print('**********', '尝试重新链接[', while_times, ']次:', url)
                    time.sleep(error_retry_sleep)
                    continue
                else:
                    # raise e
                    return None
            else:
                return result

    def get(self, url, request_times=100, sleep_time=0, timeout=15, error_retry_sleep=0.5, proxies=None):
        """
        get
        :param url:
        :param request_times:
        :param sleep_time:
        :param timeout:
        :param error_retry_sleep:
        :param proxies:
        :return:
        """
        return self._request(url, request_times=request_times, sleep_time=sleep_time, timeout=timeout,
                             error_retry_sleep=error_retry_sleep, proxies=proxies, method="GET")

    def post(self, url, request_times=100, sleep_time=0, timeout=15, error_retry_sleep=0.5, data=None, json='', proxies=None):
        """
        get
        :param url:
        :param request_times:
        :param sleep_time:
        :param timeout:
        :param error_retry_sleep:
        :param data:
        :param json:
        :param proxies:
        :return:
        """
        return self._request(url, request_times=request_times, sleep_time=sleep_time, timeout=timeout,
                             error_retry_sleep=error_retry_sleep, method="POST", data=data, json=json, proxies=proxies)
