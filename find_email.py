#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/10 10:39
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : find_email.py
# @Software: PyCharm

import requests
import re
import time


def find_email_yingyanso(target_url):
    """
    查找邮箱（通过yingyanso）
    :return:
    """
    url = r'https://www.yingyanso.cn/getemail'
    headers = {
        'Host': 'www.yingyanso.cn',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:56.0) Gecko/20100101 Firefox/56.0',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',
        'Accept-Encoding': 'gzip, deflate, br',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'X-Requested-With': 'XMLHttpRequest',
        'Referer': 'https://www.yingyanso.cn/',
        'Connection': 'keep-alive'
    }
    form_data = {
        'weburl': target_url,
        'homesspd': '1'
    }
    result = requests.post(url, data=form_data, timeout=60, headers=headers)
    if result.status_code == 200:
        print(result.text)


def check_url(target_url):
    """
    检查目标url格式
    :param target_url:
    :return:
    """
    return target_url


def find_email_from_domain(target_url):
    """
    通过域名查找邮箱
    :param target_url:
    :return:
    """
    target_url = check_url(target_url)
    result = requests.get(target_url)
    if result.status_code == 200:
        time_1 = time.time()

        content = bytes(result.text, 'iso-8859-1').decode('utf-8')

        pattern_at = re.compile(r'@')
        pattern = re.compile(r'(?:[a-zA-Z0-9_.-]?)+@[a-zA-Z0-9_-]+(?:\.[a-zA-Z0-9_-]+)+')
        # pattern = re.compile(r'([A-Za-z0-9_-]+(\.\w+)*@(\w+\.)+\w{2,5})')

        span_len = 30
        new_content = ' ' * span_len + content + ' ' * span_len
        at_result_list = pattern_at.finditer(new_content)
        at_index_list = []
        email_set = set()
        if at_result_list:
            for item_at in at_result_list:
                item_index_span = item_at.span()
                at_index_list.append(item_index_span[0])

            for i in range(len(at_index_list)):

                if i == 0:
                    start_index = at_index_list[i] - span_len
                else:
                    start_index = at_index_list[i] - span_len \
                        if at_index_list[i] - span_len > at_index_list[i - 1] else at_index_list[i - 1] + 1

                if i == len(at_index_list) - 1:
                    end_index = at_index_list[i] + span_len
                else:
                    end_index = at_index_list[i] + span_len \
                        if at_index_list[i] + span_len < at_index_list[i + 1] else at_index_list[i + 1]

                result = pattern.search(new_content, start_index, end_index)
                # print('content:', new_content[start_index: end_index])
                if result:
                    email_set.add(result.group())
        time_2 = time.time()
        for item_email in email_set:
            print(item_email)
        print(time_2 - time_1)
    else:
        print('请求异常')


if __name__ == '__main__':
    url_1 = r'http://www.festo.com'
    url_2 = r'http://www.closeoutdistributors.com'
    url_3 = r'https://www.azure.cn/'
    url_4 = r'http://www.bokesoft.com/boke/contact'
    url_5 = r'http://www.hoo-design.com/'
    url_6 = r'http://www.hoo-design.com/e_product.asp'
    url_7 = r'http://www.woo-hoo.in/'
    # find_email_yingyanso(url_2)
    # time_1 = time.time()
    find_email_from_domain(url_7)
    # time_2 = time.time()
    # print(time_2 - time_1)
